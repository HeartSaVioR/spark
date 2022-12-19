/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{CurrentBatchTimestamp, ExpressionWithRandomSeed}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution.{LocalLimitExec, QueryExecution, SparkPlan, SparkPlanner, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, MergingSessionsExec, ObjectHashAggregateExec, SortAggregateExec, UpdatingSessionsExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.python.FlatMapGroupsInPandasWithStateExec
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
class IncrementalExecution(
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    val outputMode: OutputMode,
    val checkpointLocation: String,
    val queryId: UUID,
    val runId: UUID,
    val currentBatchId: Long,
    val prevOffsetSeqMetadata: Option[OffsetSeqMetadata],
    val offsetSeqMetadata: OffsetSeqMetadata)
  extends QueryExecution(sparkSession, logicalPlan) with Logging {

  private val isDebugLog: Boolean = checkpointLocation != "<unknown>"
  private def debugLog(str: => String): Unit = {
    if (isDebugLog) {
      logWarning(str)
    }
  }

  debugLog(s"====== Initialing IncrementalExecution: id $id with batch ID $currentBatchId ======")

  // Modified planner with stateful operations.
  override val planner: SparkPlanner = new SparkPlanner(
      sparkSession,
      sparkSession.sessionState.experimentalMethods) {
    override def strategies: Seq[Strategy] =
      extraPlanningStrategies ++
      sparkSession.sessionState.planner.strategies

    override def extraPlanningStrategies: Seq[Strategy] =
      StreamingJoinStrategy ::
      StatefulAggregationStrategy ::
      FlatMapGroupsWithStateStrategy ::
      FlatMapGroupsInPandasWithStateStrategy ::
      StreamingRelationStrategy ::
      StreamingDeduplicationStrategy ::
      StreamingGlobalLimitStrategy(outputMode) :: Nil
  }

  private[sql] val numStateStores = offsetSeqMetadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key)
    .map(SQLConf.SHUFFLE_PARTITIONS.valueConverter)
    .getOrElse(sparkSession.sessionState.conf.numShufflePartitions)

  /**
   * See [SPARK-18339]
   * Walk the optimized logical plan and replace CurrentBatchTimestamp
   * with the desired literal
   */
  override
  lazy val optimizedPlan: LogicalPlan = executePhase(QueryPlanningTracker.OPTIMIZATION) {
    // Performing streaming specific pre-optimization.
    val preOptimized = withCachedData.transform {
      // We eliminate the "marker" node for writer on DSv1 as it's only used as representation
      // of sink information.
      case w: WriteToMicroBatchDataSourceV1 => w.child
    }
    sparkSession.sessionState.optimizer.executeAndTrack(preOptimized,
      tracker).transformAllExpressionsWithPruning(
      _.containsAnyPattern(CURRENT_LIKE, EXPRESSION_WITH_RANDOM_SEED)) {
      case ts @ CurrentBatchTimestamp(timestamp, _, _) =>
        logInfo(s"Current batch timestamp = $timestamp")
        ts.toLiteral
      case e: ExpressionWithRandomSeed => e.withNewSeed(Utils.random.nextLong())
    }
  }

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private val statefulOperatorId = new AtomicInteger(0)

  /** Get the state info of the next stateful operator */
  private def nextStatefulOperationStateInfo(): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(
      checkpointLocation,
      runId,
      statefulOperatorId.getAndIncrement(),
      currentBatchId,
      numStateStores)
  }

  // FIXME: How to deal with behavioral change? there is a config
  //  SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE and we need to think through what is the right
  //  behavior when this config is turned off and watermark per operator kicks in.

  // Watermarks to use for late record filtering and state eviction in stateful operators.
  // Using the previous watermark for late record filtering is a Spark behavior change so we allow
  // this to be disabled.
  private val eventTimeWatermarkForEviction = offsetSeqMetadata.batchWatermarkMs
  private val eventTimeWatermarkForLateEvents =
    if (sparkSession.conf.get(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)) {
      debugLog(s"====== prevOffsetSeqMetadata: $prevOffsetSeqMetadata ======")
      prevOffsetSeqMetadata.getOrElse(offsetSeqMetadata).batchWatermarkMs
    } else {
      eventTimeWatermarkForEviction
    }

  /** Locates save/restore pairs surrounding aggregation. */
  val state = new Rule[SparkPlan] {

    /**
     * Ensures that this plan DOES NOT have any stateful operation in it whose pipelined execution
     * depends on this plan. In other words, this function returns true if this plan does
     * have a narrow dependency on a stateful subplan.
     */
    private def hasNoStatefulOp(plan: SparkPlan): Boolean = {
      var statefulOpFound = false

      def findStatefulOp(planToCheck: SparkPlan): Unit = {
        planToCheck match {
          case s: StatefulOperator =>
            statefulOpFound = true

          case e: ShuffleExchangeLike =>
            // Don't search recursively any further as any child stateful operator as we
            // are only looking for stateful subplans that this plan has narrow dependencies on.

          case p: SparkPlan =>
            p.children.foreach(findStatefulOp)
        }
      }

      findStatefulOp(plan)
      !statefulOpFound
    }

    private var numApplyCalled: Int = 0

    override def apply(plan: SparkPlan): SparkPlan = {
      numApplyCalled += 1
      debugLog(s"======== state rule: called ${numApplyCalled} times =======")
      debugLog("======== state rule: issuing state info ========")

      val planWithStateInfo = plan transformDown {
        // NOTE: we should include all aggregate execs here which are used in streaming aggregations
        case a: SortAggregateExec if a.isStreaming && a.numShufflePartitions.isEmpty =>
          a.copy(numShufflePartitions = Some(numStateStores))

        case a: HashAggregateExec if a.isStreaming && a.numShufflePartitions.isEmpty =>
          a.copy(numShufflePartitions = Some(numStateStores))

        case a: ObjectHashAggregateExec if a.isStreaming && a.numShufflePartitions.isEmpty =>
          a.copy(numShufflePartitions = Some(numStateStores))

        case a: MergingSessionsExec if a.isStreaming && a.numShufflePartitions.isEmpty =>
          a.copy(numShufflePartitions = Some(numStateStores))

        case a: UpdatingSessionsExec if a.isStreaming && a.numShufflePartitions.isEmpty =>
          a.copy(numShufflePartitions = Some(numStateStores))

        case StateStoreSaveExec(keys, None, None, None, None, stateFormatVersion,
        UnaryExecNode(agg,
        StateStoreRestoreExec(_, None, _, child))) =>
          val aggStateInfo = nextStatefulOperationStateInfo
          StateStoreSaveExec(
            keys,
            Some(aggStateInfo),
            Some(outputMode),
            None,
            None,
            stateFormatVersion,
            agg.withNewChildren(
              StateStoreRestoreExec(
                keys,
                Some(aggStateInfo),
                stateFormatVersion,
                child) :: Nil))

        case SessionWindowStateStoreSaveExec(keys, session, None, None, None, None,
        stateFormatVersion,
        UnaryExecNode(agg,
        SessionWindowStateStoreRestoreExec(_, _, None, None, None, _, child))) =>
          val aggStateInfo = nextStatefulOperationStateInfo
          SessionWindowStateStoreSaveExec(
            keys,
            session,
            Some(aggStateInfo),
            Some(outputMode),
            None,
            None,
            stateFormatVersion,
            agg.withNewChildren(
              SessionWindowStateStoreRestoreExec(
                keys,
                session,
                Some(aggStateInfo),
                None,
                None,
                stateFormatVersion,
                child) :: Nil))

        case StreamingDeduplicateExec(keys, child, None, None, None) =>
          val stateInfo = nextStatefulOperationStateInfo
          StreamingDeduplicateExec(
            keys,
            child,
            Some(stateInfo),
            None,
            None)

        case m: FlatMapGroupsWithStateExec if m.stateInfo.isEmpty =>
          // We set this to true only for the first batch of the streaming query.
          val hasInitialState = (currentBatchId == 0L && m.hasInitialState)
          val stateInfo = nextStatefulOperationStateInfo
          m.copy(
            stateInfo = Some(stateInfo),
            batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
            eventTimeWatermarkForLateEvents = None,
            eventTimeWatermarkForEviction = None,
            hasInitialState = hasInitialState
          )

        case m: FlatMapGroupsInPandasWithStateExec if m.stateInfo.isEmpty =>
          val stateInfo = nextStatefulOperationStateInfo
          m.copy(
            stateInfo = Some(stateInfo),
            batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs))

        case j: StreamingSymmetricHashJoinExec if j.stateInfo.isEmpty =>
          val stateInfo = nextStatefulOperationStateInfo
          j.copy(stateInfo = Some(stateInfo))

        case l: StreamingGlobalLimitExec if l.stateInfo.isEmpty =>
          l.copy(
            stateInfo = Some(nextStatefulOperationStateInfo),
            outputMode = Some(outputMode))

        case StreamingLocalLimitExec(limit, child) if hasNoStatefulOp(child) =>
          // Optimize limit execution by replacing StreamingLocalLimitExec (consumes the iterator
          // completely) to LocalLimitExec (does not consume the iterator) when the child plan has
          // no stateful operator (i.e., consuming the iterator is not needed).
          LocalLimitExec(limit, child)
      }

      if (plan.fastEquals(planWithStateInfo)) {
        return plan
      }

      debugLog("======== state rule: calculating watermark propagation ========")
      debugLog(s"======== state rule: late events: $eventTimeWatermarkForLateEvents, " +
        s"eviction: $eventTimeWatermarkForEviction ========")

      val watermarkCalc = new WatermarkPropagationCalculator(planWithStateInfo, isDebugLog)
      watermarkCalc.calculate(eventTimeWatermarkForLateEvents, eventTimeWatermarkForEviction)

      debugLog("======== state rule: applying watermark to state ops ========")

      val planWithWatermark = planWithStateInfo transform {
        case StateStoreSaveExec(keys, Some(stateInfo), _, None, None, stateFormatVersion,
        UnaryExecNode(agg,
        StateStoreRestoreExec(_, _, _, child))) =>
          StateStoreSaveExec(
            keys,
            Some(stateInfo),
            Some(outputMode),
            Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
            Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)),
            stateFormatVersion,
            agg.withNewChildren(
              StateStoreRestoreExec(
                keys,
                Some(stateInfo),
                stateFormatVersion,
                child) :: Nil))

        case SessionWindowStateStoreSaveExec(keys, session, Some(stateInfo), _, None, None,
        stateFormatVersion,
        UnaryExecNode(agg,
        SessionWindowStateStoreRestoreExec(_, _, _, None, None, _, child))) =>
          SessionWindowStateStoreSaveExec(
            keys,
            session,
            Some(stateInfo),
            Some(outputMode),
            Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
            Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)),
            stateFormatVersion,
            agg.withNewChildren(
              SessionWindowStateStoreRestoreExec(
                keys,
                session,
                Some(stateInfo),
                Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
                Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)),
                stateFormatVersion,
                child) :: Nil))

        case StreamingDeduplicateExec(keys, child, Some(stateInfo), None, None) =>
          StreamingDeduplicateExec(
            keys,
            child,
            Some(stateInfo),
            Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
            Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)))

        case m: FlatMapGroupsWithStateExec
          if m.stateInfo.isDefined && m.eventTimeWatermarkForLateEvents.isEmpty =>
          val stateInfo = m.stateInfo.get
          m.copy(
            eventTimeWatermarkForLateEvents =
              Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
            eventTimeWatermarkForEviction =
              Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)))

        case m: FlatMapGroupsInPandasWithStateExec
          if m.stateInfo.isDefined && m.eventTimeWatermarkForLateEvents.isEmpty =>
          val stateInfo = m.stateInfo.get
          m.copy(
            eventTimeWatermarkForLateEvents =
              Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
            eventTimeWatermarkForEviction =
              Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)))

        case j: StreamingSymmetricHashJoinExec
          if j.stateInfo.isDefined && j.eventTimeWatermarkForLateEvents.isEmpty =>
          val stateInfo = j.stateInfo.get
          j.copy(
            eventTimeWatermarkForLateEvents =
              Some(watermarkCalc.getWatermarkForLateEvents(stateInfo.operatorId)),
            eventTimeWatermarkForEviction =
              Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId)),
            stateWatermarkPredicates =
              StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
                j.left.output, j.right.output, j.leftKeys, j.rightKeys, j.condition.full,
                Some(watermarkCalc.getWatermarkForEviction(stateInfo.operatorId))))
      }

      planWithWatermark
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = {
    debugLog(s"====== preparations called from execution id ${this.id} ======")
    debugLog(s"======== preparations: late events: $eventTimeWatermarkForLateEvents, " +
      s"eviction: $eventTimeWatermarkForEviction ========")
    state +: super.preparations
  }

  /** No need assert supported, as this check has already been done */
  override def assertSupported(): Unit = { }

  /**
   * Should the MicroBatchExecution run another batch based on this execution and the current
   * updated metadata.
   */
  def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    debugLog("======== shouldRunAnotherBatch ========")
    val watermarkCalc = new WatermarkPropagationCalculator(executedPlan, isDebugLog)
    watermarkCalc.calculate(0, newMetadata.batchWatermarkMs)
    executedPlan.collect {
      case p: StateStoreWriter => p.shouldRunAnotherBatch(
        watermarkCalc.getWatermarkForEviction(p.stateInfo.get.operatorId))
    }.exists(_ == true)
  }
}

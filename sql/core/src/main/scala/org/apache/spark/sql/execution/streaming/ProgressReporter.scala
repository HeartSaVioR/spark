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

import java.text.SimpleDateFormat
import java.util.{Date, Optional}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_SECOND
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, ReportsSinkMetrics, ReportsSourceMetrics, SparkDataStream}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.{MicroBatchScanExec, StreamingDataSourceV2Relation, StreamWriterCommitProgress}
import org.apache.spark.sql.streaming.{SinkProgress, SourceProgress, StateOperatorProgress, StreamingQueryException, StreamingQueryListener, StreamingQueryProgress, StreamingQueryStatus}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.util.Utils

/**
 * Responsible for continually reporting statistics about the amount of data processed as well
 * as latency for a streaming query. [[StreamExecution]] is responsible for calling `startTrigger`
 * and `finishTrigger` at the appropriate times, to track and report statistics for each epoch run.
 * Additionally, the status can updated with `updateStatusMessage` to allow reporting on the
 * streams current state (i.e. "Fetching more data").
 */
class ProgressReporter(private val queryProperties: StreamingQueryProperties)
  extends Logging {

  private val sparkSession: SparkSession = queryProperties.sparkSession

  // Static properties that do not change once streaming query has been planned
  private var queryPlanningProperties: StreamingQueryPlanProperties = _

  // This is guaranteed to be set before calling startTrigger().
  def setPlanningProperties(planningProperties: StreamingQueryPlanProperties): Unit = {
    queryPlanningProperties = planningProperties
  }

  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L

  /** Flag that signals whether any error with input metrics have already been logged */
  private val metricWarningLogged: AtomicBoolean = new AtomicBoolean(false)

  /** Holds the most recent query progress updates.  Accesses must lock on the queue itself. */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has not executed anything
  private var lastNoExecutionProgressEventTime = Long.MinValue

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))

  /** Holds the most recent status for the query. Access must lock on the `statusLock` field. */
  // TODO: it's not crystally clear where is the best place for status. Leave this as it is
  //  till we find a better place.
  private var currentStatus: StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = "Initializing StreamExecution",
      isDataAvailable = false,
      isTriggerActive = false)
  }
  private val statusLock: Object = new Object

  /** Returns the current status of the query. */
  def status: StreamingQueryStatus = statusLock.synchronized {
    currentStatus
  }

  /** Returns an array containing the most recent query progress updates. */
  def recentProgress: Array[StreamingQueryProgress] = progressBuffer.synchronized {
    progressBuffer.toArray
  }

  /** Returns the most recent query progress update or null if there were no progress updates. */
  def lastProgress: StreamingQueryProgress = progressBuffer.synchronized {
    progressBuffer.lastOption.orNull
  }

  /**
   * Begins recording statistics about query progress for a given trigger.
   *
   * This method also gets initial values of some parameters because currently Spark also reports
   * progress for the trigger which does not actually run the batch, and for the case it looks
   * back for most recent values rather than emptying out all the info.
   * TODO: How much it has been useful for reporting progress on the trigger which corresponding
   *   batch does not exist (no executed)? Wouldn't it be much clearer if we only report progress
   *   on "executed" batch? Who cares about trigger with no batch execution?
   */
  def startTrigger(
      initialOffsetSeqMetadata: OffsetSeqMetadata,
      initialLastExecution: IncrementalExecution): EpochProgressReportContext = {
    logDebug("Starting Trigger Calculation")

    assert(queryProperties != null)
    assert(queryPlanningProperties != null)

    val currentTriggerStartTimestamp = queryProperties.triggerClock.getTimeMillis()
    val context = new EpochProgressReportContext(queryProperties, queryPlanningProperties,
      lastTriggerStartTimestamp, currentTriggerStartTimestamp, metricWarningLogged)
    lastTriggerStartTimestamp = currentTriggerStartTimestamp
    context.updateOffsetSeqMetadata(initialOffsetSeqMetadata)
    context.updateLastExecution(initialLastExecution)
    context
  }

  def finishTrigger(
      progressCtx: EpochProgressReportContext,
      hasNewData: Boolean,
      hasExecuted: Boolean,
      lastEpochId: Long): Unit = {
    finishTrigger(progressCtx, hasNewData, hasExecuted, null, lastEpochId)
  }

  /**
   * Finalizes the query progress and adds it to list of recent status updates.
   *
   * @param hasNewData Whether the sources of this stream had new data for this trigger.
   * @param hasExecuted Whether any batch was executed during this trigger. Streaming queries that
   *                    perform stateful aggregations with timeouts can still run batches even
   *                    though the sources don't have any new data.
   */
  def finishTrigger(
      progressCtx: EpochProgressReportContext,
      hasNewData: Boolean,
      hasExecuted: Boolean,
      sourceToNumInputRowsMap: Map[SparkDataStream, Long],
      lastEpochId: Long): Unit = {
    val startProcessingFinishTriggerMillis = queryProperties.triggerClock.getTimeMillis()

    val newProgress = if (sourceToNumInputRowsMap != null) {
      progressCtx.buildQueryProgress(hasNewData, hasExecuted, sourceToNumInputRowsMap,
        lastEpochId)
    } else {
      progressCtx.buildQueryProgress(hasNewData, hasExecuted, lastEpochId)
    }

    if (hasExecuted) {
      // Reset noDataEventTimestamp if we processed any data
      lastNoExecutionProgressEventTime = queryProperties.triggerClock.getTimeMillis()
      updateProgress(newProgress)
    } else {
      val now = queryProperties.triggerClock.getTimeMillis()
      if (now - noDataProgressEventInterval >= lastNoExecutionProgressEventTime) {
        lastNoExecutionProgressEventTime = now
        updateProgress(newProgress)
      }
    }

    // Log a warning message if finishTrigger step takes more time than processing the batch and
    // also longer than min threshold (1 minute).
    val processingTimeMills = progressCtx.processingTimeMillis
    val finishTriggerDurationMillis = queryProperties.triggerClock.getTimeMillis() -
      startProcessingFinishTriggerMillis
    val thresholdForLoggingMillis = 60 * 1000
    if (finishTriggerDurationMillis > math.max(thresholdForLoggingMillis, processingTimeMills)) {
      logWarning("Query progress update takes longer than batch processing time. Progress " +
        s"update takes $finishTriggerDurationMillis milliseconds. Batch processing takes " +
        s"$processingTimeMills milliseconds")
    }

    // Mark trigger as deactivated when the execution for specific trigger is finished.
    updateTriggerActive(activated = false)
  }

  /** Updates the message returned in `status`. */
  def updateStatusMessage(message: String): Unit = statusLock.synchronized {
    currentStatus = currentStatus.copy(message = message)
  }

  def updateTriggerActive(activated: Boolean): Unit = statusLock.synchronized {
    currentStatus = currentStatus.copy(isTriggerActive = activated)
  }

  def updateNewDataAvailability(newDataAvailable: Boolean): Unit = statusLock.synchronized {
    currentStatus = currentStatus.copy(isDataAvailable = newDataAvailable)
  }

  def postQueryStarted(): Unit = {
    val startTimestamp = queryProperties.triggerClock.getTimeMillis()
    postEvent(new QueryStartedEvent(
      queryProperties.id, queryProperties.runId, queryProperties.name,
      formatTimestamp(startTimestamp)))
  }

  def postQueryTerminated(exception: Option[StreamingQueryException]): Unit = {
    statusLock.synchronized {
      currentStatus = currentStatus.copy(isTriggerActive = false,
        isDataAvailable = false)
    }

    postEvent(new QueryTerminatedEvent(
      queryProperties.id, queryProperties.runId,
      exception.map(_.cause).map(Utils.exceptionString)))
  }

  private def formatTimestamp(millis: Long): String = {
    timestampFormat.format(new Date(millis))
  }

  private def updateProgress(newProgress: StreamingQueryProgress): Unit = {
    progressBuffer.synchronized {
      progressBuffer += newProgress
      while (progressBuffer.length >= sparkSession.sqlContext.conf.streamingProgressRetention) {
        progressBuffer.dequeue()
      }
    }
    postEvent(new QueryProgressEvent(newProgress))
    logInfo(s"Streaming query made progress: $newProgress")
  }

  private def postEvent(event: StreamingQueryListener.Event): Unit = {
    sparkSession.streams.postListenerEvent(event)
  }
}

/**
 * The context class for tracking and reporting progress for specific epoch (batch for microbatch
 * execution, epoch for continuous execution).
 */
class EpochProgressReportContext(
    queryProperties: StreamingQueryProperties,
    planningProperties: StreamingQueryPlanProperties,
    lastTriggerStartTimestamp: Long,
    currentTriggerStartTimestamp: Long,
    metricWarnLogged: AtomicBoolean) extends Logging {

  case class ExecutionStats(
      inputRows: Map[SparkDataStream, Long],
      stateOperators: Seq[StateOperatorProgress],
      eventTimeStats: Map[String, String])

  @volatile private var offsetSeqMetadata: OffsetSeqMetadata = _
  @volatile private var newData: Map[SparkDataStream, LogicalPlan] = _
  @volatile private var sinkCommitProgress: Option[StreamWriterCommitProgress] = _
  @volatile private var lastExecution: IncrementalExecution = _

  def updateOffsetSeqMetadata(update: OffsetSeqMetadata): Unit = {
    offsetSeqMetadata = update
  }

  def updateNewData(update: Map[SparkDataStream, LogicalPlan]): Unit = {
    newData = update
  }

  def updateSinkCommitProgress(update: Option[StreamWriterCommitProgress]): Unit = {
    sinkCommitProgress = update
  }

  def updateLastExecution(update: IncrementalExecution): Unit = {
    lastExecution = update
  }

  logDebug("Starting Trigger Calculation")

  // Local timestamps and counters.
  // Begins recording statistics about query progress for a given trigger.
  private var currentTriggerEndTimestamp = -1L
  private var currentTriggerStartOffsets: Map[SparkDataStream, String] = _
  private var currentTriggerEndOffsets: Map[SparkDataStream, String] = _
  private var currentTriggerLatestOffsets: Map[SparkDataStream, String] = _

  private val currentDurationsMs = new mutable.HashMap[String, Long]()

  private var latestStreamProgress: StreamProgress = _

  // This is not thread-safe, and the cost of creating this instance per trigger isn't a big deal.
  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))

  private def formatTimestamp(millis: Long): String = {
    timestampFormat.format(new Date(millis))
  }

  /**
   * Record the offsets range this trigger will process. Call this before updating
   * `committedOffsets` in `StreamExecution` to make sure that the correct range is recorded.
   */
  def recordTriggerOffsets(
      from: StreamProgress,
      to: StreamProgress,
      latest: StreamProgress): Unit = {
    currentTriggerStartOffsets = from.mapValues(_.json).toMap
    currentTriggerEndOffsets = to.mapValues(_.json).toMap
    latestStreamProgress = to
    currentTriggerLatestOffsets = latest.mapValues(_.json).toMap
  }

  /** Records the duration of running `body` for the next query progress update. */
  def reportTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = queryProperties.triggerClock.getTimeMillis()
    val result = body
    val endTime = queryProperties.triggerClock.getTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    reportTimeTaken(triggerDetailKey, timeTaken)
    result
  }

  /**
   * Reports an input duration for a particular detail key in the next query progress
   * update. Can be used directly instead of reportTimeTaken(key)(body) when the duration
   * is measured asynchronously.
   */
  def reportTimeTaken(triggerDetailKey: String, timeTakenMs: Long): Unit = {
    val previousTime = currentDurationsMs.getOrElse(triggerDetailKey, 0L)
    currentDurationsMs.put(triggerDetailKey, previousTime + timeTakenMs)
    logDebug(s"$triggerDetailKey took $timeTakenMs ms")
  }

  def processingTimeMillis: Long = currentTriggerEndTimestamp - currentTriggerStartTimestamp

  def buildQueryProgress(
      hasNewData: Boolean,
      hasExecuted: Boolean,
      lastEpochId: Long): StreamingQueryProgress = {
    // Microbatch execution.
    val map: Map[SparkDataStream, Long] =
      if (hasNewData) extractSourceToNumInputRows(lastExecution) else Map.empty
    buildQueryProgress(hasNewData, hasExecuted, map, lastEpochId)
  }

  def buildQueryProgress(
      hasNewData: Boolean,
      hasExecuted: Boolean,
      sourceToNumInputRowsMap: Map[SparkDataStream, Long],
      lastEpochId: Long): StreamingQueryProgress = {
    assert(currentTriggerStartOffsets != null && currentTriggerEndOffsets != null &&
      currentTriggerLatestOffsets != null)
    currentTriggerEndTimestamp = queryProperties.triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(
      hasNewData, hasExecuted, sourceToNumInputRowsMap, lastExecution)
    val processingTimeMills = currentTriggerEndTimestamp - currentTriggerStartTimestamp
    val processingTimeSec = Math.max(1L, processingTimeMills).toDouble / MILLIS_PER_SECOND

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / MILLIS_PER_SECOND
    } else {
      Double.PositiveInfinity
    }
    logDebug(s"Execution stats: $executionStats")

    val sourceProgress = planningProperties.sources.distinct.map { source =>
      val numRecords = executionStats.inputRows.getOrElse(source, 0L)
      val sourceMetrics = source match {
        case withMetrics: ReportsSourceMetrics =>
          withMetrics.metrics(Optional.ofNullable(latestStreamProgress.get(source).orNull))
        case _ => Map[String, String]().asJava
      }
      new SourceProgress(
        description = source.toString,
        startOffset = currentTriggerStartOffsets.get(source).orNull,
        endOffset = currentTriggerEndOffsets.get(source).orNull,
        latestOffset = currentTriggerLatestOffsets.get(source).orNull,
        numInputRows = numRecords,
        inputRowsPerSecond = numRecords / inputTimeSec,
        processedRowsPerSecond = numRecords / processingTimeSec,
        metrics = sourceMetrics
      )
    }

    val sinkOutput = if (hasExecuted) {
      sinkCommitProgress.map(_.numOutputRows)
    } else {
      // The batch hasn't been executed anyway, so it doesn't sound meaningful for the
      // number of output rows for sink to be 0 vs -1.
      None
    }

    val sinkMetrics = queryProperties.sink match {
      case withMetrics: ReportsSinkMetrics =>
        withMetrics.metrics()
      case _ => Map[String, String]().asJava
    }

    val sinkProgress = SinkProgress(
      queryProperties.sink.toString, sinkOutput, sinkMetrics)

    val observedMetrics = extractObservedMetrics(hasNewData, lastExecution)

    new StreamingQueryProgress(
      id = queryProperties.id,
      runId = queryProperties.runId,
      name = queryProperties.name,
      timestamp = formatTimestamp(currentTriggerStartTimestamp),
      batchId = lastEpochId,
      batchDuration = processingTimeMills,
      durationMs =
        new java.util.HashMap(currentDurationsMs.toMap.mapValues(long2Long).toMap.asJava),
      eventTime = new java.util.HashMap(executionStats.eventTimeStats.asJava),
      stateOperators = executionStats.stateOperators.toArray,
      sources = sourceProgress.toArray,
      sink = sinkProgress,
      observedMetrics = new java.util.HashMap(observedMetrics.asJava))
  }

  /** Extract statistics about stateful operators from the executed query plan. */
  private def extractStateOperatorMetrics(
      hasExecuted: Boolean,
      lastExecution: IncrementalExecution): Seq[StateOperatorProgress] = {
    if (lastExecution == null) return Nil
    // lastExecution could belong to one of the previous triggers if `!hasExecuted`.
    // Walking the plan again should be inexpensive.
    lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[StateStoreWriter] =>
        val progress = p.asInstanceOf[StateStoreWriter].getProgress()
        if (hasExecuted) {
          progress
        } else {
          progress.copy(newNumRowsUpdated = 0, newNumRowsDroppedByWatermark = 0)
        }
    }
  }

  /** Extracts statistics from the most recent query execution. */
  private def extractExecutionStats(
      hasNewData: Boolean,
      hasExecuted: Boolean,
      sourceToNumInputRows: Map[SparkDataStream, Long],
      lastExecution: IncrementalExecution): ExecutionStats = {
    val hasEventTime = planningProperties.logicalPlan.collect {
      case e: EventTimeWatermark => e
    }.nonEmpty
    val watermarkTimestamp =
      if (hasEventTime) Map("watermark" -> formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
      else Map.empty[String, String]

    // SPARK-19378: Still report metrics even though no data was processed while reporting progress.
    val stateOperators = extractStateOperatorMetrics(hasExecuted, lastExecution)

    if (!hasNewData) {
      return ExecutionStats(Map.empty, stateOperators, watermarkTimestamp)
    }

    val eventTimeStats = lastExecution.executedPlan.collect {
      case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
        val stats = e.eventTimeStats.value
        Map(
          "max" -> stats.max,
          "min" -> stats.min,
          "avg" -> stats.avg.toLong).mapValues(formatTimestamp)
    }.headOption.getOrElse(Map.empty) ++ watermarkTimestamp

    ExecutionStats(sourceToNumInputRows, stateOperators, eventTimeStats.toMap)
  }

  /** Extracts observed metrics from the most recent query execution. */
  private def extractObservedMetrics(
      hasNewData: Boolean,
      lastExecution: QueryExecution): Map[String, Row] = {
    if (!hasNewData || lastExecution == null) {
      return Map.empty
    }
    lastExecution.observedMetrics
  }

  /** Extract number of input sources for each streaming source in plan */
  private def extractSourceToNumInputRows(
      lastExecution: IncrementalExecution): Map[SparkDataStream, Long] = {

    assert(newData != null)

    def sumRows(tuples: Seq[(SparkDataStream, Long)]): Map[SparkDataStream, Long] = {
      tuples.groupBy(_._1).mapValues(_.map(_._2).sum).toMap // sum up rows for each source
    }

    def unrollCTE(plan: LogicalPlan): LogicalPlan = {
      val containsCTE = plan.exists {
        case _: WithCTE => true
        case _ => false
      }

      if (containsCTE) {
        InlineCTE(alwaysInline = true).apply(plan)
      } else {
        plan
      }
    }

    val onlyDataSourceV2Sources = {
      // Check whether the streaming query's logical plan has only V2 micro-batch data sources
      val allStreamingLeaves = planningProperties.logicalPlan.collect {
        case s: StreamingDataSourceV2Relation => s.stream.isInstanceOf[MicroBatchStream]
        case _: StreamingExecutionRelation => false
      }
      allStreamingLeaves.forall(_ == true)
    }

    if (onlyDataSourceV2Sources) {
      // It's possible that multiple DataSourceV2ScanExec instances may refer to the same source
      // (can happen with self-unions or self-joins). This means the source is scanned multiple
      // times in the query, we should count the numRows for each scan.
      val sourceToInputRowsTuples = lastExecution.executedPlan.collect {
        case s: MicroBatchScanExec =>
          val numRows = s.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          val source = s.stream
          source -> numRows
      }
      logDebug("Source -> # input rows\n\t" + sourceToInputRowsTuples.mkString("\n\t"))
      sumRows(sourceToInputRowsTuples)
    } else {

      // Since V1 source do not generate execution plan leaves that directly link with source that
      // generated it, we can only do a best-effort association between execution plan leaves to the
      // sources. This is known to fail in a few cases, see SPARK-24050.
      //
      // We want to associate execution plan leaves to sources that generate them, so that we match
      // the their metrics (e.g. numOutputRows) to the sources. To do this we do the following.
      // Consider the translation from the streaming logical plan to the final executed plan.
      //
      // streaming logical plan (with sources) <==> trigger's logical plan <==> executed plan
      //
      // 1. We keep track of streaming sources associated with each leaf in trigger's logical plan
      //  - Each logical plan leaf will be associated with a single streaming source.
      //  - There can be multiple logical plan leaves associated with a streaming source.
      //  - There can be leaves not associated with any streaming source, because they were
      //      generated from a batch source (e.g. stream-batch joins)
      //
      // 2. Assuming that the executed plan has same number of leaves in the same order as that of
      //    the trigger logical plan, we associate executed plan leaves with corresponding
      //    streaming sources.
      //
      // 3. For each source, we sum the metrics of the associated execution plan leaves.
      //
      val logicalPlanLeafToSource = newData.flatMap { case (source, logicalPlan) =>
        logicalPlan.collectLeaves().map { leaf => leaf -> source }
      }

      // SPARK-41198: CTE is inlined in optimization phase, which ends up with having different
      // number of leaf nodes between (analyzed) logical plan and executed plan. Here we apply
      // inlining CTE against logical plan manually if there is a CTE node.
      val finalLogicalPlan = unrollCTE(lastExecution.logical)

      val allLogicalPlanLeaves = finalLogicalPlan.collectLeaves() // includes non-streaming
      val allExecPlanLeaves = lastExecution.executedPlan.collectLeaves()
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (_, ep: MicroBatchScanExec) =>
            // SPARK-41199: `logicalPlanLeafToSource` contains OffsetHolder instance for DSv2
            // streaming source, hence we cannot lookup the actual source from the map.
            // The physical node for DSv2 streaming source contains the information of the source
            // by itself, so leverage it.
            Some(ep -> ep.stream)
          case (lp, ep) =>
            logicalPlanLeafToSource.get(lp).map { source => ep -> source }
        }
        val sourceToInputRowsTuples = execLeafToSource.map { case (execLeaf, source) =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          source -> numRows
        }
        sumRows(sourceToInputRowsTuples)
      } else {
        if (!metricWarnLogged.getAndSet(true)) {
          def toString[T](seq: Seq[T]): String = s"(size = ${seq.size}), ${seq.mkString(", ")}"

          logWarning(
            "Could not report metrics as number leaves in trigger logical plan did not match that" +
              s" of the execution plan:\n" +
              s"logical plan leaves: ${toString(allLogicalPlanLeaves)}\n" +
              s"execution plan leaves: ${toString(allExecPlanLeaves)}\n")
        }
        Map.empty
      }
    }
  }
}

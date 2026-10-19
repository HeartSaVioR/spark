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

package org.apache.spark.sql.execution.joins

import java.util.{LinkedHashMap => JLinkedHashMap, Map => JMap}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import org.apache.spark.{SparkIllegalStateException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, CollationKey, Expression,
  GenericInternalRow, JoinedRow, Predicate, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.optimizer.NormalizeFloatingNumbers
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupCacheOptions,
  ExternalLookupFilter, ExternalLookupProject, ExternalLookupTransform}
import org.apache.spark.sql.connector.read.{ExternalLookupReader, ExternalLookupReaderFactory,
  ExternalLookupResultCursor}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.SupportsCustomDriverMetrics
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.util.Utils

private case class ExternalLookupCacheEntry(values: Array[InternalRow], createdAtNanos: Long) {
  def isExpired(ttlNanos: Long, nowNanos: Long): Boolean =
    ttlNanos > 0 && nowNanos - createdAtNanos >= ttlNanos
}

/** Executes synchronous, parameterized external lookup joins without a shuffle. */
case class ExternalLookupJoinExec(
    lookupParameters: Seq[Expression],
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    lookupTransforms: Seq[ExternalLookupTransform],
    nullSafeParameters: Seq[Boolean],
    joinType: JoinType,
    residualCondition: Option[Expression],
    @transient scan: org.apache.spark.sql.connector.read.Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    cacheOptions: ExternalLookupCacheOptions,
    child: SparkPlan) extends UnaryExecNode with SupportsCustomDriverMetrics {

  private val EngineBatchSize = 1000

  override def output: Seq[Attribute] = joinType match {
    case Inner => child.output ++ lookupOutput
    case LeftOuter => child.output ++ lookupOutput.map(_.withNullability(true))
    case LeftSemi | LeftAnti => child.output
    case _ => child.output ++ lookupOutput
  }

  override def references: AttributeSet =
    AttributeSet(lookupParameters.flatMap(_.references)) ++
      residualCondition.map(_.references.intersect(child.outputSet)).getOrElse(AttributeSet.empty)

  override lazy val customMetrics: Map[String, SQLMetric] =
    createCustomMetrics(scan.supportedCustomMetrics())

  override protected lazy val sparkMetrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "external lookup input rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "external lookup input batches"),
    "batchFillTime" ->
      SQLMetrics.createNanoTimingMetric(sparkContext, "external lookup batch fill time"),
    "numLookupRequests" -> SQLMetrics.createMetric(sparkContext, "lookup requests"),
    "numLookupParameterRows" ->
      SQLMetrics.createMetric(sparkContext, "lookup parameter rows"),
    "numCacheHits" -> SQLMetrics.createMetric(sparkContext, "cache hits"),
    "lookupTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "external lookup time"))

  override protected def doExecute(): RDD[InternalRow] = {
    postDriverMetrics(scan.reportDriverMetrics())
    val boundParameters = lookupParameters
    val boundNullSafeParameters = nullSafeParameters
    val leftOutput = child.output
    val rightOutput = lookupOutput
    val rightTransforms = lookupTransforms
    val outputAttrs = output
    val lookupFactory = readerFactory
    val requestBatchSize = math.min(EngineBatchSize, maxParameterRowsPerRequest)
    val useCache = cacheOptions.enabled
    val cacheMaxEntries = cacheOptions.maxEntries
    val cacheTtlNanos =
      if (cacheOptions.ttlMillis == 0) 0L
      else TimeUnit.MILLISECONDS.toNanos(cacheOptions.ttlMillis)
    val cacheMaxRowsPerKey = cacheOptions.maxRowsPerKey
    val join = joinType
    val condition = residualCondition
    val outputRows = longMetric("numOutputRows")
    val inputRowCount = longMetric("numInputRows")
    val inputBatches = longMetric("numInputBatches")
    val batchFillTime = longMetric("batchFillTime")
    val requests = longMetric("numLookupRequests")
    val lookupParameterRows = longMetric("numLookupParameterRows")
    val cacheHits = longMetric("numCacheHits")
    val lookupTime = longMetric("lookupTime")

    child.execute().mapPartitionsInternal { input =>
      var reader: ExternalLookupReader = null
      val closed = new AtomicBoolean(false)
      var activeCursor: ExternalLookupResultCursor = null
      var resultRowsSinceMetricsUpdate = 0
      def getOrCreateReader(): ExternalLookupReader = {
        if (reader == null) reader = lookupFactory.createReader()
        reader
      }
      def updateCustomMetrics(): Unit = {
        if (reader != null) {
          CustomMetrics.updateMetrics(reader.currentMetricsValues().toSeq, customMetrics)
        }
        resultRowsSinceMetricsUpdate = 0
      }
      def closeActiveCursor(): Unit = if (activeCursor != null) {
        val cursor = activeCursor
        activeCursor = null
        cursor.close()
      }
      def closeReader(): Unit = if (closed.compareAndSet(false, true)) {
        ExternalLookupJoinExec.closeResources(
          closeActiveCursor(), updateCustomMetrics(), if (reader != null) reader.close())
      }
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => closeReader()))

      val parameterProjection = UnsafeProjection.create(boundParameters, leftOutput)
      val semanticParameterProjection = UnsafeProjection.create(boundParameters.map { expression =>
        NormalizeFloatingNumbers.normalize(CollationKey.injectCollationKey(expression))
      }, leftOutput)
      require(boundParameters.length == boundNullSafeParameters.length,
        "Every external lookup parameter must define its null semantics")
      def hasUnmatchableNull(parameters: InternalRow): Boolean =
        boundNullSafeParameters.indices.exists { index =>
          !boundNullSafeParameters(index) && parameters.isNullAt(index)
        }
      val lookupStages = rightTransforms.map {
        case ExternalLookupProject(projectList, stageInput) =>
          Left(UnsafeProjection.create(projectList, stageInput))
        case ExternalLookupFilter(condition, stageInput) =>
          Right(Predicate.create(condition, stageInput))
      }
      val partitionIndex = TaskContext.getPartitionId()
      parameterProjection.initialize(partitionIndex)
      semanticParameterProjection.initialize(partitionIndex)
      lookupStages.foreach {
        case Left(project) => project.initialize(partitionIndex)
        case Right(predicate) => predicate.initialize(partitionIndex)
      }
      def transformLookupValue(value: InternalRow): Option[InternalRow] = {
        lookupStages.foldLeft(Option(value)) {
          case (Some(row), Left(project)) => Some(project(row).copy())
          case (Some(row), Right(predicate)) if predicate.eval(row) => Some(row)
          case _ => None
        }
      }
      val joinedRightInput = if (join == LeftOuter) {
        rightOutput.map(_.withNullability(true))
      } else {
        rightOutput
      }
      val joinedInput = leftOutput ++ joinedRightInput
      val joinedRow = new JoinedRow()
      val resultPredicate = condition.map(Predicate.create(_, joinedInput))
      val resultProjection = UnsafeProjection.create(outputAttrs, joinedInput)
      resultPredicate.foreach(_.initialize(partitionIndex))
      resultProjection.initialize(partitionIndex)
      val nullRight = new GenericInternalRow(rightOutput.length)
      val cache = if (useCache) {
        new JLinkedHashMap[UnsafeRow, ExternalLookupCacheEntry](16, 0.75f, true) {
          override def removeEldestEntry(
              eldest: JMap.Entry[UnsafeRow, ExternalLookupCacheEntry]): Boolean =
            size() > cacheMaxEntries
        }
      } else {
        null
      }
      def getCached(key: UnsafeRow): ExternalLookupCacheEntry = {
        val cached = cache.get(key)
        if (cached != null && cached.isExpired(
            cacheTtlNanos, ExternalLookupJoinExec.cacheTimeNanos())) {
          cache.remove(key)
          null
        } else {
          cached
        }
      }

      def processRows(inputRows: Seq[InternalRow]): Iterator[InternalRow] = {
        inputRowCount += inputRows.size
        inputBatches += 1
        val pending = mutable.Queue.empty[InternalRow]
        val rows = inputRows.map { row =>
          (row, parameterProjection(row).copy(), semanticParameterProjection(row).copy())
        }

        val misses = mutable.LinkedHashMap.empty[UnsafeRow, UnsafeRow]
        val cachedValues = mutable.HashMap.empty[UnsafeRow, Array[InternalRow]]
        rows.foreach { case (_, requestParameters, semanticParameters) =>
          if (!hasUnmatchableNull(requestParameters)) {
            val cached = if (useCache) getCached(semanticParameters) else null
            if (cached != null) {
              cachedValues.put(semanticParameters, cached.values)
              cacheHits += 1
            } else {
              misses.getOrElseUpdate(semanticParameters, requestParameters)
            }
          }
        }

        val semanticParameters = misses.keys.toArray
        val requestedParameters = misses.values.toArray
        val rowsByParameters = rows.indices.groupBy(index => rows(index)._3)
        val matched = Array.fill(rows.length)(false)

        def emitMatch(index: Int, right: InternalRow): Unit = {
          val left = rows(index)._1
          joinedRow(left, right)
          if (resultPredicate.forall(_.eval(joinedRow))) {
            val firstMatch = !matched(index)
            matched(index) = true
            join match {
              case Inner | LeftOuter =>
                pending.enqueue(resultProjection(joinedRow).copy())
                outputRows += 1
              case LeftSemi if firstMatch =>
                pending.enqueue(left)
                outputRows += 1
              case LeftSemi | LeftAnti =>
              case _ =>
            }
          }
        }

        def finishRow(index: Int): Unit = {
          val left = rows(index)._1
          if (!matched(index)) {
            join match {
              case LeftOuter =>
                joinedRow(left, nullRight)
                pending.enqueue(resultProjection(joinedRow).copy())
                outputRows += 1
              case LeftAnti =>
                pending.enqueue(left)
                outputRows += 1
              case _ =>
            }
          }
        }

        val completedRows = mutable.BitSet.empty
        cachedValues.foreach { case (key, values) =>
          rowsByParameters.getOrElse(key, Seq.empty).foreach { index =>
            values.foreach(emitMatch(index, _))
            finishRow(index)
            completedRows += index
          }
        }
        rows.indices.filter(index => hasUnmatchableNull(rows(index)._2)).foreach { index =>
          finishRow(index)
          completedRows += index
        }

        val cacheBuffers =
          Array.fill(semanticParameters.length)(mutable.ArrayBuffer.empty[InternalRow])
        val cacheable = Array.fill(semanticParameters.length)(useCache)
        val lookupStartNanos = System.nanoTime()
        var cursorFinished = requestedParameters.isEmpty
        if (requestedParameters.nonEmpty) {
          requests += 1
          lookupParameterRows += requestedParameters.length
          activeCursor = Option(
            getOrCreateReader().lookupBatch(
              requestedParameters.asInstanceOf[Array[InternalRow]])).getOrElse {
            throw new SparkIllegalStateException(
              errorClass = "INTERNAL_ERROR",
              messageParameters = Map(
                "message" -> "External lookup returned a null result cursor"))
          }
        }

        def finishCursor(): Unit = if (!cursorFinished) {
          cursorFinished = true
          try {
            closeActiveCursor()
          } finally {
            lookupTime += System.nanoTime() - lookupStartNanos
            updateCustomMetrics()
          }
          val cacheWriteTimeNanos = ExternalLookupJoinExec.cacheTimeNanos()
          semanticParameters.indices.foreach { ordinal =>
            val key = semanticParameters(ordinal)
            if (cacheable(ordinal)) {
              cache.put(
                key.copy(),
                ExternalLookupCacheEntry(cacheBuffers(ordinal).toArray, cacheWriteTimeNanos))
            }
            rowsByParameters.getOrElse(key, Seq.empty).foreach { index =>
              if (!completedRows.contains(index)) {
                finishRow(index)
                completedRows += index
              }
            }
          }
        }

        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            fillPending()
            pending.nonEmpty
          }

          override def next(): InternalRow = {
            fillPending()
            if (pending.isEmpty) throw new NoSuchElementException("external lookup result")
            pending.dequeue()
          }

          private def fillPending(): Unit = {
            try {
              while (pending.isEmpty && !cursorFinished) {
                if (activeCursor.next()) {
                  resultRowsSinceMetricsUpdate += 1
                  if (resultRowsSinceMetricsUpdate >= CustomMetrics.NUM_ROWS_PER_UPDATE) {
                    updateCustomMetrics()
                  }
                  val ordinal = activeCursor.inputOrdinal()
                  if (ordinal < 0 || ordinal >= requestedParameters.length) {
                    throw new SparkIllegalStateException(
                      errorClass = "INTERNAL_ERROR",
                      messageParameters = Map("message" ->
                        (s"External lookup returned invalid input ordinal $ordinal for " +
                          s"${requestedParameters.length} requested parameter rows")))
                  }
                  transformLookupValue(activeCursor.get()).foreach { value =>
                    if (cacheable(ordinal)) {
                      if (cacheBuffers(ordinal).length < cacheMaxRowsPerKey) {
                        cacheBuffers(ordinal) += value.copy()
                      } else {
                        cacheable(ordinal) = false
                        cacheBuffers(ordinal).clear()
                      }
                    }
                    rowsByParameters.getOrElse(semanticParameters(ordinal), Seq.empty)
                      .foreach(emitMatch(_, value))
                  }
                } else {
                  finishCursor()
                }
              }
            } catch {
              case error: Throwable =>
                Utils.tryWithSafeFinally(throw error) {
                  finishCursor()
                }
            }
          }
        }
      }

      new Iterator[InternalRow] {
        private var pending: Iterator[InternalRow] = Iterator.empty
        private var finished = false

        override def hasNext: Boolean = {
          fillPending()
          pending.hasNext
        }

        override def next(): InternalRow = {
          fillPending()
          if (!pending.hasNext) {
            throw new SparkIllegalStateException(
              errorClass = "INTERNAL_ERROR",
              messageParameters = Map("message" -> "External lookup join exhausted"))
          }
          pending.next()
        }

        private def fillPending(): Unit = {
          while (!pending.hasNext && !finished) {
            val rows = mutable.ArrayBuffer.empty[InternalRow]
            val fillStartNanos = System.nanoTime()
            while (rows.length < requestBatchSize && input.hasNext) rows += input.next().copy()
            if (rows.nonEmpty) {
              batchFillTime += System.nanoTime() - fillStartNanos
              pending = processRows(rows.toSeq)
            }
            if (!input.hasNext) {
              finished = true
            }
          }
          if (finished && !pending.hasNext) closeReader()
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ExternalLookupJoinExec =
    copy(child = newChild)

  override def simpleString(maxFields: Int): String = {
    Utils.redact(conf.stringRedactionPattern, s"$nodeName ${scan.description()}")
  }

  override protected def stringArgs: Iterator[Any] =
    Iterator(child, simpleString(conf.maxToStringFields))
}

object ExternalLookupJoinExec {
  @volatile private[sql] var cacheTimeNanos: () => Long = () => System.nanoTime()

  private[sql] def withCacheTimeNanos[T](timeNanos: () => Long)(body: => T): T = {
    val previous = cacheTimeNanos
    cacheTimeNanos = timeNanos
    try {
      body
    } finally {
      cacheTimeNanos = previous
    }
  }

  private[sql] def closeResources(
      closeCursor: => Unit,
      updateMetrics: => Unit,
      closeReader: => Unit): Unit = {
    Utils.tryWithSafeFinally(closeCursor) {
      Utils.tryWithSafeFinally(updateMetrics) {
        closeReader
      }
    }
  }
}

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
package org.apache.spark.sql.execution.datasources.v2.python

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReader, ExternalLookupReaderFactory, ExternalLookupResultCursor, LookupParameter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

private[python] case class PythonExternalLookup(
    source: UserDefinedPythonDataSource,
    pickledLookupFunc: Array[Byte],
    selectedPredicates: Seq[Predicate],
    outputSchema: StructType,
    maxParameterRows: Int,
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String]) extends ExternalLookup {

  override def maxParameterRowsPerRequest(): Int = maxParameterRows

  override def createReaderFactory(): ExternalLookupReaderFactory = {
    PythonExternalLookupReaderFactory(
      source,
      pickledLookupFunc,
      selectedPredicates,
      outputSchema,
      jobArtifactUUID,
      sessionUUID)
  }
}

private case class PythonExternalLookupReaderFactory(
    source: UserDefinedPythonDataSource,
    pickledLookupFunc: Array[Byte],
    selectedPredicates: Seq[Predicate],
    outputSchema: StructType,
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String]) extends ExternalLookupReaderFactory {

  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    private[this] val metrics: Map[String, SQLMetric] = PythonCustomMetric.pythonMetrics
    private var activeCursor: PythonExternalLookupResultCursor = _
    private val closed = new AtomicBoolean(false)
    private val cancelWorker = new AtomicReference[Option[() => Unit]](None)

    TaskContext.get().addTaskInterruptListener { (_, _) => cancelActiveWorker() }

    private def cancelActiveWorker(): Unit = {
      cancelWorker.getAndSet(None).foreach(_.apply())
    }

    private def registerWorkerCancellation(cancel: () => Unit): Unit = {
      cancelWorker.set(Some(cancel))
      TaskContext.get().addTaskCompletionListener[Unit] { _ => cancel() }
      if (closed.get() || TaskContext.get().isInterrupted()) {
        cancelActiveWorker()
      }
    }

    private val parameters = PythonExternalLookupSchema.parameters(selectedPredicates)
    private val inputSchema = PythonExternalLookupSchema.inputSchema(parameters)
    private val pythonOutputSchema = PythonExternalLookupSchema.outputSchema(outputSchema)

    private lazy val evaluatorFactory = source.createMapInBatchEvaluatorFactory(
      pickledLookupFunc,
      "lookup_from_data_source",
      inputSchema,
      pythonOutputSchema,
      metrics,
      jobArtifactUUID,
      sessionUUID,
      registerWorkerCancellation)

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      require(!closed.get(), "Python external lookup reader is closed")
      if (activeCursor != null) {
        activeCursor.close()
      }
      val inputRows = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val values = new Array[Any](inputSchema.length)
        values(0) = ordinal
        parameters.indices.foreach { index =>
          val dataType = parameters(index).dataType()
          values(index + 1) = if (key.isNullAt(index)) null else key.get(index, dataType)
        }
        InternalRow.fromSeq(values.toIndexedSeq)
      }
      activeCursor = new PythonExternalLookupResultCursor(
        evaluatorFactory.createEvaluator().eval(TaskContext.getPartitionId(), inputRows),
        outputSchema,
        () => cancelActiveWorker())
      activeCursor
    }

    override def currentMetricsValues(): Array[CustomTaskMetric] = {
      source.createPythonTaskMetrics(metrics.map { case (name, metric) => name -> metric.value})
    }

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        try {
          cancelActiveWorker()
        } finally {
          if (activeCursor != null) {
            activeCursor.close()
            activeCursor = null
          }
        }
      }
    }
  }
}

private object PythonExternalLookupSchema {
  val InputOrdinalColumnName = "input_ordinal"

  def parameters(selectedPredicates: Seq[Predicate]): Seq[LookupParameter] = {
    def collect(expression: V2Expression): Seq[LookupParameter] = expression match {
      case parameter: LookupParameter => Seq(parameter)
      case other => other.children().toIndexedSeq.flatMap(collect)
    }
    selectedPredicates.flatMap(collect)
  }

  def inputSchema(parameters: Seq[LookupParameter]): StructType = {
    StructType(
      StructField(InputOrdinalColumnName, IntegerType, nullable = false) +:
        parameters.zipWithIndex.map { case (parameter, index) =>
          StructField(s"parameter_$index", parameter.dataType(), nullable = true)
        })
  }

  def outputSchema(outputSchema: StructType): StructType = {
    StructType(
      StructField(InputOrdinalColumnName, IntegerType, nullable = false) +:
        outputSchema.fields.toIndexedSeq)
  }
}

private class PythonExternalLookupResultCursor(
    private var outputIterator: Iterator[InternalRow],
    outputSchema: StructType,
    cancelWorker: () => Unit) extends ExternalLookupResultCursor {

  private var current: InternalRow = _
  private val closed = new AtomicBoolean(false)
  private val valueProjection = if (outputSchema.isEmpty) {
    null
  } else {
    UnsafeProjection.create(
      outputSchema.fields.zipWithIndex.map { case (field, index) =>
        BoundReference(index + 1, field.dataType, field.nullable)
      }.toIndexedSeq)
  }

  override def next(): Boolean = {
    if (outputIterator.hasNext) {
      current = outputIterator.next()
      require(!current.isNullAt(0), "Python external lookup returned a null input_ordinal")
      true
    } else {
      close()
      false
    }
  }

  override def inputOrdinal(): Int = current.getInt(0)

  override def get(): InternalRow = {
    if (outputSchema.isEmpty) InternalRow.empty else valueProjection(current)
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try {
        cancelWorker()
      } finally {
        current = null
        outputIterator = Iterator.empty
      }
    }
  }
}

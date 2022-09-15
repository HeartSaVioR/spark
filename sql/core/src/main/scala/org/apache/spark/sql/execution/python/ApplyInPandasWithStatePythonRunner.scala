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

package org.apache.spark.sql.execution.python

import java.io._

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.api.python._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.python.ApplyInPandasWithStatePythonRunner.{InType, OutType, OutTypeForState, STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER}
import org.apache.spark.sql.execution.python.ApplyInPandasWithStateWriter.STATE_METADATA_SCHEMA
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}


/**
 * [[ArrowPythonRunner]] with [[org.apache.spark.sql.streaming.GroupState]].
 */
class ApplyInPandasWithStatePythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    inputSchema: StructType,
    override protected val timeZoneId: String,
    initialWorkerConf: Map[String, String],
    stateEncoder: ExpressionEncoder[Row],
    keySchema: StructType,
    valueSchema: StructType,
    stateValueSchema: StructType,
    softLimitBytesPerBatch: Long,
    minDataCountForSample: Int,
    softTimeoutMillsPurgeBatch: Long)
  extends BasePythonRunner[InType, OutType](funcs, evalType, argOffsets)
  with PythonArrowInput[InType]
  with PythonArrowOutput[OutType] {

  override protected val schema: StructType = inputSchema.add("!__state__!", STATE_METADATA_SCHEMA)

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  override protected val workerConf: Map[String, String] = initialWorkerConf +
    (SQLConf.MAP_PANDAS_UDF_WITH_STATE_SOFT_LIMIT_SIZE_PER_BATCH.key ->
      softLimitBytesPerBatch.toString) +
    (SQLConf.MAP_PANDAS_UDF_WITH_STATE_MIN_DATA_COUNT_FOR_SAMPLE.key ->
      minDataCountForSample.toString) +
    (SQLConf.MAP_PANDAS_UDF_WITH_STATE_SOFT_TIMEOUT_PURGE_BATCH.key ->
      softTimeoutMillsPurgeBatch.toString)

  private val stateRowDeserializer = stateEncoder.createDeserializer()

  override protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    super.handleMetadataBeforeExec(stream)
    // Also write the schema for state value
    PythonRDD.writeUTF(stateValueSchema.json, stream)
  }

  protected def writeIteratorToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[InType]): Unit = {
    val w = new ApplyInPandasWithStateWriter(root, writer, softLimitBytesPerBatch,
      minDataCountForSample, softTimeoutMillsPurgeBatch)

    while (inputIterator.hasNext) {
      val (keyRow, groupState, dataIter) = inputIterator.next()
      assert(dataIter.hasNext, "should have at least one data row!")
      w.startNewGroup(keyRow, groupState)

      while (dataIter.hasNext) {
        val dataRow = dataIter.next()
        w.writeRow(dataRow)
      }

      w.finalizeGroup()
    }

    w.finalizeData()
  }

  protected def deserializeColumnarBatch(batch: ColumnarBatch, schema: StructType): OutType = {
    // This should at least have one row for state. Also, we ensure that all columns across
    // data and state metadata have same number of rows, which is required by Arrow record
    // batch.
    assert(batch.numRows() > 0)
    assert(schema.length == 2)

    def getColumnarBatchForStructTypeColumn(
        batch: ColumnarBatch,
        ordinal: Int,
        expectedType: StructType): ColumnarBatch = {
      //  UDF returns a StructType column in ColumnarBatch, select the children here
      val structVector = batch.column(ordinal).asInstanceOf[ArrowColumnVector]
      val dataType = schema(ordinal).dataType.asInstanceOf[StructType]
      assert(dataType.sameType(expectedType))

      val outputVectors = dataType.indices.map(structVector.getChild)
      val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
      flattenedBatch.setNumRows(batch.numRows())

      flattenedBatch
    }

    def constructIterForData(batch: ColumnarBatch): Iterator[InternalRow] = {
      val dataBatch = getColumnarBatchForStructTypeColumn(batch, 0, valueSchema)
      dataBatch.rowIterator.asScala.flatMap { row =>
        if (row.isNullAt(0)) {
          // The entire row in record batch seems to be for state metadata.
          None
        } else {
          Some(row)
        }
      }
    }

    def constructIterForState(batch: ColumnarBatch): Iterator[OutTypeForState] = {
      val stateMetadataBatch = getColumnarBatchForStructTypeColumn(batch, 1,
        STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER)

      stateMetadataBatch.rowIterator().asScala.flatMap { row =>
        implicit val formats = org.json4s.DefaultFormats

        if (row.isNullAt(0)) {
          // The entire row in record batch seems to be for data.
          None
        } else {
          // NOTE: See StateReaderIterator.STATE_METADATA_SCHEMA for the schema.
          val propertiesAsJson = parse(row.getUTF8String(0).toString)
          val keyRowAsUnsafeAsBinary = row.getBinary(1)
          val keyRowAsUnsafe = new UnsafeRow(keySchema.fields.length)
          keyRowAsUnsafe.pointTo(keyRowAsUnsafeAsBinary, keyRowAsUnsafeAsBinary.length)
          val maybeObjectRow = if (row.isNullAt(2)) {
            None
          } else {
            val pickledStateValue = row.getBinary(2)
            Some(PythonSQLUtils.toJVMRow(pickledStateValue, stateValueSchema,
              stateRowDeserializer))
          }
          val oldTimeoutTimestamp = row.getLong(3)

          Some((keyRowAsUnsafe, GroupStateImpl.fromJson(maybeObjectRow, propertiesAsJson),
            oldTimeoutTimestamp))
        }
      }
    }

    (constructIterForState(batch), constructIterForData(batch))
  }
}

object ApplyInPandasWithStatePythonRunner {
  type InType = (UnsafeRow, GroupStateImpl[Row], Iterator[InternalRow])
  type OutTypeForState = (UnsafeRow, GroupStateImpl[Row], Long)
  type OutType = (Iterator[OutTypeForState], Iterator[InternalRow])

  val STATE_METADATA_SCHEMA_FROM_PYTHON_WORKER: StructType = StructType(
    Array(
      StructField("properties", StringType),
      StructField("keyRowAsUnsafe", BinaryType),
      StructField("object", BinaryType),
      StructField("oldTimeoutTimestamp", LongType)
    )
  )
}

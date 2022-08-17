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
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.python.PythonSQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils


// FIXME: schema should account to the state data as well... Use apply?
// FIXME: argOffsets should account to the state field as well... Use apply?
/*
  val schemaWithState: StructType = schema.add(
    "!__state__!", StructType(
      Array(
        StructField("properties", StringType),
        StructField("keySchema", StringType),
        StructField("keyRow", BinaryType),
        StructField("objectSchema", StringType),
        StructField("object", BinaryType)
      )
    )
  )
  val argOffsetsWithState: Array[Array[Int]] = Array(argOffsets(0) :+ schemaWithState.length - 1)
 */

object ArrowPythonRunnerWithState {
  def schemaWithState(schema: StructType): StructType = {
    schema.add("!__state__!",
      StructType(
        Array(
          StructField("properties", StringType),
          StructField("keySchema", StringType),
          StructField("keyRow", BinaryType),
          StructField("objectSchema", StringType),
          StructField("object", BinaryType)
        )
      )
    )
  }

  def argOffsetsWithState(
      schema: StructType,
      argOffsets: Array[Array[Int]]): Array[Array[Int]] = {
    val argOffsetsForFn = argOffsets(0)
    val newArgOffsetsForFn = argOffsetsForFn :+ schemaWithState(schema).length - 1
    Array(newArgOffsetsForFn)
  }
}

/**
 * [[ArrowPythonRunner]] with [[org.apache.spark.sql.streaming.GroupState]].
 */
class ArrowPythonRunnerWithState(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    inputSchema: StructType,
    timeZoneId: String,
    workerConf: Map[String, String],
    keyEncoder: ExpressionEncoder[Row],
    stateEncoder: ExpressionEncoder[Row],
    keySchema: StructType,
    valueSchema: StructType,
    stateSchema: StructType)
  extends BasePythonRunner[
    (InternalRow, GroupStateImpl[Row], Iterator[InternalRow]),
    (InternalRow, GroupStateImpl[Row], Iterator[InternalRow])](
    // FIXME: move to apply?
    funcs, evalType, ArrowPythonRunnerWithState.argOffsetsWithState(inputSchema, argOffsets)) {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  // FIXME: move to apply?
  val schemaWithState = ArrowPythonRunnerWithState.schemaWithState(inputSchema)

  val keyRowSerializer = keyEncoder.createSerializer()
  val keyRowDeserializer = keyEncoder.createDeserializer()
  val stateRowSerializer = stateEncoder.createSerializer()
  val stateRowDeserializer = stateEncoder.createDeserializer()

  protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    stream.writeInt(workerConf.size)
    for ((k, v) <- workerConf) {
      PythonRDD.writeUTF(k, stream)
      PythonRDD.writeUTF(v, stream)
    }
  }

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[(InternalRow, GroupStateImpl[Row], Iterator[InternalRow])],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        handleMetadataBeforeExec(dataOut)
        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      private def buildStateInfoRow(
          keyRow: InternalRow,
          groupState: GroupStateImpl[Row]): InternalRow = {
        val keyRowAsPublicRow = keyRowDeserializer.apply(keyRow)
        val stateUnderlyingRow = new GenericInternalRow(
          Array[Any](
            groupState.json(),
            keySchema.json,
            PythonSQLUtils.toPyRow(keyRowAsPublicRow),
            stateSchema.json,
            groupState.getOption.map(PythonSQLUtils.toPyRow).orNull
          )
        )
        new GenericInternalRow(Array[Any](stateUnderlyingRow))
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val arrowSchema = ArrowUtils.toArrowSchema(schemaWithState, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val nullDataRow = new GenericInternalRow(Array.fill(inputSchema.length)(null: Any))
          val nullStateInfoRow = new GenericInternalRow(Array.fill(1)(null: Any))

          val arrowWriter = ArrowWriter.create(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          val joinedRow = new JoinedRow
          while (inputIterator.hasNext) {
            val (keyRow, groupState, dataIter) = inputIterator.next()

            // Provide state info row in the first row
            val stateInfoRow = buildStateInfoRow(keyRow, groupState)
            joinedRow.withLeft(nullDataRow).withRight(stateInfoRow)
            arrowWriter.write(joinedRow)

            // Continue providing remaining data rows
            while (dataIter.hasNext) {
              val dataRow = dataIter.next()
              joinedRow.withLeft(dataRow).withRight(nullStateInfoRow)
              arrowWriter.write(joinedRow)
            }

            arrowWriter.finish()
            writer.writeBatch()
            arrowWriter.reset()
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          // If we close root and allocator in TaskCompletionListener, there could be a race
          // condition where the writer thread keeps writing to the VectorSchemaRoot while
          // it's being closed by the TaskCompletion listener.
          // Closing root and allocator here is cleaner because root and allocator is owned
          // by the writer thread and is only visible to the writer thread.
          //
          // If the writer thread is interrupted by TaskCompletionListener, it should either
          // (1) in the try block, in which case it will get an InterruptedException when
          // performing io, and goes into the finally block or (2) in the finally block,
          // in which case it will ignore the interruption and close the resources.
          root.close()
          allocator.close()
        }
      }
    }
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[(InternalRow, GroupStateImpl[Row], Iterator[InternalRow])] = {

    new ReaderIterator(
      stream, writerThread, startTime, env, worker, pid, releasedOrClosed, context) {

      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        allocator.close()
      }

      private var batchLoaded = true

      protected override def read(): (InternalRow, GroupStateImpl[Row], Iterator[InternalRow]) = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              deserializeColumnarBatch(batch)
            } else {
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                // FIXME: should we validate schema here with value schema?
                schema = ArrowUtils.fromArrowSchema(root.getSchema())
                vectors = root.getFieldVectors().asScala.map { vector =>
                  new ArrowColumnVector(vector)
                }.toArray[ColumnVector]
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }

      private def deserializeColumnarBatch(
          batch: ColumnarBatch): (InternalRow, GroupStateImpl[Row], Iterator[InternalRow]) = {
        // FIXME: this assumes we have consolidated schema of data and state info, and we want to
        //   split out when giving data to upstream
        assert(schema.length == valueSchema.length + 1)

        val fullAttributes = schema.toAttributes
        val stateInfoAttribute = fullAttributes(valueSchema.length)
        val dataAttributes = fullAttributes.dropRight(1)

        val unsafeProjForStateInfo = UnsafeProjection.create(
          Seq(stateInfoAttribute), fullAttributes)
        val unsafeProjForData = UnsafeProjection.create(
          dataAttributes, fullAttributes)

        //  UDF returns a StructType column in ColumnarBatch, select the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = schema.indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())

        val rowIterator = flattenedBatch.rowIterator.asScala
        // FIXME: first row should be state info row
        assert(rowIterator.hasNext)
        val joinedRowForStateInfo = rowIterator.next()
        val wrappedRowForStateInfo = unsafeProjForStateInfo(joinedRowForStateInfo)

        // FIXME: we rely on known schema for state info, but would we want to access this by
        //  column name?
        /*
        Array(
          StructField("properties", StringType),
          StructField("keySchema", StringType),
          StructField("keyRow", BinaryType),
          StructField("objectSchema", StringType),
          StructField("object", BinaryType)
        )
        */
        val rowForStateInfo = wrappedRowForStateInfo.getStruct(0, 5)

        implicit val formats = org.json4s.DefaultFormats

        val propertiesAsJson = parse(rowForStateInfo.getUTF8String(0).toString)
        // FIXME: keySchema is probably not needed as we already know it... let's check whether
        //   it is needed for python worker, and if it does not, remove it.
        val pickledKeyRow = rowForStateInfo.getBinary(2)
        val keyRowAsGenericRow = PythonSQLUtils.toJVMRow(pickledKeyRow, keySchema,
          keyRowDeserializer)
        val keyRowAsInternalRow = keyRowSerializer.apply(keyRowAsGenericRow)
        val maybeObjectRow = if (rowForStateInfo.isNullAt(4)) {
          None
        } else {
          val pickledRow = rowForStateInfo.getBinary(4)
          Some(PythonSQLUtils.toJVMRow(pickledRow, stateSchema, stateRowDeserializer))
        }

        val newGroupState = GroupStateImpl.fromJson(maybeObjectRow, propertiesAsJson)

        (keyRowAsInternalRow, newGroupState, rowIterator.map(unsafeProjForData))
      }
    }
  }
}

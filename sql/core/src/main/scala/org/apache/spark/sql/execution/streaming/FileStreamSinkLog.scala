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

import java.io.{DataInputStream, DataOutputStream, InputStream, IOException, OutputStream}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.io.{Source => IOSource}

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.{FileStatus, Path}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The status of a file outputted by [[FileStreamSink]]. A file is visible only if it appears in
 * the sink log and its action is not "delete".
 *
 * @param path the file path.
 * @param size the file size.
 * @param isDir whether this file is a directory.
 * @param modificationTime the file last modification time.
 * @param blockReplication the block replication.
 * @param blockSize the block size.
 * @param action the file action. Must be either "add" or "delete".
 */
case class SinkFileStatus(
    path: String,
    size: Long,
    isDir: Boolean,
    modificationTime: Long,
    blockReplication: Int,
    blockSize: Long,
    action: String) {

  def toFileStatus: FileStatus = {
    new FileStatus(
      size, isDir, blockReplication, blockSize, modificationTime, new Path(new URI(path)))
  }
}

object SinkFileStatus {
  def apply(f: FileStatus): SinkFileStatus = {
    SinkFileStatus(
      path = f.getPath.toUri.toString,
      size = f.getLen,
      isDir = f.isDirectory,
      modificationTime = f.getModificationTime,
      blockReplication = f.getReplication,
      blockSize = f.getBlockSize,
      action = FileStreamSinkLog.ADD_ACTION)
  }
}

object SinkFileStatusV2 {
  val SCHEMA = new StructType(
    Array(
      StructField("path", StringType),
      StructField("size", LongType),
      StructField("isDir", BooleanType),
      StructField("modificationTime", LongType),
      StructField("blockReplication", IntegerType),
      StructField("blockSize", LongType),
      StructField("action", StringType)
    )
  )

  val PROJ_UNSAFE_ROW = UnsafeProjection.create(SCHEMA.fields.map(_.dataType))

  def fromRow(row: UnsafeRow): SinkFileStatus = {
    SinkFileStatus(
      row.getString(0),
      row.getLong(1),
      row.getBoolean(2),
      row.getLong(3),
      row.getInt(4),
      row.getLong(5),
      row.getString(6)
    )
  }

  def toRow(entry: SinkFileStatus): UnsafeRow = {
    val row = new GenericInternalRow(Array[Any](
      UTF8String.fromString(entry.path), entry.size, entry.isDir, entry.modificationTime,
      entry.blockReplication, entry.blockSize, UTF8String.fromString(entry.action)))
    PROJ_UNSAFE_ROW.apply(row).copy()
  }
}

/**
 * A special log for [[FileStreamSink]]. It will write one log file for each batch. The first line
 * of the log file is the version number, and there are multiple JSON lines following. Each JSON
 * line is a JSON format of [[SinkFileStatus]].
 *
 * As reading from many small files is usually pretty slow, [[FileStreamSinkLog]] will compact log
 * files every "spark.sql.sink.file.log.compactLen" batches into a big file. When doing a
 * compaction, it will read all old log files and merge them with the new batch. During the
 * compaction, it will also delete the files that are deleted (marked by [[SinkFileStatus.action]]).
 * When the reader uses `allFiles` to list all files, this method only returns the visible files
 * (drops the deleted files).
 */
class FileStreamSinkLog(
    metadataLogVersion: Int,
    sparkSession: SparkSession,
    path: String)
  extends CompactibleFileStreamLog[SinkFileStatus](metadataLogVersion, sparkSession, path) {

  private implicit val formats = Serialization.formats(NoTypeHints)

  protected override val fileCleanupDelayMs = sparkSession.sessionState.conf.fileSinkLogCleanupDelay

  protected override val isDeletingExpiredLog = sparkSession.sessionState.conf.fileSinkLogDeletion

  protected override val defaultCompactInterval =
    sparkSession.sessionState.conf.fileSinkLogCompactInterval

  private val sparkConf = sparkSession.sparkContext.getConf

  require(defaultCompactInterval > 0,
    s"Please set ${SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key} (was $defaultCompactInterval) " +
      "to a positive value.")

  override def compactLogs(logs: Seq[SinkFileStatus]): Seq[SinkFileStatus] = {
    val deletedFiles = logs.filter(_.action == FileStreamSinkLog.DELETE_ACTION).map(_.path).toSet
    if (deletedFiles.isEmpty) {
      logs
    } else {
      logs.filter(f => !deletedFiles.contains(f.path))
    }
  }

  override def dataToUnsafeRow(data: SinkFileStatus): UnsafeRow = SinkFileStatusV2.toRow(data)

  override def unsafeRowToData(row: UnsafeRow): SinkFileStatus = SinkFileStatusV2.fromRow(row)

  override def numFieldsForUnsafeRow: Int = SinkFileStatusV2.SCHEMA.fields.length
}

object FileStreamSinkLog {
  val VERSION = 2
  val DELETE_ACTION = "delete"
  val ADD_ACTION = "add"
}

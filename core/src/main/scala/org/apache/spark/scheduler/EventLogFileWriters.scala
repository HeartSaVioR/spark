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

package org.apache.spark.scheduler

import java.io._
import java.net.URI

import scala.collection.mutable.Map

import org.apache.commons.compress.utils.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

/**
 * The base class of writer which will write event logs into file.
 *
 * The following configurable parameters are available to tune the behavior of writing:
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.compression.codec - The codec to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 *
 * Note that descendant classes can maintain its own parameters: refer the javadoc of each class
 * for more details.
 *
 * NOTE: CountingOutputStream being returned by "initLogFile" counts "non-compressed" bytes.
 */
abstract class EventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  protected val shouldCompress = sparkConf.get(EVENT_LOG_COMPRESS)
  protected val shouldOverwrite = sparkConf.get(EVENT_LOG_OVERWRITE)
  protected val shouldAllowECLogs = sparkConf.get(EVENT_LOG_ALLOW_EC)
  protected val outputBufferSize = sparkConf.get(EVENT_LOG_OUTPUT_BUFFER_SIZE).toInt
  protected val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  protected val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf, sparkConf.get(EVENT_LOG_COMPRESSION_CODEC)))
    } else {
      None
    }

  private[scheduler] val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  protected def requireLogBaseDirAsDirectory(): Unit = {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }
  }

  protected def initLogFile(path: Path): (Option[FSDataOutputStream],
    Option[CountingOutputStream]) = {

    if (shouldOverwrite && fileSystem.delete(path, true)) {
      logWarning(s"Event log $path already exists. Overwriting...")
    }

    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val uri = path.toUri

    var hadoopDataStream: Option[FSDataOutputStream] = None
    /* The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
     * Therefore, for local files, use FileOutputStream instead. */
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(if (shouldAllowECLogs) {
          fileSystem.create(path)
        } else {
          SparkHadoopUtil.createNonECFile(fileSystem, path)
        })
        hadoopDataStream.get
      }

    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)
      val ostream = new CountingOutputStream(bstream)
      fileSystem.setPermission(path, EventLogFileWriter.LOG_FILE_PERMISSIONS)
      logInfo(s"Logging events to $path")

      (hadoopDataStream, Some(ostream))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  protected def renameFile(src: Path, dest: Path, overwrite: Boolean): Unit = {
    if (fileSystem.exists(dest)) {
      if (overwrite) {
        logWarning(s"Event log $dest already exists. Overwriting...")
        if (!fileSystem.delete(dest, true)) {
          logWarning(s"Error deleting $dest")
        }
      } else {
        throw new IOException(s"Target log file already exists ($dest)")
      }
    }
    fileSystem.rename(src, dest)
    // touch file to ensure modtime is current across those filesystems where rename()
    // does not set it, -and which support setTimes(); it's a no-op on most object stores
    try {
      fileSystem.setTimes(dest, System.currentTimeMillis(), -1)
    } catch {
      case e: Exception => logDebug(s"failed to set time of $dest", e)
    }
  }

  // ================ methods to be override ================

  /** starts writer instance - initialize writer for event logging */
  def start(): Unit

  /** writes JSON format of event to file */
  def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit

  /** stops writer - indicating the application has been completed */
  def stop(): Unit

  /** returns representative path of log */
  def logPath: String
}

object EventLogFileWriter {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"

  val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  def createEventLogFileWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    if (sparkConf.get(EVENT_LOG_ENABLE_ROLLING)) {
      new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    } else {
      new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    }
  }

  def nameForAppAndAttempt(appId: String, appAttemptId: Option[String]): String = {
    val base = Utils.sanitizeDirName(appId)
    if (appAttemptId.isDefined) {
      base + "_" + Utils.sanitizeDirName(appAttemptId.get)
    } else {
      base
    }
  }

  def codecName(log: Path): Option[String] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    logName.split("\\.").tail.lastOption
  }
}

/**
 * The writer to write event logs into single file.
 */
class SingleEventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) with Logging {

  override val logPath: String = SingleEventLogFileWriter.getLogPath(logBaseDir, appId,
    appAttemptId, compressionCodecName)

  private val inProgressPath = logPath + EventLogFileWriter.IN_PROGRESS

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  private var writer: Option[PrintWriter] = None

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    val streams = initLogFile(new Path(inProgressPath))
    hadoopDataStream = streams._1
    if (streams._2.isDefined) {
      writer = Some(new PrintWriter(streams._2.get))
    }
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    // scalastyle:off println
    writer.foreach(_.println(eventJson))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
  }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
   */
  override def stop(): Unit = {
    writer.foreach(_.close())
    renameFile(new Path(inProgressPath), new Path(logPath), shouldOverwrite)
  }
}

object SingleEventLogFileWriter {
  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
   *
   * The log file name will identify the compression codec used for the contents, if any.
   * For example, app_123 for an uncompressed log, app_123.lzf for an LZF-compressed log.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param appAttemptId A unique attempt id of appId. May be the empty string.
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val codec = compressionCodecName.map("." + _).getOrElse("")
    new Path(logBaseDir).toString.stripSuffix("/") + "/" +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId) + codec
  }
}

/**
 * The writer to write event logs into multiple log files, rolled over via configured size.
 *
 * The class creates each directory per application, and stores event log files as well as
 * metadata files. The name of directory and files in the directory would follow:
 *
 * - The name of directory: eventlog_v2_appId(_[appAttemptId])
 * - The prefix of name on event files: events_[sequence]_[appId](_[appAttemptId])(.[codec])
 *   - "sequence" would be monotonically increasing value
 * - The name of metadata (app. status) file name: appstatus_[appId](_[appAttemptId])(.inprogress)
 *
 * The writer will roll over the event log file when configured size is reached. Note that the
 * writer doesn't check the size on file being open for write: the writer tracks the count of bytes
 * being written currently.
 *
 * For metadata files, the class will leverage zero-byte file, as it provides minimized cost.
 *
 * The writer leverages the following configurable parameters:
 *   spark.eventLog.rollLog - Whether rolling over event log files is enabled.
 *   spark.eventLog.rollLog.maxFileSize - The max size of event log file to be rolled over.
 */
class RollingEventLogFilesWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  import RollingEventLogFilesWriter._

  private val eventFileMaxLengthKiB = sparkConf.get(EVENT_LOG_ROLLED_EVENT_LOG_MAX_FILE_SIZE)

  private val logDirForAppPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None
  private var countingOutputStream: Option[CountingOutputStream] = None
  private var writer: Option[PrintWriter] = None

  // seq and event log path will be updated soon in rollNewEventLogFile, which `start` will call
  private var sequence: Long = 0L
  private var currentEventLogFilePath: Path = logDirForAppPath

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    if (fileSystem.exists(logDirForAppPath) && shouldOverwrite) {
      // try to delete the directory
      fileSystem.delete(logDirForAppPath, true)
    }

    if (fileSystem.exists(logDirForAppPath)) {
      // we tried to delete the existing one, but failed
      throw new IOException(s"Target log directory already exists ($logDirForAppPath)")
    } else {
      fileSystem.mkdirs(logDirForAppPath, EventLogFileWriter.LOG_FILE_PERMISSIONS)
      createAppStatusFile(inProgress = true)
      rollNewEventLogFile(None)
    }
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    writer.foreach { w =>
      val currentLen = countingOutputStream.get.getBytesWritten
      if (currentLen + eventJson.length > eventFileMaxLengthKiB * 1024) {
        rollNewEventLogFile(Some(w))
      }
    }

    // if the event log file is rolled over, writer will refer next event log file

    // scalastyle:off println
    writer.foreach(_.println(eventJson))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
  }

  private def rollNewEventLogFile(w: Option[PrintWriter]): Unit = {
    writer.foreach(_.close())

    sequence += 1
    currentEventLogFilePath = getEventLogFilePath(logDirForAppPath, appId, appAttemptId, sequence,
      compressionCodecName)

    val streams = initLogFile(currentEventLogFilePath)
    hadoopDataStream = streams._1
    countingOutputStream = streams._2
    if (countingOutputStream.isDefined) {
      writer = Some(new PrintWriter(streams._2.get))
    } else {
      writer = None
    }
  }

  override def stop(): Unit = {
    writer.foreach(_.close())
    val appStatusPathIncomplete = getAppStatusFilePath(logDirForAppPath, appId, appAttemptId,
      inProgress = true)
    val appStatusPathComplete = getAppStatusFilePath(logDirForAppPath, appId, appAttemptId,
      inProgress = false)
    renameFile(appStatusPathIncomplete, appStatusPathComplete, overwrite = true)
  }

  override def logPath: String = logDirForAppPath.toString

  private def createAppStatusFile(inProgress: Boolean): Unit = {
    val appStatusPath = getAppStatusFilePath(logDirForAppPath, appId, appAttemptId, inProgress)
    val streams = initLogFile(appStatusPath)
    val outputStream = streams._2.get
    // we intentionally create zero-byte file to minimize the cost
    outputStream.close()
  }
}

object RollingEventLogFilesWriter {
  def getAppEventLogDirPath(logBaseDir: URI, appId: String, appAttemptId: Option[String]): Path =
    new Path(new Path(logBaseDir), "eventlog_v2_" +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId))

  def getAppStatusFilePath(
      appLogDir: Path,
      appId: String,
      appAttemptId: Option[String],
      inProgress: Boolean): Path = {
    val base = "appstatus_" + EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId)
    val name = if (inProgress) base + EventLogFileWriter.IN_PROGRESS else base
    new Path(appLogDir, name)
  }

  def getEventLogFilePath(
      appLogDir: Path,
      appId: String,
      appAttemptId: Option[String],
      seq: Long,
      codecName: Option[String]): Path = {
    val base = s"events_${seq}_" + EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId)
    val codec = codecName.map("." + _).getOrElse("")
    new Path(appLogDir, base + codec)
  }

  def isEventLogDir(status: FileStatus): Boolean = {
    status.isDirectory && status.getPath.getName.startsWith("eventlog_v2_")
  }

  def isEventLogFile(status: FileStatus): Boolean = {
    status.isFile && isEventLogFile(status.getPath)
  }

  def isEventLogFile(path: Path): Boolean = {
    path.getName.startsWith("events_")
  }

  def isAppStatusFile(status: FileStatus): Boolean = {
    status.isFile && isAppStatusFile(status.getPath)
  }

  def isAppStatusFile(path: Path): Boolean = {
    path.getName.startsWith("appstatus")
  }

  def getSequence(eventLogFileName: String): Long = {
    require(eventLogFileName.startsWith("events_"), "Not a event log file!")
    val seq = eventLogFileName.stripPrefix("events_").split("_")(0)
    seq.toLong
  }
}

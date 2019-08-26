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
import java.nio.charset.StandardCharsets

import scala.collection.mutable.Map
import org.apache.commons.compress.utils.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

// FIXME: document it!
sealed trait EventLogWriter {
  def start(): Unit
  def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit
  def stop(): Unit
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String
}

abstract class EventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends EventLogWriter with Logging {

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

  protected val compressionCodecName = compressionCodec.map { c =>
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
      if (shouldOverwrite) {
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
}

object EventLogFileWriter {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"

  val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = Map.empty[String, CompressionCodec]

  def createEventLogFileWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogWriter = {
    if (sparkConf.get(EVENT_LOG_ENABLE_ROLLING)) {
      new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    } else {
      new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    }
  }

  /**
    * Opens an event log file and returns an input stream that contains the event data.
    *
    * @return input stream that holds one JSON record per line.
    */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))
    try {
      val codec = codecName(log).map { c =>
        codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
      }
      codec.map(_.compressedContinuousInputStream(in)).getOrElse(in)
    } catch {
      case e: Throwable =>
        in.close()
        throw e
    }
  }

  def codecName(log: Path): Option[String] = {
    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    logName.split("\\.").tail.lastOption
  }
}

class SingleEventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) with Logging {

  private val logPath = getLogPath(logBaseDir, appId, appAttemptId,
    compressionCodecName)

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
  override def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String]): String = {
    SingleEventLogFileWriter.getLogPath(logBaseDir, appId, appAttemptId, compressionCodecName)
  }
}

object SingleEventLogFileWriter {
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String]): String = {
    val base = new Path(logBaseDir).toString.stripSuffix("/") + "/" + Utils.sanitizeDirName(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    if (appAttemptId.isDefined) {
      base + "_" + Utils.sanitizeDirName(appAttemptId.get) + codec
    } else {
      base + codec
    }
  }
}

// FIXME: document how the directory and files look like!
/*
We will name the directory as:

eventlog_v2_appId(_<appAttemptId>)

The name doesn't contain ".inprogress" suffix, as renaming directory in S3 doesn't seem to be possible. Even it would be abstracted and supported in some libraries, it may require all belonging files to be renamed as well which the time complexity of renaming is O(N) where N is the size of the file. HDFS may support it trivially but that's not feasible if we also consider S3.

We will name the event log files as:

events_<sequence>_<appId>(_<appAttemptId>)(.<codec>)

which "sequence" would be monotonically increasing value. So rolled event log files will look like:

events_1_<appId>(_<appAttemptId>)(.<codec>) <<== events_1
events_2_<appId>(_<appAttemptId>)(.<codec>)
events_3_<appId>(_<appAttemptId>)(.<codec>)
...

appstatus_<appId>(_<appAttemptId>)(.inprogress) <<== appstatus(.inprogress)

We will write dummy data (like '1', or Spark version to help diagnose?) in both files, so that it would cost very tiny to rename even in S3. The reason having two files separately is to avoid concurrent renaming, as renaming "appstatus" will happen in driver (EventLoggingListener), whereas renaming "lastsnapshot" will happen in whatever which is in charge of snapshotting.

<< lastsnapshot not required
 */
class RollingEventLogFilesWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  import RollingEventLogFilesWriter._

  private val eventFileMaxLength = sparkConf.get(EVENT_LOG_ROLLED_EVENT_LOG_MAX_FILE_SIZE)

  private val logDirForAppPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None
  private var countingOutputStream: Option[CountingOutputStream] = None
  private var writer: Option[PrintWriter] = None

  // seq and event log path will be updated soon in rollNewEventLogFile, `start` is called
  private var sequence: Long = 0L
  private var currentEventLogFilePath: Path = logDirForAppPath

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    fileSystem.mkdirs(logDirForAppPath, EventLogFileWriter.LOG_FILE_PERMISSIONS)
    createAppStatusFile(inProgress = true)
    rollNewEventLogFile(None)
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    writer.foreach { w =>
      val currentLen = countingOutputStream.get.getBytesWritten
      if (currentLen + eventJson.length > eventFileMaxLength) {
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
    if (w.isDefined) {
      w.get.close()
    }

    sequence += 1
    currentEventLogFilePath = getEventLogFilePath(logDirForAppPath, sequence, compressionCodecName)

    val streams = initLogFile(currentEventLogFilePath)
    hadoopDataStream = streams._1
    countingOutputStream = streams._2
    if (countingOutputStream.isDefined) {
      writer = Some(new PrintWriter(streams._2.get))
    }
  }

  override def stop(): Unit = {
    writer.foreach(_.close())
    val appStatusPathIncomplete = getAppStatusFilePath(logDirForAppPath, inProgress = true)
    val appStatusPathComplete = getAppStatusFilePath(logDirForAppPath, inProgress = false)
    renameFile(appStatusPathIncomplete, appStatusPathComplete, overwrite = true)
  }

  override def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String]): String = {
    logDirForAppPath.toString
  }

  private def createAppStatusFile(inProgress: Boolean): Unit = {
    val appStatusPath = getAppStatusFilePath(logDirForAppPath, inProgress)
    val streams = initLogFile(appStatusPath)
    val outputStream = streams._2.get
    // scalastyle:off println
    outputStream.write(SPARK_VERSION.getBytes(StandardCharsets.UTF_8))
    // scalastyle:on println
    outputStream.close()
  }
}

object RollingEventLogFilesWriter {
  def getAppEventLogDirPath(logBaseDir: URI, appId: String, appAttemptId: Option[String]): Path = {
    val base = "eventlog_v2_" + Utils.sanitizeDirName(appId)
    val dirName = if (appAttemptId.isDefined) {
      base + "_" + Utils.sanitizeDirName(appAttemptId.get)
    } else {
      base
    }
    new Path(new Path(logBaseDir), dirName)
  }

  def getAppStatusFilePath(appLogDir: Path, inProgress: Boolean): Path = {
    val name = if (inProgress) "appstatus" + EventLogFileWriter.IN_PROGRESS else "appstatus"
    new Path(appLogDir, name)
  }

  def getEventLogFilePath(appLogDir: Path, seq: Long, codecName: Option[String]): Path = {
    val codec = codecName.map("." + _).getOrElse("")
    new Path(appLogDir, s"events_$seq$codec")
  }

  def isEventLogFile(status: FileStatus): Boolean = {
    status.isFile && isEventLogFile(status.getPath)
  }

  def isEventLogFile(path: Path): Boolean = {
    path.getName.startsWith("events_")
  }

  def getSequence(eventLogFileName: String): Long = {
    require(eventLogFileName.startsWith("events_"), "Not a event log file!")
    val seq = eventLogFileName.stripPrefix("events_").split("\\.")(0)
    seq.toLong
  }
}
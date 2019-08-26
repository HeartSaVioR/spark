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

import java.io.File
import java.net.URI

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils


abstract class EventLogWritersSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfter
  with Logging {

  protected val fileSystem = Utils.getHadoopFileSystem("/",
    SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  protected var testDir: File = _
  protected var testDirPath: Path = _

  before {
    testDir = Utils.createTempDir(namePrefix = s"event log")
    testDir.deleteOnExit()
    testDirPath = new Path(testDir.getAbsolutePath())
  }

  after {
    Utils.deleteRecursively(testDir)
  }

  def getUniqueApplicationId: String = "test-" + System.currentTimeMillis

  test("create EventLogFileWriter with enable/disable rolling") {
    def buildWriterAndVerify(conf: SparkConf, expectedClazz: Class[_]): Unit = {
      val writer = EventLogFileWriter.createEventLogFileWriter(
        getUniqueApplicationId, None, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))
      val writerClazz = writer.getClass
      assert(expectedClazz === writerClazz,
        s"default file writer should be $expectedClazz, but $writerClazz")
    }

    val conf = new SparkConf
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_DIR, testDir.toString)

    // default config
    buildWriterAndVerify(conf, classOf[SingleEventLogFileWriter])

    conf.set(EVENT_LOG_ENABLE_ROLLING, true)
    buildWriterAndVerify(conf, classOf[RollingEventLogFilesWriter])

    conf.set(EVENT_LOG_ENABLE_ROLLING, false)
    buildWriterAndVerify(conf, classOf[SingleEventLogFileWriter])
  }

  test("initialize, write, stop - with all codecs") {
    val compressionCodecs = Seq(None) ++
      CompressionCodec.ALL_COMPRESSION_CODECS.map(c => Some(CompressionCodec.getShortName(c)))

    compressionCodecs.foreach { codecShortName =>
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = new SparkConf
      conf.set(EVENT_LOG_ENABLED, true)
      conf.set(EVENT_LOG_DIR, testDir.toString)
      conf.set(EVENT_LOG_COMPRESS, false)
      codecShortName.foreach { c =>
        conf.set(EVENT_LOG_COMPRESS, true)
        conf.set(EVENT_LOG_COMPRESSION_CODEC, c)
      }

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri, codecShortName,
        isCompleted = false)

      val dummyData = Seq("dummy1", "dummy2", "dummy3")

      dummyData.foreach(writer.writeEvent(_, flushLogger = true))

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri, codecShortName,
        isCompleted = false, dummyData)

      writer.stop()

      verifyWriteEventLogFile(appId, attemptId, testDirPath.toUri, codecShortName,
        isCompleted = true, dummyData)
    }
  }

  protected def createWriter(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogWriter

  protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean,
      expectedLines: Seq[String] = Seq.empty): Unit
}

class SingleEventLogFileWriterSuite extends EventLogWritersSuite {
  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogWriter = {
    new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean,
      expectedLines: Seq[String]): Unit = {
    // read single event log file
    val logPath = SingleEventLogFileWriter.getLogPath(logBaseDir, appId, appAttemptId,
      compressionCodecShortName)

    val finalLogPath = if (!isCompleted) {
      new Path(logPath + EventLogFileWriter.IN_PROGRESS)
    } else {
      new Path(logPath)
    }

    assert(fileSystem.exists(finalLogPath) && fileSystem.isFile(finalLogPath))

    val logDataStream = EventLogFileWriter.openEventLog(finalLogPath, fileSystem)
    try {
      val lines = Source.fromInputStream(logDataStream).getLines()
      assert(expectedLines === lines)
    } finally {
      logDataStream.close()
    }
  }
}

class RollingEventLogFilesWriterSuite extends EventLogWritersSuite {
  import RollingEventLogFilesWriter._

  // FIXME: add test for rolling event log files

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogWriter = {
    new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def verifyWriteEventLogFile(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean,
      expectedLines: Seq[String]): Unit = {
    val logDirPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

    assert(fileSystem.exists(logDirPath) && fileSystem.isDirectory(logDirPath))

    val appStatusFile = getAppStatusFilePath(logDirPath, !isCompleted)
    assert(fileSystem.exists(appStatusFile) && fileSystem.isFile(appStatusFile))

    val files = fileSystem.listFiles(logDirPath, false)
    val eventLogFiles = mutable.ArrayBuffer[FileStatus]()
    while (files.hasNext) {
      val status = files.next()
      if (isEventLogFile(status)) {
        eventLogFiles.append(status)
      }
    }

    val sortedFiles = eventLogFiles.sortBy(fs => getSequence(fs.getPath.getName))
    val allLines = mutable.ArrayBuffer[String]()
    sortedFiles.foreach { file =>
      val logDataStream = EventLogFileWriter.openEventLog(file.getPath, fileSystem)
      try {
        val lines = Source.fromInputStream(logDataStream).getLines()
        allLines.appendAll(lines)
      } finally {
        logDataStream.close()
      }
    }

    assert(expectedLines === allLines)
  }
}
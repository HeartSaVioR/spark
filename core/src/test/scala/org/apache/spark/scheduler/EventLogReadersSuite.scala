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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.zip.{ZipInputStream, ZipOutputStream}

import scala.collection.mutable

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler.EventLogTestHelper._
import org.apache.spark.util.Utils

abstract class EventLogReadersSuite extends SparkFunSuite with LocalSparkContext
  with BeforeAndAfter with Logging {

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

  test("Retrieve EventLogReader correctly") {
    def assertInstanceOfEventLogReader(
        expectedClazz: Option[Class[_ <: EventLogReader]],
        actual: Option[EventLogReader]): Unit = {
      if (expectedClazz.isEmpty) {
        assert(actual.isEmpty, s"Expected no EventLogReader instance but was " +
          s"${actual.map(_.getClass).getOrElse("<None>")}")
      } else {
        assert(actual.isDefined, s"Expected an EventLogReader instance but was empty")
        assert(expectedClazz.get.isAssignableFrom(actual.get.getClass),
          s"Expected ${expectedClazz.get} but was ${actual.get.getClass}")
      }
    }

    def testForPathWithoutSeq(
        path: Path,
        isFile: Boolean,
        expectedClazz: Option[Class[_ <: EventLogReader]]): Unit = {
      if (isFile) {
        Utils.tryWithResource(fileSystem.create(path)) { is =>
          is.writeInt(10)
        }
      } else {
        fileSystem.mkdirs(path)
      }

      val reader = EventLogReaders.getEventLogReader(fileSystem, path)
      assertInstanceOfEventLogReader(expectedClazz, reader)
      val reader2 = EventLogReaders.getEventLogReader(fileSystem,
        fileSystem.getFileStatus(path))
      assertInstanceOfEventLogReader(expectedClazz, reader)
    }

    // path with no last sequence - single event log
    val reader1 = EventLogReaders.getEventLogReader(fileSystem, new Path(testDirPath, "aaa"), None)
    assertInstanceOfEventLogReader(Some(classOf[SingleFileEventLogReader]), Some(reader1))

    // path with last sequence - rolling event log
    val reader2 = EventLogReaders.getEventLogReader(fileSystem,
      new Path(testDirPath, "eventlog_v2_aaa"), Some(3))
    assertInstanceOfEventLogReader(Some(classOf[RollingEventLogFilesReader]), Some(reader2))

    // path - file (both path and FileStatus)
    val eventLogFile = new Path(testDirPath, "bbb")
    testForPathWithoutSeq(eventLogFile, isFile = true, Some(classOf[SingleFileEventLogReader]))

    // path - file starting with "."
    val invalidEventLogFile = new Path(testDirPath, ".bbb")
    testForPathWithoutSeq(invalidEventLogFile, isFile = true, None)

    // path - directory with "eventlog_v2_" prefix
    val eventLogDir = new Path(testDirPath, "eventlog_v2_ccc")
    testForPathWithoutSeq(eventLogDir, isFile = false, Some(classOf[RollingEventLogFilesReader]))

    // path - directory with no "eventlog_v2_" prefix
    val invalidEventLogDir = new Path(testDirPath, "ccc")
    testForPathWithoutSeq(invalidEventLogDir, isFile = false, None)
  }

  val allCodecs = Seq(None) ++
    CompressionCodec.ALL_COMPRESSION_CODECS.map(c => Some(CompressionCodec.getShortName(c)))

  allCodecs.foreach { codecShortName =>
    test(s"get information, list event log files, zip log files - with code $codecShortName") {
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf, hadoopConf)
      writer.start()

      // The test for writing events into EventLogFileWriter is covered to its own test suite.
      val dummyData = Seq("dummy1", "dummy2", "dummy3")
      dummyData.foreach(writer.writeEvent(_, flushLogger = true))

      val logPathIncompleted = getCurrentLogPath(writer.logPath, isCompleted = false)
      val readerOpt = EventLogReaders.getEventLogReader(fileSystem, new Path(logPathIncompleted))
      assertAppropriateReader(readerOpt)
      val reader = readerOpt.get

      verifyReader(reader, new Path(logPathIncompleted), codecShortName, isCompleted = false)

      writer.stop()

      val logPathCompleted = getCurrentLogPath(writer.logPath, isCompleted = true)
      val readerOpt2 = EventLogReaders.getEventLogReader(fileSystem, new Path(logPathCompleted))
      assertAppropriateReader(readerOpt2)
      val reader2 = readerOpt2.get

      verifyReader(reader2, new Path(logPathCompleted), codecShortName, isCompleted = true)
    }
  }

  protected def createWriter(
      appId: String,
      appAttemptId : Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter

  protected def getCurrentLogPath(logPath: String, isCompleted: Boolean): String

  protected def assertAppropriateReader(actualReader: Option[EventLogReader]): Unit

  protected def verifyReader(
      reader: EventLogReader,
      logPath: Path,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Unit
}

class SingleFileEventLogReaderSuite extends EventLogReadersSuite {
  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def assertAppropriateReader(actualReader: Option[EventLogReader]): Unit = {
    assert(actualReader.isDefined, s"Expected an EventLogReader instance but was empty")
    assert(actualReader.get.isInstanceOf[SingleFileEventLogReader],
      s"Expected SingleFileEventLogReader but was ${actualReader.get.getClass}")
  }

  override protected def getCurrentLogPath(logPath: String, isCompleted: Boolean): String = {
    if (!isCompleted) logPath + EventLogFileWriter.IN_PROGRESS else logPath
  }

  override protected def verifyReader(
      reader: EventLogReader,
      logPath: Path,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Unit = {
    val stats = fileSystem.getFileStatus(logPath)

    assert(stats.isFile)
    assert(reader.rootPath === logPath)
    assert(reader.lastSequence.isEmpty)
    assert(reader.fileSizeForLastSequence === stats.getLen)
    assert(reader.completed === isCompleted)
    assert(reader.modificationTime === stats.getModificationTime)
    assert(reader.listEventLogFiles.length === 1)
    assert(reader.listEventLogFiles.map(_.getPath.toUri.getPath) ===
      Seq(logPath.toUri.getPath))
    assert(reader.compression === compressionCodecShortName)
    assert(reader.allSize === stats.getLen)

    val underlyingStream = new ByteArrayOutputStream()
    Utils.tryWithResource(new ZipOutputStream(underlyingStream)) { os =>
      reader.zipEventLogFiles(os)
    }

    Utils.tryWithResource(new ZipInputStream(
      new ByteArrayInputStream(underlyingStream.toByteArray))) { is =>

      var entry = is.getNextEntry
      assert(entry != null)
      val actual = new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8)
      val expected = Files.toString(new File(logPath.toString), StandardCharsets.UTF_8)
      assert(actual === expected)
      assert(is.getNextEntry === null)
    }
  }
}

class RollingEventLogFilesReaderSuite extends EventLogReadersSuite {
  allCodecs.foreach { codecShortName =>
    test(s"rolling event log files - codec $codecShortName") {
      val appId = getUniqueApplicationId
      val attemptId = None

      val conf = getLoggingConf(testDirPath, codecShortName)
      conf.set(EVENT_LOG_ENABLE_ROLLING, true)
      conf.set(EVENT_LOG_ROLLED_EVENT_LOG_MAX_FILE_SIZE.key, "1k")

      val writer = createWriter(appId, attemptId, testDirPath.toUri, conf,
        SparkHadoopUtil.get.newConfiguration(conf))

      writer.start()

      val dummyString = "dummy"
      val dummyStringBytesLen = dummyString.getBytes(StandardCharsets.UTF_8).length
      val expectedLines = mutable.ArrayBuffer[String]()

      // write log more than 2k (intended to roll over to 3 files)
      val repeatCount = Math.floor((1024 * 2) / dummyStringBytesLen).toInt
      (0 until repeatCount).foreach { _ =>
        expectedLines.append(dummyString)
        writer.writeEvent(dummyString, flushLogger = true)
      }

      val logPathIncompleted = getCurrentLogPath(writer.logPath, isCompleted = false)
      val readerOpt = EventLogReaders.getEventLogReader(fileSystem, new Path(logPathIncompleted))
      verifyReader(readerOpt.get, new Path(logPathIncompleted), codecShortName, isCompleted = false)
      assert(readerOpt.get.listEventLogFiles.length === 3)

      writer.stop()

      val logPathCompleted = getCurrentLogPath(writer.logPath, isCompleted = true)
      val readerOpt2 = EventLogReaders.getEventLogReader(fileSystem, new Path(logPathCompleted))
      verifyReader(readerOpt2.get, new Path(logPathCompleted), codecShortName, isCompleted = true)
      assert(readerOpt.get.listEventLogFiles.length === 3)
    }
  }

  override protected def createWriter(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
  }

  override protected def assertAppropriateReader(actualReader: Option[EventLogReader]): Unit = {
    assert(actualReader.isDefined, s"Expected an EventLogReader instance but was empty")
    assert(actualReader.get.isInstanceOf[RollingEventLogFilesReader],
      s"Expected RollingEventLogFilesReader but was ${actualReader.get.getClass}")
  }

  override protected def getCurrentLogPath(logPath: String, isCompleted: Boolean): String = logPath

  override protected def verifyReader(
      reader: EventLogReader,
      logPath: Path,
      compressionCodecShortName: Option[String],
      isCompleted: Boolean): Unit = {
    import RollingEventLogFilesWriter._

    val stats = fileSystem.getFileStatus(logPath)
    assert(stats.isDirectory)

    val statsInDir = fileSystem.listStatus(logPath)
    val eventFiles = statsInDir.filter(isEventLogFile).sortBy(s => getSequence(s.getPath.getName))
    assert(eventFiles.nonEmpty)
    val lastEventFile = eventFiles.last
    val allLen = eventFiles.map(_.getLen).sum

    assert(reader.rootPath === logPath)
    assert(reader.lastSequence === Some(getSequence(lastEventFile.getPath.getName)))
    assert(reader.fileSizeForLastSequence === lastEventFile.getLen)
    assert(reader.completed === isCompleted)
    assert(reader.modificationTime === lastEventFile.getModificationTime)
    assert(reader.listEventLogFiles.length === eventFiles.length)
    assert(reader.listEventLogFiles.map(_.getPath) === eventFiles.map(_.getPath))
    assert(reader.compression === compressionCodecShortName)
    assert(reader.allSize === allLen)

    val underlyingStream = new ByteArrayOutputStream()
    Utils.tryWithResource(new ZipOutputStream(underlyingStream)) { os =>
      reader.zipEventLogFiles(os)
    }

    Utils.tryWithResource(new ZipInputStream(
      new ByteArrayInputStream(underlyingStream.toByteArray))) { is =>

      var entry = is.getNextEntry
      assert(entry != null)

      // directory
      assert(entry.getName === logPath.getName + "/")

      fileSystem.listStatus(logPath).foreach { file =>
        entry = is.getNextEntry
        assert(entry != null)

        assert(entry.getName === logPath.getName + "/" + file.getPath.getName)

        val actual = new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8)
        val expected = Files.toString(new File(logPath.toString, file.getPath.getName),
          StandardCharsets.UTF_8)
        assert(actual === expected)
      }

      assert(is.getNextEntry === null)
    }
  }
}
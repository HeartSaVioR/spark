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

import java.util.zip.{ZipEntry, ZipOutputStream}

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DFSInputStream

import org.apache.spark.util.Utils

// FIXME: javadoc!!
sealed trait EventLogReader {
  def rootPath: Path

  def lastSequence: Option[Long]

  def fileSizeForLastSequence: Long

  def completed: Boolean

  def fileSizeForLastSequenceForDFS: Option[Long]

  def modificationTime: Long

  /**
   * This method compresses the files passed in, and writes the compressed data out into the
   * [[OutputStream]] passed in. Each file is written as a new [[ZipEntry]] with its name being
   * the name of the file being compressed.
   */
  def zipEventLogFiles(zipStream: ZipOutputStream): Unit

  def listEventLogFiles: Seq[FileStatus]

  def compression: Option[String]

  def allSize: Long
}

object EventLogReaders {
  def getEventLogReader(fs: FileSystem, path: Path, lastSequence: Option[Long]): EventLogReader = {
    lastSequence match {
      case Some(_) => new RollingEventLogFilesReader(fs, path)
      case None => new SingleFileEventLogReader(fs, path)
    }
  }

  def getEventLogReader(fs: FileSystem, path: Path): Option[EventLogReader] = {
    val status = fs.getFileStatus(path)
    if (isSingleEventLog(status)) {
      Some(new SingleFileEventLogReader(fs, path))
    } else if (isRollingEventLogs(status)) {
      Some(new RollingEventLogFilesReader(fs, path))
    } else {
      None
    }
  }

  def getEventLogReader(fs: FileSystem, status: FileStatus): Option[EventLogReader] = {
    if (isSingleEventLog(status)) {
      Some(new SingleFileEventLogReader(fs, status.getPath))
    } else if (isRollingEventLogs(status)) {
      Some(new RollingEventLogFilesReader(fs, status.getPath))
    } else {
      None
    }
  }

  private def isSingleEventLog(status: FileStatus): Boolean = {
    !status.isDirectory &&
      // FsHistoryProvider used to generate a hidden file which can't be read.  Accidentally
      // reading a garbage file is safe, but we would log an error which can be scary to
      // the end-user.
      !status.getPath.getName.startsWith(".")
  }

  private def isRollingEventLogs(status: FileStatus): Boolean = {
    status.isDirectory && RollingEventLogFilesWriter.isEventLogDir(status)
  }
}

class SingleFileEventLogReader(fs: FileSystem, override val rootPath: Path) extends EventLogReader {
  // FIXME: get stats with constructor and only call if it's needed?
  private lazy val stats = fs.getFileStatus(rootPath)

  override def lastSequence: Option[Long] = None

  override def fileSizeForLastSequence: Long = stats.getLen

  override def completed: Boolean = {
    !rootPath.getName.endsWith(EventLogFileWriter.IN_PROGRESS)
  }

  override def fileSizeForLastSequenceForDFS: Option[Long] = {
    Utils.tryWithResource(fs.open(rootPath)) { in =>
      in.getWrappedStream match {
        case dfsIn: DFSInputStream => Some(dfsIn.getFileLength)
        case _ => None
      }
    }
  }

  override def modificationTime: Long = stats.getModificationTime

  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {
    Utils.tryWithResource(fs.open(rootPath, 1 * 1024 * 1024)) { inputStream =>
      zipStream.putNextEntry(new ZipEntry(rootPath.getName))
      ByteStreams.copy(inputStream, zipStream)
      zipStream.closeEntry()
    }
  }

  override def listEventLogFiles: Seq[FileStatus] = Seq(stats)

  override def compression: Option[String] = EventLogFileWriter.codecName(rootPath)

  override def allSize: Long = fileSizeForLastSequence
}

class RollingEventLogFilesReader(
    fs: FileSystem,
    override val rootPath: Path) extends EventLogReader {
  import RollingEventLogFilesWriter._

  private lazy val files: Seq[FileStatus] = {
    val ret = fs.listStatus(rootPath).asInstanceOf[Seq[FileStatus]]
    require(ret.exists(isEventLogFile), "Log directory must contain at least one event log file!")
    require(ret.exists(isAppStatusFile), "Log directory must contain an appstatus file!")
    ret
  }

  override def lastSequence: Option[Long] = {
    val maxSeq = files.filter(isEventLogFile)
      .map(stats => getSequence(stats.getPath.getName))
      .max
    Some(maxSeq)
  }

  override def fileSizeForLastSequence: Long = lastEventLogFile.getLen

  override def completed: Boolean = {
    files.find(isAppStatusFile).exists(_.getPath.getName.endsWith(EventLogFileWriter.IN_PROGRESS))
  }

  override def fileSizeForLastSequenceForDFS: Option[Long] = {
    Utils.tryWithResource(fs.open(lastEventLogFile.getPath)) { in =>
      in.getWrappedStream match {
        case dfsIn: DFSInputStream => Some(dfsIn.getFileLength)
        case _ => None
      }
    }
  }

  override def modificationTime: Long = lastEventLogFile.getModificationTime

  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {
    val dirEntryName = rootPath.getName + "/"
    zipStream.putNextEntry(new ZipEntry(dirEntryName))
    files.foreach { file =>
      Utils.tryWithResource(fs.open(file.getPath, 1 * 1024 * 1024)) { inputStream =>
        zipStream.putNextEntry(new ZipEntry(dirEntryName + file.getPath.getName))
        ByteStreams.copy(inputStream, zipStream)
        zipStream.closeEntry()
      }
    }
  }

  override def listEventLogFiles: Seq[FileStatus] = eventLogFiles

  override def compression: Option[String] = {
    EventLogFileWriter.codecName(eventLogFiles.head.getPath)
  }

  override def allSize: Long = eventLogFiles.map(_.getLen).sum

  private def eventLogFiles: Seq[FileStatus] = {
    files.filter(isEventLogFile).sortBy(stats => getSequence(stats.getPath.getName))
  }

  private def lastEventLogFile: FileStatus = eventLogFiles.reverse.head
}

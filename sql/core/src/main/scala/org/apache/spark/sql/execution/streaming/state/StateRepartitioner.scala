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

package org.apache.spark.sql.execution.streaming.state

import java.io.FileNotFoundException
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, OffsetSeqLog, OffsetSeqMetadata}
import org.apache.spark.sql.internal.SQLConf.{SHUFFLE_PARTITIONS, STATE_STORE_PROVIDER_CLASS}

class StateRepartitioner(sparkSession: SparkSession,
                         hadoopConf: Configuration,
                         checkpointLocationStr: String,
                         newShufflePartitions: Int) {
  // FIXME: we may want to just assume this would be based on HDFSBackedStateStoreProvider
  // or at least receiving analyzed plan to find all available operators
  final val FILENAME_OFFSETS: String = "offsets"

  val checkpointLocation = new Path(checkpointLocationStr).toUri.toString

  assertCheckpointOffsetsExist()

  lazy val resolvedCheckpointRoot = {
    val checkpointPath = new Path(checkpointLocation)
    val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }

  lazy val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile(FILENAME_OFFSETS))

  lazy val (latestBatchId, latestOffsetLog) = offsetLog.getLatest() match {
    case Some((bid, olog)) => (bid, olog)
    case None => throw new IllegalStateException("No offset log has been written to checkpoint.")
  }

  val (latestCommittedBatchId, latestCommittedBatchOffsetLog) = if (latestBatchId != 0) {
    val secondLatestBatchOffsetLog = offsetLog.get(latestBatchId - 1).getOrElse {
      throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
    }
    (latestBatchId - 1, secondLatestBatchOffsetLog)
  } else {
    throw new IllegalStateException("Latest batch id is 0, meaning commit didn't happen")
  }

  val (prevShufPartsStr, stateStoreProviderClass) = latestCommittedBatchOffsetLog.metadata
      .getOrElse(new OffsetSeqMetadata()) match {
    case metadata =>
      val shufflePartitions = readConfigFromMetadataOrSystemDefaultValue(
        metadata, SHUFFLE_PARTITIONS)
      val stateStoreProviderClass = readConfigFromMetadataOrSystemDefaultValue(
        metadata, STATE_STORE_PROVIDER_CLASS)
      (shufflePartitions, stateStoreProviderClass)
  }

  val prevShufflePartitions = prevShufPartsStr.toInt

  if (newShufflePartitions == prevShufflePartitions) {
    // FIXME: just return here
    throw new UnsupportedOperationException("FIXME: should return here after cleaning up code")
  }

  private val checkpointRootLocationForState: String = checkpointFile("state")
  // FIXME: read all operator ids from state
  val operatorIds = getAvailableOperatorIds(new Path(checkpointRootLocationForState), hadoopConf)

  val fakeQueryId = UUID.randomUUID()

  operatorIds.foreach(opId => {
    val pathForFirstPartition = new Path(s"$checkpointRootLocationForState/$opId/0")
    val storeName = getStoreName(pathForFirstPartition, hadoopConf)

    (0 until prevShufflePartitions).foreach(partId => {
      val storeId = new StateStoreId(checkpointRootLocationForState, opId, partId, storeName)

      // FIXME: we need to know about key schema as well as value schema...
      val provider = StateStoreProvider.createAndInit(
        storeId,
        keySchema,
        valueSchema,
        indexOrdinal,
        storeConf,
        hadoopConf
      )

      val stateStore = provider.getStore(latestCommittedBatchId)
      stateStore.iterator().foreach(rowPair => {
        // FIXME: apply hash function against rowPair.key to get new partition (most important!)
        rowPair.key

        // FIXME: store key and value to temporary file marked as operator id & new partition id
        rowPair.value
      })
    })

    (0 until newShufflePartitions).foreach(partId => {
      // FIXME: loads temporary file and put to new StateStoreProvider,
      // having different checkpointRootLocation to swap after done

    })
  })

  private def assertCheckpointOffsetsExist(): Unit = {
    val checkpointPath = new Path(checkpointLocation, FILENAME_OFFSETS)
    val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    if (!fs.exists(checkpointPath)) {
      throw new FileNotFoundException(s"Checkpoint location doesn't exist: $checkpointPath")
    }
  }

  private def checkpointFileAsPath(name: String): Path =
    new Path(new Path(resolvedCheckpointRoot), name)

  private def checkpointFile(name: String): String =
    checkpointFileAsPath(name).toUri.toString

  private def readConfigFromMetadataOrSystemDefaultValue(metadata: OffsetSeqMetadata,
                                                         confKey: ConfigEntry[_]): String = {
    metadata.conf.get(confKey.key) match {
      case Some(valueInMetadata) => valueInMetadata
      case None => confKey.defaultValueString
    }
  }

  private def getAvailableOperatorIds(checkpointRootLocation: Path,
                              hadoopConf: Configuration): Seq[Long] = {
    getAvailableNumberSubdirectories(checkpointRootLocation, hadoopConf)
      .map(fs => fs.getPath.getName.toLong)
  }

  private def getAvailablePartitionIds(checkpointRootLocation: Path,
                               hadoopConf: Configuration,
                               operatorId: Long): Seq[Long] = {
    val parentLocation = new Path(checkpointRootLocation, operatorId.toString)
    getAvailableNumberSubdirectories(parentLocation, hadoopConf)
      .map(fs => fs.getPath.getName.toLong)
  }

  private def getAvailableNumberSubdirectories(parentLocation: Path,
                                               hadoopConf: Configuration): Seq[FileStatus] = {
    val fm = CheckpointFileManager.create(parentLocation, hadoopConf)
    fm.list(parentLocation, (path: Path) => {
      try {
        path.getName.toLong
        true
      } catch {
        case _: NumberFormatException => false
      }
    }).filter(fs => fs.isDirectory)
  }

  private def getStoreName(parentLocation: Path, hadoopConf: Configuration): String = {
    val fm = CheckpointFileManager.create(parentLocation, hadoopConf)
    val files = fm.list(parentLocation)
    val stateFileCount = files.count(fs => fs.isFile &&
      (fs.getPath.getName.endsWith(".snapshot") || fs.getPath.getName.endsWith(".delta")))
    if (stateFileCount > 0) {
      StateStoreId.DEFAULT_STORE_NAME
    } else {
      // we assume there's only one directory for store name
      val directories = files.filter(_.isDirectory)
      if (directories.length != 1) {
        throw new IllegalStateException("There should be one directory representing store name")
      }

      directories(0).getPath.getName
    }
  }
}

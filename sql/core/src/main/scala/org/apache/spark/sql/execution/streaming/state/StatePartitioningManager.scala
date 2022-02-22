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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state.PartitioningHelper.{PartitioningReader, PartitioningWriter}
import org.apache.spark.sql.types.{DataType, StructType}

class StatePartitioningManager(
    checkpointLocation: String,
    operatorId: Long,
    hadoopConf: Configuration) extends Logging {
  import StatePartitioningManager._

  private val storeCpLocation: Path = {
    val storeId = StateStoreId(checkpointLocation, operatorId,
      PARTITION_ID_FOR_PARTITIONING_METADATA)
    storeId.storeCheckpointLocation()
  }

  private val fm = CheckpointFileManager.create(storeCpLocation, hadoopConf)
  private val partitionFileLocation = partitionFile(storeCpLocation)
  private val partitionWriter = PartitioningWriter.createPartitioningWriter(VERSION)

  fm.mkdirs(partitionFileLocation.getParent)

  def findPartitioningAttributes(
      candidateAttrs: Seq[Seq[Attribute]],
      childrenOutputPartitioning: Seq[Partitioning]): (Boolean, Seq[Seq[Attribute]]) = {
    // FIXME: better error message?
    require(candidateAttrs.length == childrenOutputPartitioning.length)

    if (fm.exists(partitionFileLocation)) {
      val prevPartitioning: Seq[Seq[(String, DataType)]] = readPartitioningFile()
      // FIXME: we have to try to find the attribute from candidateAttrs...
      //  previous partitioning has to be subset or exact of candidate...
      //  if candidate is a subset of previous partitioning, it is already broken...
      //  it is unfortunately our best trial to match up the attribute via name and data type


    } else {
      logDebug("Partitioning file doesn't exist. Picking up current output partitioning.")
      childrenOutputPartitioning
      childrenOutputPartitioning match {
        case p: HashPartitioning =>

          p.expressions.map {
            case ne: NamedExpression => ne.toAttribute
            case _ =>
          }

          candidateAttrs.find()

        case _ =>
          // FIXME: better error message or throw exception with information to
          //  include more context
          logWarning("The output partitioning from child is not HashPartitioning!")
          (false, candidateAttrs)
      }

      // FIXME: better log message

      createPartitioningFile(candidateAttrs)
      candidateAttrs
    }
  }

  // Visible for testing
  private[sql] def readPartitioningFile(): Seq[Seq[(String, DataType)]] = {
    val inStream = fm.open(partitionFileLocation)
    try {
      val versionStr = inStream.readUTF()
      val schemaReader = PartitioningReader.createPartitioningReader(versionStr)
      schemaReader.read(inStream)
    } catch {
      case e: Throwable =>
        logError(s"Fail to read partitioning file from $partitionFileLocation", e)
        throw e
    } finally {
      inStream.close()
    }
  }

  private def createPartitioningFile(partitioning: Seq[Seq[Attribute]]): Unit = {
    createPartitioningFile(partitioning, partitionWriter)
  }

  // Visible for testing
  private[sql] def createPartitioningFile(
      partitioning: Seq[Seq[Attribute]],
      partitionWriter: PartitioningWriter): Unit = {
    val outStream = fm.createAtomic(partitionFileLocation, overwriteIfPossible = false)
    try {
      partitionWriter.write(partitioning, outStream)
      outStream.close()
    } catch {
      case e: Throwable =>
        logError(s"Fail to write partitioning file to $partitionFileLocation", e)
        outStream.cancel()
        throw e
    }
  }

  private def partitionFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "partitioning")
}

object StatePartitioningManager {
  val PARTITION_ID_FOR_PARTITIONING_METADATA = 0
  val VERSION = 1
}
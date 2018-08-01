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

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, Literal, Murmur3Hash, Murmur3HashFunction, Pmod, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.GroupedIterator
import org.apache.spark.sql.execution.streaming.continuous.EpochTracker
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration, ThreadUtils}

/**
 * An RDD that allows computations to be executed against [[StateStore]]s. It
 * uses the [[StateStoreCoordinator]] to get the locations of loaded state stores
 * and use that as the preferred locations.
 */
class StateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U],
    storeCompletionFunction: StateStore => Any,
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long,
    numStateKeyGroups: Int,
    stateKeyGroupExpression: Option[(Seq[Attribute], Expression)],
    keySchema: StructType,
    valueSchema: StructType,
    indexOrdinal: Option[Int],
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef])
  extends RDD[U](dataRDD) {

  private val storeConf = new StateStoreConf(sessionState.conf)

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcast = dataRDD.context.broadcast(
    new SerializableConfiguration(sessionState.newHadoopConf()))

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  /**
   * Set the preferred location of each partition using the executor that has the related
   * [[StateStoreProvider]] already loaded.
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val stateStoreProviderId = StateStoreProviderId(
      StateStoreId(checkpointLocation, operatorId, partition.index),
      queryRunId)
    storeCoordinator.flatMap(_.getLocation(stateStoreProviderId)).toSeq
  }

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    // If we're in continuous processing mode, we should get the store version for the current
    // epoch rather than the one at planning time.
    val currentVersion = EpochTracker.getCurrentEpoch match {
      case None => storeVersion
      case Some(value) => value
    }

    val inputIter = dataRDD.iterator(partition, ctxt)

    stateKeyGroupExpression match {
      case Some((keyAttrs, expr)) =>
        require(inputIter.isInstanceOf[Iterator[InternalRow]], "state key group is only " +
          "supported with data type InternalRow")

        // FIXME: isn't it dirty enough?
        val schema = new StructType().add("value", "int")
        val attribute = schema.toAttributes
        val expr = Pmod(new Murmur3Hash(attribute), Literal(getPartitions.length))
        val proj = UnsafeProjection.create(expr :: Nil, attribute)

        val stateKeyGroupsToCover = (0 until numStateKeyGroups).filter { keyGroupIndex =>
          val expectedPartitionIdRow = proj.apply(new GenericInternalRow(Array[Any](keyGroupIndex)))
          val expectedPartitionId = expectedPartitionIdRow.getInt(0)
          expectedPartitionId == partition.index
        }.toSet

        // FIXME: debug
        logWarning(s"DEBUG: numStateKeyGroups: $numStateKeyGroups, partition length: " +
          s"${getPartitions.length}, partition id: ${partition.index}, " +
          s"keyGroupIdsToCover: $stateKeyGroupsToCover")

        val stateStoreGroup = stateKeyGroupsToCover.map { stateKeyGroupIdx =>
          val storeProviderId = StateStoreProviderId(
            StateStoreId(checkpointLocation, operatorId, stateKeyGroupIdx),
            queryRunId)
          val store = StateStore.get(
            storeProviderId, keySchema, valueSchema, indexOrdinal, currentVersion,
            storeConf, hadoopConfBroadcast.value.value)

          stateKeyGroupIdx -> store
        }.toMap

        val usedKeyGroups = new mutable.HashSet[Int]()

        // this should be reused to represent they're referencing same
        val keyAttributes = keySchema.toAttributes
        val groupedIterator = GroupedIterator(inputIter.asInstanceOf[Iterator[InternalRow]],
          keyAttributes, keyAttributes ++ valueSchema.toAttributes)

        val retIter = groupedIterator.map { case (keyRow, dataIter) =>
          val getBucketId = UnsafeProjection.create(Seq(expr), keyAttrs)
          val stateKeyGroupIdRow = getBucketId(keyRow).asInstanceOf[InternalRow]
          val stateKeyGroupId = stateKeyGroupIdRow.getInt(0)

          val stateStore = stateStoreGroup
            .get(stateKeyGroupId)
            .orElse {
              throw new IllegalStateException(s"Unexpected state key group id " +
                s"$stateKeyGroupId in partition ${partition.index}, and partition count " +
                s"${getPartitions.length} " +
                s"Expected state key group ids are $stateKeyGroupsToCover")
            }
            .get

          usedKeyGroups.add(stateKeyGroupId)

          storeUpdateFunction(stateStore, dataIter.asInstanceOf[Iterator[T]])
        }.foldLeft(Iterator[U]())(_ ++ _)

        val retIter2 = stateKeyGroupsToCover.diff(usedKeyGroups).map { unusedKeyGroupId =>
          val stateStore = stateStoreGroup
            .get(unusedKeyGroupId)
            .orElse(throw new IllegalStateException(s"Unexpected state key group id " +
              s"$unusedKeyGroupId in partition ${partition.index}... " +
              s"Expected state key group ids are $stateKeyGroupsToCover"))
            .get

          usedKeyGroups.add(unusedKeyGroupId)

          storeUpdateFunction(stateStore, Iterator.empty)
        }.foldLeft(retIter)(_ ++ _)

        CompletionIterator[U, Iterator[U]](retIter2, {
          implicit val globalExecutionContext = ExecutionContext.global
          val futures = stateStoreGroup.values.map { store =>
            Future { storeCompletionFunction(store) }
          }
          val f = Future.sequence(futures)
          ThreadUtils.awaitResult(f, atMost = Duration.Inf)
        })

      case None =>
        val storeProviderId = StateStoreProviderId(
          StateStoreId(checkpointLocation, operatorId, partition.index),
          queryRunId)
        val store = StateStore.get(
          storeProviderId, keySchema, valueSchema, indexOrdinal, currentVersion,
          storeConf, hadoopConfBroadcast.value.value)
        val retIter = storeUpdateFunction(store, inputIter)

        CompletionIterator[U, Iterator[U]](retIter, storeCompletionFunction(store))
    }
  }
}

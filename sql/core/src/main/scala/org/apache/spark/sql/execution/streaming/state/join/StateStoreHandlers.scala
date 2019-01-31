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

package org.apache.spark.sql.execution.streaming.state.join

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinSide
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.join.StreamingJoinStateManager._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.NextIterator

/** Helper trait for invoking common functionalities of a state store. */
private[sql] abstract class StateStoreHandler(
    stateStoreType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) extends Logging {
  /** StateStore that the subclasses of this class is going to operate on */
  protected def stateStore: StateStore

  def commit(): Unit = {
    stateStore.commit()
    logDebug("Committed, metrics = " + stateStore.metrics)
  }

  def abortIfNeeded(): Unit = {
    if (!stateStore.hasCommitted) {
      logInfo(s"Aborted store ${stateStore.id}")
      stateStore.abort()
    }
  }

  def metrics: StateStoreMetrics = stateStore.metrics

  /** Get the StateStore with the given schema */
  protected def getStateStore(keySchema: StructType, valueSchema: StructType): StateStore = {
    val storeProviderId = StateStoreProviderId(
      stateInfo.get, TaskContext.getPartitionId(), getStateStoreName(joinSide, stateStoreType))
    val store = StateStore.get(
      storeProviderId, keySchema, valueSchema, None,
      stateInfo.get.storeVersion, storeConf, hadoopConf)
    logInfo(s"Loaded store ${store.id}")
    store
  }
}

/**
 * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
 * Designed for object reuse.
 */
private[sql] case class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
  def withNew(newKey: UnsafeRow, newNumValues: Long): this.type = {
    this.key = newKey
    this.numValue = newNumValues
    this
  }
}

/** A wrapper around a [[StateStore]] that stores [key -> number of values]. */
private[sql] class KeyToNumValuesStore(
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute])
  extends StateStoreHandler(
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf) {
  private val keySchema = keyAttributes.toStructType
  private val longValueSchema = new StructType().add("value", "long")
  @transient private lazy val longToUnsafeRow = UnsafeProjection.create(longValueSchema)
  private val valueRow = longToUnsafeRow(new SpecificInternalRow(longValueSchema))
  protected val stateStore: StateStore = getStateStore(keySchema, longValueSchema)

  /** Get the number of values the key has */
  def get(key: UnsafeRow): Long = {
    val longValueRow = stateStore.get(key)
    if (longValueRow != null) longValueRow.getLong(0) else 0L
  }

  /** Set the number of values the key has */
  def put(key: UnsafeRow, numValues: Long): Unit = {
    require(numValues > 0)
    valueRow.setLong(0, numValues)
    stateStore.put(key, valueRow)
  }

  def remove(key: UnsafeRow): Unit = {
    stateStore.remove(key)
  }

  def iterator: Iterator[KeyAndNumValues] = {
    val keyAndNumValues = new KeyAndNumValues()
    stateStore.getRange(None, None).map { case pair =>
      keyAndNumValues.withNew(pair.key, pair.value.getLong(0))
    }
  }
}

private[sql] abstract class KeyWithIndexToValueStore[T](
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute],
    valueSchema: StructType)
  extends StateStoreHandler(
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf) {

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  case class KeyWithIndexAndValue(
      var key: UnsafeRow = null,
      var valueIndex: Long = -1,
      var value: T = null.asInstanceOf[T]) {
    def withNew(newKey: UnsafeRow, newIndex: Long, newValue: T): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      this.value = newValue
      this
    }
  }

  private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
  private val keySchema = keyAttributes.toStructType
  private val keyWithIndexSchema = keySchema.add("index", LongType)
  private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

  @transient private lazy val keyWithIndexRowGenerator =
    UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

  // Projection to generate key row from (key + index) row
  @transient private lazy val keyRowGenerator = UnsafeProjection.create(
    keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

  protected val stateStore = getStateStore(keyWithIndexSchema, valueSchema)

  def get(key: UnsafeRow, valueIndex: Long): T = {
    convertValue(stateStore.get(keyWithIndexRow(key, valueIndex)))
  }

  /**
    * Get all values and indices for the provided key.
    * Should not return null.
    */
  def getAll(key: UnsafeRow, numValues: Long): Iterator[KeyWithIndexAndValue] = {
    val keyWithIndexAndValue = new KeyWithIndexAndValue()
    var index = 0
    new NextIterator[KeyWithIndexAndValue] {
      override protected def getNext(): KeyWithIndexAndValue = {
        if (index >= numValues) {
          finished = true
          null
        } else {
          val keyWithIndex = keyWithIndexRow(key, index)
          val value = stateStore.get(keyWithIndex)
          keyWithIndexAndValue.withNew(key, index, convertValue(value))
          index += 1
          keyWithIndexAndValue
        }
      }

      override protected def close(): Unit = {}
    }
  }

  /** Put new value for key at the given index */
  def put(key: UnsafeRow, valueIndex: Long, value: T): Unit = {
    val keyWithIndex = keyWithIndexRow(key, valueIndex)
    val row = convertToValueRow(value)
    if (row != null) {
      stateStore.put(keyWithIndex, row)
    }
  }

  /**
    * Remove key and value at given index. Note that this will create a hole in
    * (key, index) and it is upto the caller to deal with it.
    */
  def remove(key: UnsafeRow, valueIndex: Long): Unit = {
    stateStore.remove(keyWithIndexRow(key, valueIndex))
  }

  /** Remove all values (i.e. all the indices) for the given key. */
  def removeAllValues(key: UnsafeRow, numValues: Long): Unit = {
    var index = 0
    while (index < numValues) {
      stateStore.remove(keyWithIndexRow(key, index))
      index += 1
    }
  }

  def iterator: Iterator[KeyWithIndexAndValue] = {
    val keyWithIndexAndValue = new KeyWithIndexAndValue()
    stateStore.getRange(None, None).map { pair =>
      keyWithIndexAndValue.withNew(keyRowGenerator(pair.key),
        pair.key.getLong(indexOrdinalInKeyWithIndexRow), convertValue(pair.value))
      keyWithIndexAndValue
    }
  }

  /** Generated a row using the key and index */
  protected def keyWithIndexRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
    val row = keyWithIndexRowGenerator(key)
    row.setLong(indexOrdinalInKeyWithIndexRow, valueIndex)
    row
  }

  protected def convertValue(value: UnsafeRow): T
  protected def convertToValueRow(value: T): UnsafeRow
}

/** A wrapper around a [[StateStore]] that stores [(key, index) -> value]. */
private[sql] class KeyWithIndexToRowValueStore(
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute])
  extends KeyWithIndexToValueStore[UnsafeRow](
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf,
    keyAttributes,
    valueAttributes.toStructType) {

  override protected def convertValue(value: UnsafeRow): UnsafeRow = value

  override protected def convertToValueRow(value: UnsafeRow): UnsafeRow = value
}

private[sql] class KeyWithIndexToMatchedStore(
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute])
  extends KeyWithIndexToValueStore[Option[Boolean]](
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf,
    keyAttributes,
    KeyWithIndexToMatchedStore.booleanValueSchema) {

  import KeyWithIndexToMatchedStore._

  @transient private lazy val booleanToUnsafeRow = UnsafeProjection.create(booleanValueSchema)
  private val valueRow = booleanToUnsafeRow(new SpecificInternalRow(booleanValueSchema))

  override protected def convertValue(value: UnsafeRow): Option[Boolean] = {
    if (value != null) Some(value.getBoolean(0)) else None
  }

  override protected def convertToValueRow(value: Option[Boolean]): UnsafeRow = value match {
    case Some(matched) =>
      valueRow.setBoolean(0, matched)
      valueRow

    case None => null
  }
}

private object KeyWithIndexToMatchedStore {
  val booleanValueSchema = new StructType().add("value", "boolean")
}
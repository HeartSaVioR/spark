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

import java.util.Locale

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinSide
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.join.StreamingJoinStateManager.{KeyToValueAndMatched, StateStoreType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.NextIterator

private[sql] abstract class BaseStreamingJoinStateManagerImpl(
    protected val joinSide: JoinSide,
    protected val inputValueAttributes: Seq[Attribute],
    protected val joinKeys: Seq[Expression],
    protected val stateInfo: Option[StatefulOperatorStateInfo],
    protected val storeConf: StateStoreConf,
    protected val hadoopConf: Configuration)
  extends StreamingJoinStateManager {

  protected val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  protected val keyAttributes = keySchema.toAttributes

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }
}

private[sql] class StreamingJoinStateManagerImplV1(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration)
  extends BaseStreamingJoinStateManagerImpl(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) {

  import StreamingJoinStateManagerImplV1._

  private val keyToNumValues = new KeyToNumValuesStore(KeyToNumValuesType, joinSide, stateInfo,
    storeConf, hadoopConf, keyAttributes)
  private val keyWithIndexToValue = new KeyWithIndexToRowValueStore(KeyWithIndexToRowValueType,
    joinSide, stateInfo, storeConf, hadoopConf, keyAttributes, inputValueAttributes)

  override def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.value)
  }

  override def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean): Iterator[JoinedRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map { keyIdxToValue =>
      generateJoinedRow(keyIdxToValue.value)
    }.filter(predicate)
  }

  override def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    // V1 doesn't leverage 'matched' information
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value)
    keyToNumValues.put(key, numExistingValues + 1)
  }

  override def removeByKeyCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    new NextIterator[KeyToValueAndMatched] {

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKeyToNumValue: KeyAndNumValues = null
      private var currentValues: Iterator[keyWithIndexToValue.KeyWithIndexAndValue] = null

      private def currentKey = currentKeyToNumValue.key

      private val reusedTuple = new KeyToValueAndMatched()

      private def getAndRemoveValue(): KeyToValueAndMatched = {
        val keyWithIndexAndValue = currentValues.next()
        keyWithIndexToValue.remove(currentKey, keyWithIndexAndValue.valueIndex)
        reusedTuple.withNew(currentKey, keyWithIndexAndValue.value, None)
      }

      override def getNext(): KeyToValueAndMatched = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null && currentValues.hasNext) {
          return getAndRemoveValue()
        }

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          currentKeyToNumValue = allKeyToNumValues.next()
          if (removalCondition(currentKey)) {
            currentValues = keyWithIndexToValue.getAll(
              currentKey, currentKeyToNumValue.numValue)
            keyToNumValues.remove(currentKey)

            if (currentValues.hasNext) {
              return getAndRemoveValue()
            }
          }
        }

        // We only reach here if there were no satisfying keys left, which means we're done.
        finished = true
        return null
      }

      override def close: Unit = {}
    }
  }

  override def removeByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    new NextIterator[KeyToValueAndMatched] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedTuple = new KeyToValueAndMatched()

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKey: UnsafeRow = null
      private var numValues: Long = 0L
      private var index: Long = 0L
      private var valueRemoved: Boolean = false

      // Push the data for the current key to the numValues store, and reset the tracking variables
      // to their empty state.
      private def updateNumValueForCurrentKey(): Unit = {
        if (valueRemoved) {
          if (numValues >= 1) {
            keyToNumValues.put(currentKey, numValues)
          } else {
            keyToNumValues.remove(currentKey)
          }
        }

        currentKey = null
        numValues = 0
        index = 0
        valueRemoved = false
      }

      // Find the next value satisfying the condition, updating `currentKey` and `numValues` if
      // needed. Returns null when no value can be found.
      private def findNextValueForIndex(): UnsafeRow = {
        // Loop across all values for the current key, and then all other keys, until we find a
        // value satisfying the removal condition.
        def hasMoreValuesForCurrentKey = currentKey != null && index < numValues
        def hasMoreKeys = allKeyToNumValues.hasNext
        while (hasMoreValuesForCurrentKey || hasMoreKeys) {
          if (hasMoreValuesForCurrentKey) {
            // First search the values for the current key.
            val currentValue = keyWithIndexToValue.get(currentKey, index)
            if (removalCondition(currentValue)) {
              return currentValue
            } else {
              index += 1
            }
          } else if (hasMoreKeys) {
            // If we can't find a value for the current key, cleanup and start looking at the next.
            // This will also happen the first time the iterator is called.
            updateNumValueForCurrentKey()

            val currentKeyToNumValue = allKeyToNumValues.next()
            currentKey = currentKeyToNumValue.key
            numValues = currentKeyToNumValue.numValue
          } else {
            // Should be unreachable, but in any case means a value couldn't be found.
            return null
          }
        }

        // We tried and failed to find the next value.
        return null
      }

      override def getNext(): KeyToValueAndMatched = {
        val currentValue = findNextValueForIndex()

        // If there's no value, clean up and finish. There aren't any more available.
        if (currentValue == null) {
          updateNumValueForCurrentKey()
          finished = true
          return null
        }

        // The backing store is arraylike - we as the caller are responsible for filling back in
        // any hole. So we swap the last element into the hole and decrement numValues to shorten.
        // clean
        if (numValues > 1) {
          val valueAtMaxIndex = keyWithIndexToValue.get(currentKey, numValues - 1)
          keyWithIndexToValue.put(currentKey, index, valueAtMaxIndex)
          keyWithIndexToValue.remove(currentKey, numValues - 1)
        } else {
          keyWithIndexToValue.remove(currentKey, 0)
        }
        numValues -= 1
        valueRemoved = true

        return reusedTuple.withNew(currentKey, currentValue, None)
      }

      override def close: Unit = {}
    }
  }

  override def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  override def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  override def metrics: StateStoreMetrics = {
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase(Locale.ROOT)}: $desc"

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics.map {
        case (s @ StateStoreCustomSumMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomSizeMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomTimingMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s, _) =>
          throw new IllegalArgumentException(
            s"Unknown state store custom metric is found at metrics: $s")
      }
    )
  }
}

private[sql] object StreamingJoinStateManagerImplV1 {
  case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  case object KeyWithIndexToRowValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  def allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToRowValueType)
}

private[sql] class StreamingJoinStateManagerImplV2(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration)
  extends BaseStreamingJoinStateManagerImpl(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) {

  import StreamingJoinStateManagerImplV2._

  private val keyToNumValues = new KeyToNumValuesStore(KeyToNumValuesType, joinSide, stateInfo,
    storeConf, hadoopConf, keyAttributes)
  private val keyWithIndexToValue = new KeyWithIndexToRowValueStore(KeyWithIndexToRowValueType,
    joinSide, stateInfo, storeConf, hadoopConf, keyAttributes, inputValueAttributes)
  private val keyWithIndexToMatched = new KeyWithIndexToMatchedStore(KeyWithIndexToRowValueType,
    joinSide, stateInfo, storeConf, hadoopConf, keyAttributes)

  override def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.value)
  }

  override def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean): Iterator[JoinedRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map { keyIdxToValue =>
      val joinedRow = generateJoinedRow(keyIdxToValue.value)
      if (predicate(joinedRow)) {
        keyWithIndexToMatched.put(key, keyIdxToValue.valueIndex, Some(true))
        joinedRow
      } else {
        null
      }
    }.filter(_ != null)
  }

  override def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value)
    keyToNumValues.put(key, numExistingValues + 1)
    keyWithIndexToMatched.put(key, numExistingValues, Some(matched))
  }

  override def removeByKeyCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    new NextIterator[KeyToValueAndMatched] {

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKeyToNumValue: KeyAndNumValues = null
      private var currentValues: Iterator[keyWithIndexToValue.KeyWithIndexAndValue] = null

      private def currentKey = currentKeyToNumValue.key

      private val reusedTuple = new KeyToValueAndMatched()

      private def getAndRemoveValue(): KeyToValueAndMatched = {
        val keyWithIndexAndValue = currentValues.next()
        val matched = keyWithIndexToMatched.get(currentKey, keyWithIndexAndValue.valueIndex)
        keyWithIndexToValue.remove(currentKey, keyWithIndexAndValue.valueIndex)
        keyWithIndexToMatched.remove(currentKey, keyWithIndexAndValue.valueIndex)
        reusedTuple.withNew(currentKey, keyWithIndexAndValue.value, matched)
      }

      override def getNext(): KeyToValueAndMatched = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null && currentValues.hasNext) {
          return getAndRemoveValue()
        }

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          currentKeyToNumValue = allKeyToNumValues.next()
          if (removalCondition(currentKey)) {
            currentValues = keyWithIndexToValue.getAll(
              currentKey, currentKeyToNumValue.numValue)
            keyToNumValues.remove(currentKey)

            if (currentValues.hasNext) {
              return getAndRemoveValue()
            }
          }
        }

        // We only reach here if there were no satisfying keys left, which means we're done.
        finished = true
        return null
      }

      override def close: Unit = {}
    }
  }

  override def removeByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    new NextIterator[KeyToValueAndMatched] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedTuple = new KeyToValueAndMatched()

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKey: UnsafeRow = null
      private var numValues: Long = 0L
      private var index: Long = 0L
      private var valueRemoved: Boolean = false

      // Push the data for the current key to the numValues store, and reset the tracking variables
      // to their empty state.
      private def updateNumValueForCurrentKey(): Unit = {
        if (valueRemoved) {
          if (numValues >= 1) {
            keyToNumValues.put(currentKey, numValues)
          } else {
            keyToNumValues.remove(currentKey)
          }
        }

        currentKey = null
        numValues = 0
        index = 0
        valueRemoved = false
      }

      // Find the next value satisfying the condition, updating `currentKey` and `numValues` if
      // needed. Returns null when no value can be found.
      private def findNextValueForIndex(): (UnsafeRow, Option[Boolean]) = {
        // Loop across all values for the current key, and then all other keys, until we find a
        // value satisfying the removal condition.
        def hasMoreValuesForCurrentKey = currentKey != null && index < numValues
        def hasMoreKeys = allKeyToNumValues.hasNext
        while (hasMoreValuesForCurrentKey || hasMoreKeys) {
          if (hasMoreValuesForCurrentKey) {
            // First search the values for the current key.
            val currentValue = keyWithIndexToValue.get(currentKey, index)
            if (removalCondition(currentValue)) {
              return (currentValue, keyWithIndexToMatched.get(currentKey, index))
            } else {
              index += 1
            }
          } else if (hasMoreKeys) {
            // If we can't find a value for the current key, cleanup and start looking at the next.
            // This will also happen the first time the iterator is called.
            updateNumValueForCurrentKey()

            val currentKeyToNumValue = allKeyToNumValues.next()
            currentKey = currentKeyToNumValue.key
            numValues = currentKeyToNumValue.numValue
          } else {
            // Should be unreachable, but in any case means a value couldn't be found.
            return null
          }
        }

        // We tried and failed to find the next value.
        return null
      }

      override def getNext(): KeyToValueAndMatched = {
        val currentValue = findNextValueForIndex()

        // If there's no value, clean up and finish. There aren't any more available.
        if (currentValue == null) {
          updateNumValueForCurrentKey()
          finished = true
          return null
        }

        // The backing store is arraylike - we as the caller are responsible for filling back in
        // any hole. So we swap the last element into the hole and decrement numValues to shorten.
        // clean
        if (numValues > 1) {
          val valueAtMaxIndex = keyWithIndexToValue.get(currentKey, numValues - 1)
          keyWithIndexToValue.put(currentKey, index, valueAtMaxIndex)
          keyWithIndexToValue.remove(currentKey, numValues - 1)

          val matchedAtMaxIndex = keyWithIndexToMatched.get(currentKey, numValues - 1)
          keyWithIndexToMatched.put(currentKey, index, matchedAtMaxIndex)
          keyWithIndexToMatched.remove(currentKey, numValues - 1)
        } else {
          keyWithIndexToValue.remove(currentKey, 0)
          keyWithIndexToMatched.remove(currentKey, 0)
        }
        numValues -= 1
        valueRemoved = true

        val (value, matched) = currentValue
        return reusedTuple.withNew(currentKey, value, matched)
      }

      override def close: Unit = {}
    }
  }

  override def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
    keyWithIndexToMatched.commit()
  }

  override def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
    keyWithIndexToMatched.abortIfNeeded()
  }

  override def metrics: StateStoreMetrics = {
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    val keyWithIndexToMatchedMetrics = keyWithIndexToMatched.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase(Locale.ROOT)}: $desc"

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes +
        keyWithIndexToMatchedMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics.map {
        case (s @ StateStoreCustomSumMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomSizeMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomTimingMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s, _) =>
          throw new IllegalArgumentException(
            s"Unknown state store custom metric is found at metrics: $s")
      }
    )
  }
}

private[sql] object StreamingJoinStateManagerImplV2 {
  case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  case object KeyWithIndexToRowValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  case object KeyWithIndexToMatchedType extends StateStoreType {
    override def toString(): String = "keyWithIndexToMatched"
  }

  def allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToRowValueType)
}
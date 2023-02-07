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

package org.apache.spark.sql.execution.streaming

import java.{util => jutil}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.WatermarkPropagator.DEFAULT_WATERMARK_MS
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/** Interface for propagating watermark. */
trait WatermarkPropagator {
  /**
   * Request to propagate watermark among operators based on origin watermark value. The result
   * should be input watermark per stateful operator, which Spark will request the value by calling
   * getInputWatermarkXXX with operator ID.
   *
   * It is recommended for implementation to cache the result, as Spark can request the propagation
   * multiple times with the same batch ID and origin watermark value.
   */
  def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit

  /** Provide the calculated input watermark for late events for given stateful operator. */
  def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long

  /** Provide the calculated input watermark for eviction for given stateful operator. */
  def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long

  /**
   * Request to clean up cached result on propagation. Spark will call this method when the given
   * batch ID will be likely to be not re-executed.
   */
  def purge(batchId: Long): Unit
}

/**
 * Do nothing. This is dummy implementation to help creating a dummy IncrementalExecution instance.
 */
class NoOpWatermarkPropagator extends WatermarkPropagator {
  def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {}
  def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long = Long.MinValue
  def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long = Long.MinValue
  def purge(batchId: Long): Unit = {}
}

/**
 * This implementation uses a single global watermark for late events and eviction.
 *
 * This implementation provides the behavior before Structured Streaming supports multiple stateful
 * operators. (prior to SPARK-40925) This is only used for compatibility mode.
 */
class UseSingleWatermarkPropagator extends WatermarkPropagator {
  private val batchIdToWatermark: jutil.TreeMap[Long, Long] = new jutil.TreeMap[Long, Long]()

  private def isInitialized(batchId: Long): Boolean = batchIdToWatermark.containsKey(batchId)

  override def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    if (batchId < 0) {
      // no-op
    } else if (isInitialized(batchId)) {
      val cached = batchIdToWatermark.get(batchId)
      assert(cached == originWatermark,
        s"Watermark has been changed for the same batch ID! Batch ID: $batchId, " +
          s"Value in cache: $cached, value given: $originWatermark")
    } else {
      batchIdToWatermark.put(batchId, originWatermark)
    }
  }

  private def getInputWatermark(batchId: Long, stateOpId: Long): Long = {
    if (batchId < 0) {
      0
    } else {
      assert(isInitialized(batchId), s"Watermark for batch ID $batchId is not yet set!")
      batchIdToWatermark.get(batchId)
    }
  }

  def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId, stateOpId)

  def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId, stateOpId)

  override def purge(batchId: Long): Unit = {
    val keyIter = batchIdToWatermark.keySet().iterator()
    var stopIter = false
    while (keyIter.hasNext && !stopIter) {
      val currKey = keyIter.next()
      if (currKey <= batchId) {
        keyIter.remove()
      } else {
        stopIter = true
      }
    }
  }
}

/**
 * This implementation simulates propagation of watermark among operators.
 *
 * The high-level explanation of propagating watermark is following:
 *
 * The algorithm traverses the physical plan tree via post-order (children first). For each node,
 * below logic is applied:
 *
 * - Input watermark is decided by `min(input watermarks from all children)`.
 *   -- Children providing no input watermark (DEFAULT_WATERMARK_MS) are excluded.
 *   -- If there is no valid input watermark from children, input watermark = DEFAULT_WATERMARK_MS.
 * - Output watermark is decided as following:
 *   -- watermark nodes: origin watermark value
 *      This could be individual origin watermark value, but we decide to retain global watermark
 *      to keep the watermark model be simple.
 *   -- stateless nodes: same as input watermark
 *   -- stateful nodes: the return value of `op.produceWatermark(input watermark)`
 *
 * Once the algorithm traverses the physical plan tree, the association between stateful operator
 * and input watermark will be constructed. Spark will request the input watermark for specific
 * stateful operator, which this implementation will give the value from the association.
 */
class PropagateWatermarkSimulator extends WatermarkPropagator with Logging {
  private val batchIdToWatermark: jutil.TreeMap[Long, Long] = new jutil.TreeMap[Long, Long]()
  private val inputWatermarks: mutable.Map[Long, Map[Long, Long]] =
    mutable.Map[Long, Map[Long, Long]]()

  private def isInitialized(batchId: Long): Boolean = batchIdToWatermark.containsKey(batchId)

  private def getInputWatermarks(
      node: SparkPlan,
      nodeToOutputWatermark: mutable.Map[Int, Long]): Seq[Long] = {
    node.children.map { child =>
      nodeToOutputWatermark.getOrElse(child.id, {
        throw new IllegalStateException(
          s"watermark for the node ${child.id} should be registered")
      })
    }.filter { case curr =>
      // This path is to exclude children from watermark calculation
      // which don't have watermark information
      curr != DEFAULT_WATERMARK_MS
    }
  }

  private def doSimulate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    val statefulOperatorIdToNodeId = mutable.HashMap[Long, Int]()
    val nodeToOutputWatermark = mutable.HashMap[Int, Long]()
    val nextStatefulOperatorToWatermark = mutable.HashMap[Long, Long]()

    // This calculation relies on post-order traversal of the query plan.
    plan.transformUp {
      case node: EventTimeWatermarkExec =>
        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        if (inputWatermarks.nonEmpty) {
          throw new AnalysisException("Redefining watermark is disallowed. You can set the " +
            s"config '${SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key}' to 'false' to restore " +
            "the previous behavior. Note that multiple stateful operators will be disallowed.")
        }

        nodeToOutputWatermark.put(node.id, originWatermark)
        node

      case node: StateStoreWriter =>
        val stOpId = node.stateInfo.get.operatorId
        statefulOperatorIdToNodeId.put(stOpId, node.id)

        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        val finalInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          inputWatermarks.min
        } else {
          // We can't throw exception here, as we allow stateful operator to process without
          // watermark. E.g. streaming aggregation with update/complete mode.
          DEFAULT_WATERMARK_MS
        }

        val outputWatermarkMs = node.produceWatermark(finalInputWatermarkMs)
        nodeToOutputWatermark.put(node.id, outputWatermarkMs)
        nextStatefulOperatorToWatermark.put(stOpId, finalInputWatermarkMs)
        node

      case node =>
        // pass-through, but also consider multiple children like the case of union
        val inputWatermarks = getInputWatermarks(node, nodeToOutputWatermark)
        val finalInputWatermarkMs = if (inputWatermarks.nonEmpty) {
          val minCurrInputWatermarkMs = inputWatermarks.min
          minCurrInputWatermarkMs
        } else {
          DEFAULT_WATERMARK_MS
        }

        nodeToOutputWatermark.put(node.id, finalInputWatermarkMs)
        node
    }

    inputWatermarks.put(batchId, nextStatefulOperatorToWatermark.toMap)
    batchIdToWatermark.put(batchId, originWatermark)

    logDebug(s"global watermark for batch ID $batchId is set to $originWatermark")
    logDebug(s"input watermarks for batch ID $batchId is set to $nextStatefulOperatorToWatermark")
  }

  override def propagate(batchId: Long, plan: SparkPlan, originWatermark: Long): Unit = {
    if (batchId < 0) {
      // no-op
    } else if (isInitialized(batchId)) {
      val cached = batchIdToWatermark.get(batchId)
      assert(cached == originWatermark,
        s"Watermark has been changed for the same batch ID! Batch ID: $batchId, " +
          s"Value in cache: $cached, value given: $originWatermark")
    } else {
      logDebug(s"watermark for batch ID $batchId is received as $originWatermark, " +
        s"call site: ${Utils.getCallSite().longForm}")
      doSimulate(batchId, plan, originWatermark)
    }
  }

  private def getInputWatermark(batchId: Long, stateOpId: Long): Long = {
    if (batchId < 0) {
      0
    } else {
      assert(isInitialized(batchId), s"Watermark for batch ID $batchId is not yet set!")
      // In current Spark's logic, event time watermark cannot go down to negative. So even there is
      // no input watermark for operator, the final input watermark for operator should be 0L.
      val opWatermark = inputWatermarks(batchId).get(stateOpId)
      assert(opWatermark.isDefined, s"Watermark for batch ID $batchId and stateOpId $stateOpId " +
        "is not yet set!")
      Math.max(opWatermark.get, 0L)
    }
  }

  override def getInputWatermarkForLateEvents(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId - 1, stateOpId)

  override def getInputWatermarkForEviction(batchId: Long, stateOpId: Long): Long =
    getInputWatermark(batchId, stateOpId)

  override def purge(batchId: Long): Unit = {
    val keyIter = batchIdToWatermark.keySet().iterator()
    var stopIter = false
    while (keyIter.hasNext && !stopIter) {
      val currKey = keyIter.next()
      if (currKey <= batchId) {
        keyIter.remove()
        inputWatermarks.remove(currKey)
      } else {
        stopIter = true
      }
    }
  }
}

object WatermarkPropagator {
  val DEFAULT_WATERMARK_MS = -1L

  def apply(conf: SQLConf): WatermarkPropagator = {
    if (conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)) {
      new PropagateWatermarkSimulator
    } else {
      new UseSingleWatermarkPropagator
    }
  }

  def noop(): WatermarkPropagator = new NoOpWatermarkPropagator
}

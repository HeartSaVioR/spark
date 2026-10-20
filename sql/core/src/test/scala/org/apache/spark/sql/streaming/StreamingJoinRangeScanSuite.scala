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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.getJoinScanRange
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinRangeScanSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  test("optimized fixed-interval conditions retain bounded scans in both directions") {
    val left = MemoryStream[(Int, Long)].toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_micros($"time").as("leftTime"))
      .withWatermark("leftTime", "60 seconds")
    val right = MemoryStream[(Int, Long)].toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_micros($"time").as("rightTime"))
      .withWatermark("rightTime", "60 seconds")
    val conditions = Seq(
      ("rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 SECONDS",
        (0L, 10L), (-10L, 0L)),
      ("leftTime >= rightTime - INTERVAL 10 SECONDS AND leftTime <= rightTime",
        (0L, 10L), (-10L, 0L)),
      ("leftTime BETWEEN rightTime - INTERVAL 5 SECONDS AND rightTime + INTERVAL 5 SECONDS",
        (-5L, 5L), (-5L, 5L)),
      ("leftTime + INTERVAL 2 SECONDS >= rightTime - INTERVAL 3 SECONDS AND " +
        "leftTime + INTERVAL 2 SECONDS <= rightTime + INTERVAL 7 SECONDS",
        (-5L, 5L), (-5L, 5L)))
    conditions.foreach { case (condition, rightOffsets, leftOffsets) =>
      val joined = left.join(right, expr(s"leftKey = rightKey AND $condition"))
      val optimized = spark.sessionState.optimizer.execute(joined.queryExecution.analyzed)
      val join = optimized.collectFirst { case j: Join => j }.get
      for ((input, stored, timeName, (lowerSeconds, upperSeconds)) <- Seq(
          (join.left, join.right, "rightTime", rightOffsets),
          (join.right, join.left, "leftTime", leftOffsets))) {
        val indexedTime = stored.output.find(_.name == timeName).get
        val time = 100000001L
        val row = InternalRow.fromSeq(input.output.map { attribute =>
          if (attribute.name.endsWith("Key")) 1 else time
        })
        val (lower, upper) = getJoinScanRange(input.output, indexedTime, join.condition,
          useConstantOffsets = true).getOrElse(fail(s"No bounds: $condition on $timeName"))(row)
        val expectedLower = time + lowerSeconds * 1000000L
        val expectedUpper = time + upperSeconds * 1000000L
        assert(lower >= expectedLower - 1000L && lower <= expectedLower,
          s"Lost lower bound: $condition on $timeName")
        assert(upper >= expectedUpper && upper <= expectedUpper + 1000L,
          s"Lost upper bound: $condition on $timeName")
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    arrival <- Seq("together", "left first", "right first", "left restart", "right restart")
    shape <- Seq("between", "conjunction", "interval")
  } {
    test(s"event-time matching scan with an expiry column, V$version $arrival $shape") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "start", "end")
          .select($"leftKey", timestamp_millis($"start").as("leftTime"),
            timestamp_millis($"end").as("expiry"))
          .withWatermark("leftTime", "60 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_millis($"time").as("rightTime"))
          .withWatermark("rightTime", "60 seconds")
        val range = shape match {
          case "between" => "rightTime BETWEEN leftTime AND expiry"
          case "conjunction" => "rightTime >= leftTime AND rightTime <= expiry"
          case "interval" => "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 SECONDS"
        }
        val joined = left.join(right, expr(s"leftKey = rightKey AND $range"))
          .select(unix_millis($"rightTime") - 100000L)
        val offsets = Seq(0L, 1L, 2L, 1000L, 5000L, 10000L)
        // Include nonmatching rows immediately outside both inclusive boundaries.
        val rows = (Seq(-1L) ++ offsets ++ Seq(10001L)).map(t => (1, 100000L + t))
        val input = arrival match {
          case "together" =>
            Seq(StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 100000L, 110000L)), AddData(rightInput, rows: _*))))
          case "left first" =>
            Seq(AddData(leftInput, (1, 100000L, 110000L)), CheckNewAnswer(),
              AddData(rightInput, rows: _*))
          case "left restart" =>
            Seq(AddData(leftInput, (1, 100000L, 110000L)), CheckNewAnswer(),
              StopStream, StartStream(), AddData(rightInput, rows: _*))
          case "right restart" =>
            Seq(AddData(rightInput, rows: _*), CheckNewAnswer(),
              StopStream, StartStream(), AddData(leftInput, (1, 100000L, 110000L)))
          case "right first" =>
            Seq(AddData(rightInput, rows: _*), CheckNewAnswer(),
              AddData(leftInput, (1, 100000L, 110000L)))
        }
        testStream(joined)((input ++ Seq(
          CheckNewAnswer(offsets.map(Row(_)): _*),
          Execute { query =>
            assert(query.recentProgress.flatMap(_.stateOperators).forall { operator =>
              operator.numRowsRemoved == 0 && operator.numRowsDroppedByWatermark == 0
            })
          })): _*)
      }
    }
  }

  for {
    leftWatermark <- Seq(false, true)
    rightWatermark <- Seq(false, true)
    leftFirst <- Seq(false, true)
  } {
    test(s"microsecond scan bounds: watermarks=($leftWatermark,$rightWatermark), " +
        s"left first=$leftFirst") {
      withJoinConf(4) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "start", "end")
          .select($"leftKey", timestamp_micros($"start").as("leftTime"),
            timestamp_micros($"end").as("expiry"))
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_micros($"time").as("rightTime"))
        val leftStream = if (leftWatermark) left.withWatermark("leftTime", "60 seconds") else left
        val rightStream =
          if (rightWatermark) right.withWatermark("rightTime", "60 seconds") else right
        val joined = leftStream.join(rightStream,
          expr("leftKey = rightKey AND rightTime > leftTime AND rightTime < expiry"))
          .select(unix_micros($"expiry"), unix_micros($"rightTime"))
        val addLeft = AddData(leftInput,
          (1, 100000001L, 100000004L), (1, 100000001L, 100000006L))
        val addRight = AddData(rightInput,
          (1, 100000001L), (1, 100000002L), (1, 100000004L), (1, 100000006L))
        val input = if (leftFirst) {
          Seq(addLeft, CheckNewAnswer(), addRight)
        } else {
          Seq(addRight, CheckNewAnswer(), addLeft)
        }
        testStream(joined)((input ++ Seq(
          CheckNewAnswer((100000004L, 100000002L),
            (100000006L, 100000002L), (100000006L, 100000004L)),
          Execute { query =>
            assert(query.recentProgress.flatMap(_.stateOperators).forall { operator =>
              operator.numRowsRemoved == 0 && operator.numRowsDroppedByWatermark == 0
            })
          })): _*)
      }
    }
  }
}

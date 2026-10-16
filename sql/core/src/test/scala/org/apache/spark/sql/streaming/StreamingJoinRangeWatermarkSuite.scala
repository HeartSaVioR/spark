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
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinRangeWatermarkSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  for {
    version <- Seq(2, 3, 4)
    joinType <- Seq("inner", "left_outer")
    shape <- Seq("between", "conjunction", "upper", "pruned", "interval")
  } {
    test(s"SPARK-58904: expiry-column eviction, V$version $joinType $shape") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "start", "end")
          .select($"leftKey", timestamp_seconds($"start").as("leftTime"),
            timestamp_seconds($"end").as("expiry"))
          .withWatermark("leftTime", "60 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "60 seconds")
        val range = shape match {
          case "between" => "rightTime BETWEEN leftTime AND expiry"
          case "conjunction" => "rightTime >= leftTime AND rightTime <= expiry"
          case "interval" => "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES"
          case _ => "rightTime <= expiry"
        }
        val joined = left.join(right, expr(s"leftKey = rightKey AND $range"), joinType)
        // Retain the watermarked column for the upper-only case; explicitly test pruning too.
        val result = if (shape == "pruned") {
          joined.select($"leftKey", $"rightTime".cast("long"))
        } else {
          joined.select($"leftKey", $"rightTime".cast("long"), $"leftTime".cast("long"))
        }
        val matched = if (shape == "pruned") Row(1, 460L) else Row(1, 460L, 100L)
        val unmatched = if (shape == "pruned") Row(2, null) else Row(2, null, 100L)
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 400L)),
          // The watermark has passed the start time, but the interval has not expired.
          CheckNewAnswer(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForEviction.contains(340000L))
            assert(join.left.output.exists(_.name == "leftTime") == (shape != "pruned"))
          },
          AddData(rightInput, (1, 460L)),
          CheckNewAnswer(matched),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 1100L)),
          CheckNewAnswer((if (joinType == "left_outer") Seq(unmatched) else Seq.empty): _*)
        )
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    arrival <- Seq("together", "left first", "right first")
    shape <- Seq("between", "conjunction", "interval")
  } {
    test(s"SPARK-58904: expiry-column scan without eviction, V$version $arrival $shape") {
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
}

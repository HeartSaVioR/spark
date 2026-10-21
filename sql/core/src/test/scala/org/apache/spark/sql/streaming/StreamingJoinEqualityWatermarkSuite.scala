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
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinEqualityWatermarkSuite extends StreamTest {
  import testImplicits._

  for {
    version <- Seq(2, 3, 4)
    restart <- Seq(false, true)
  } {
    test(s"equality eviction follows the opposite watermark, V$version restart=$restart") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("a", "b")
          .select(timestamp_seconds($"a").as("la"), timestamp_seconds($"b").as("lb"))
          .withWatermark("la", "10 seconds")
        val right = rightInput.toDF().toDF("a", "b")
          .select(timestamp_seconds($"a").as("ra"), timestamp_seconds($"b").as("rb"))
          .withWatermark("rb", "10 seconds")
        val joined = left.join(right, expr("la = ra AND lb = rb"), "full_outer")
          .select($"la".cast("int"), $"lb".cast("int"),
            $"ra".cast("int"), $"rb".cast("int"))
        withTempDir { checkpoint =>
          val restartActions = if (restart) {
            Seq(StopStream, StartStream(checkpointLocation = checkpoint.getCanonicalPath))
          } else {
            Nil
          }
          testStream(joined)((Seq(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            MultiAddData(leftInput, (5, 50))(rightInput, (80, 50)),
            CheckNewAnswer(),
            MultiAddData(leftInput, (60, 100))(rightInput, (70, 50)),
            CheckNewAnswer(),
            AddData(leftInput, (70, 100)),
            // At watermark 40, left (5, 50) can still match fresh right input on rb=50.
            CheckNewAnswer()) ++ restartActions ++ Seq(
            AddData(rightInput, (5, 50)),
            CheckNewAnswer(Row(5, 50, 5, 50)),
            // Fresh on la, but already expired on lb: V4 must visit it below the old cutoff.
            AddData(leftInput, (90, 10)),
            CheckNewAnswer(Row(90, 10, null, null)),
            MultiAddData(leftInput, (200, 200))(rightInput, (200, 200)),
            CheckNewAnswer(Row(200, 200, 200, 200)),
            MultiAddData(leftInput, (210, 210))(rightInput, (210, 210)),
            CheckNewAnswer(Row(210, 210, 210, 210), Row(60, 100, null, null),
              Row(70, 100, null, null), Row(null, null, 80, 50), Row(null, null, 70, 50))
          )): _*)
        }
      }
    }
  }
}

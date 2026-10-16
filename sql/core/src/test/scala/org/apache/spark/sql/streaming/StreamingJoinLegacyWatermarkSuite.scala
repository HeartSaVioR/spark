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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinLegacyWatermarkSuite extends StreamTest {
  import testImplicits._

  private def removePinnedSetting(checkpoint: String, key: String): Unit = {
    val log = new OffsetSeqLog(spark, checkpoint + "/offsets")
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = m.conf - key)))
      case o: OffsetMap =>
        o.copy(metadata = o.metadata.copy(conf = o.metadata.conf - key))
    }
    val path = new Path(checkpoint + "/offsets", batchId.toString)
    assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
    assert(log.add(batchId, updated))
  }

  for {
    version <- Seq(2, 3, 4)
    logVersion <- Seq(1, 2)
    missingSetting <- Seq(false, true)
    shape <- Seq("expiry", "pruned expiry", "interval")
  } {
    test(s"SPARK-58904: legacy $shape restart, V$version log=$logVersion " +
        s"missing=$missingSetting") {
      val correctedKey = if (version == 4) {
        SQLConf.STREAMING_JOIN_V4_CORRECTED_WATERMARK_INDEX_ENABLED.key
      } else {
        SQLConf.STREAMING_JOIN_V1_TO_V3_CORRECTED_WATERMARK_ENABLED.key
      }
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> logVersion.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
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
          case "expiry" => "rightTime BETWEEN leftTime AND expiry"
          case "pruned expiry" => "rightTime <= expiry"
          case "interval" => "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES"
        }
        val joined = left.join(right, expr(s"leftKey = rightKey AND $range"))
          .select(unix_millis($"rightTime") - 100000L)
        val offsets = Seq(0L, 1L, 2L, 1000L, 5000L, 10000L)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          withSQLConf(correctedKey -> "false") {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              StreamProgressLockedActions(Seq(
                AddData(leftInput, (1, 100000L, 700000L)), AddData(rightInput, (9, 100000L)))),
              CheckNewAnswer(),
              StopStream)
          }
          if (missingSetting) removePinnedSetting(checkpoint, correctedKey)
          val warning = new LogAppender("legacy expiry join warning", maxEvents = 10000)
          withLogAppender(warning) {
            withSQLConf(correctedKey -> "true",
                SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "2") {
              // Compatibility preserves the old V4 scan and premature-eviction results.
              // The restart warning must identify the affected shape even before any eviction.
              val expectedOffsets = if (version == 4 && shape == "expiry") {
                offsets.take(2)
              } else {
                offsets
              }
              testStream(joined)(
                StartStream(checkpointLocation = checkpoint),
                AddData(rightInput, offsets.map(t => (1, 100000L + t)): _*),
                CheckNewAnswer(expectedOffsets.map(Row(_)): _*),
                Execute { query =>
                  val join = query.lastExecution.executedPlan.collectFirst {
                    case j: StreamingSymmetricHashJoinExec => j
                  }.get
                  assert(join.useLegacyWatermarkSemantics)
                  assert(join.stateFormatVersion == version)
                  assert(join.left.output.exists(_.name == "leftTime") ==
                    (shape != "pruned expiry"))
                },
                StreamProgressLockedActions(Seq(
                  AddData(leftInput, (8, 400000L, 1000000L)),
                  AddData(rightInput, (9, 400000L)))),
                CheckNewAnswer(),
                AddData(rightInput, (9, 400000L)),
                CheckNewAnswer(),
                AddData(rightInput, (1, 460000L)),
                CheckNewAnswer((if (shape == "expiry") Seq.empty else Seq(Row(360000L))): _*),
                StopStream)
            }
          }
          val messages = warning.loggingEvents.map(_.getMessage.getFormattedMessage)
            .filter(_.startsWith("SPARK-58904: This restarted stream-stream join"))
          val expectedWarning = "SPARK-58904: This restarted stream-stream join " +
            "(operator 0, type Inner) uses legacy watermark behavior. " +
            "This shape has potential consequences: left-state cleanup: " +
            "can retain state or change match results. " +
            "Review the streaming migration guide before changing this checkpoint."
          assert(messages.toSeq == (if (shape == "interval") Nil else Seq(expectedWarning)))
          val pinned = new OffsetSeqLog(spark, checkpoint + "/offsets")
            .getLatest().get._2.metadataOpt.get.conf
          assert(pinned.get(correctedKey).contains("false"))
        }
      }
    }
  }
}

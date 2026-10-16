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

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinCheckpointCompatibilitySuite extends StreamTest {
  import testImplicits._

  private val watermarkVersionKey = "spark.sql.streaming.join.watermarkVersion"

  private def setWatermarkVersion(checkpoint: String, version: Option[String]): Unit = {
    def update(conf: Map[String, String]): Map[String, String] = {
      (conf - watermarkVersionKey) ++ version.map(watermarkVersionKey -> _)
    }
    val offsetsPath = checkpoint + "/offsets"
    val log = new OffsetSeqLog(spark, offsetsPath)
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = update(m.conf))))
      case o: OffsetMap =>
        o.copy(metadata = o.metadata.copy(conf = update(o.metadata.conf)))
    }
    val path = new Path(offsetsPath, batchId.toString)
    assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
    assert(log.add(batchId, updated))
  }

  private def snapshot(directory: File): Map[String, Seq[Byte]] = {
    Using.resource(Files.walk(directory.toPath)) { paths =>
      paths.iterator().asScala.filter(Files.isRegularFile(_)).map { path =>
        directory.toPath.relativize(path).toString -> Files.readAllBytes(path).toSeq
      }.toMap
    }
  }

  private def pinnedVersion(checkpoint: String): Option[String] = {
    new OffsetSeqLog(spark, checkpoint + "/offsets").getLatest().get._2.metadataOpt.get
      .conf.get(watermarkVersionKey)
  }

  private def withJoinConf(version: Int, logVersion: Int)(f: => Unit): Unit = {
    withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> logVersion.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  private def joinInputs(shape: String): (MemoryStream[(Int, Int)],
      MemoryStream[(Int, Int)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val leftData = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
    val rightData = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
    val left = if (shape == "equality") {
      leftData.withWatermark("leftTime", "100 seconds")
    } else {
      leftData
    }
    val right = if (shape != "no watermark") {
      rightData.withWatermark("rightTime", "100 seconds")
    } else {
      rightData
    }
    val condition = if (shape == "range") {
      "leftKey = rightKey AND leftTime > rightTime - INTERVAL 5 SECONDS"
    } else {
      "leftKey = rightKey AND leftTime = rightTime"
    }
    (leftInput, rightInput, left.join(right, expr(condition)).select($"leftKey", $"rightKey"))
  }

  for {
    version <- Seq(1, 2, 3, 4)
    logVersion <- Seq(1, 2)
    oldVersion <- Seq(None, Some("1"))
  } {
    test(s"SPARK-58904: reject old range checkpoint, V$version log=$logVersion old=$oldVersion") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = joinInputs("range")
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (1, 10)),
            CheckNewAnswer(Row(1, 1)),
            StopStream)
          // Emulate old offset metadata. State is written by the current implementation;
          // rejection must not open or reinterpret it, regardless of its physical layout.
          setWatermarkVersion(checkpoint, oldVersion)
          val stateBefore = snapshot(new File(checkpoint, "state"))
          val commitsBefore = snapshot(new File(checkpoint, "commits"))
          for (attempt <- 1 to 2) {
            // Session defaults must not bypass the checkpoint-pinned version or state format.
            withSQLConf(watermarkVersionKey -> "2",
                SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "2") {
              testStream(joined)(
                StartStream(checkpointLocation = checkpoint),
                AddData(rightInput, (2, 20 + attempt)),
                ExpectFailure[AnalysisException](assertFailure = error => {
                  checkError(error.asInstanceOf[AnalysisException],
                    condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                    parameters = Map("operatorId" -> "0",
                      "stateFormatVersion" -> version.toString))
                }))
            }
            assert(snapshot(new File(checkpoint, "state")) == stateBefore)
            assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
            assert(pinnedVersion(checkpoint).contains("1"))
          }
        }
      }
    }
  }

  for {
    version <- Seq(1, 2, 3, 4)
    logVersion <- Seq(1, 2)
    shape <- Seq("equality", "no watermark", "range")
  } {
    test(s"SPARK-58904: resume compatible $shape checkpoint, V$version log=$logVersion") {
      withJoinConf(version, logVersion) {
        val (leftInput, rightInput, joined) = joinInputs(shape)
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (2, 10)),
            CheckNewAnswer(),
            StopStream)
          assert(pinnedVersion(checkpoint).contains("2"))
          val expectedVersion = if (shape == "range") "2" else "1"
          if (shape != "range") setWatermarkVersion(checkpoint, None)
          withSQLConf(watermarkVersionKey -> "1",
              SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "2") {
            testStream(joined)(
              StartStream(checkpointLocation = checkpoint),
              AddData(rightInput, (1, 10)),
              CheckNewAnswer(Row(1, 1)),
              Execute { query =>
                val join = query.lastExecution.executedPlan.collect {
                  case j: StreamingSymmetricHashJoinExec => j
                }.head
                assert(join.stateFormatVersion == version)
              },
              StopStream)
          }
          assert(pinnedVersion(checkpoint).contains(expectedVersion))
        }
      }
    }
  }

  Seq(2, 4).foreach { version =>
    test(s"SPARK-58904: reject an incompatible checkpoint retrying batch zero, V$version") {
      withJoinConf(version, logVersion = 1) {
        val (leftInput, rightInput, joined) = joinInputs("range")
        withTempDir { checkpointDir =>
          val checkpoint = checkpointDir.getCanonicalPath
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            MultiAddData(leftInput, (1, 10))(rightInput, (1, 10)),
            CheckNewAnswer(Row(1, 1)),
            StopStream)
          val offsets = new OffsetSeqLog(spark, checkpoint + "/offsets")
          assert(offsets.getLatest().get._1 == 0L)
          setWatermarkVersion(checkpoint, None)
          // Emulate a crash after state was written but before the first batch committed.
          val commit = new Path(checkpoint, "commits/0")
          assert(commit.getFileSystem(spark.sessionState.newHadoopConf()).delete(commit, false))
          val before = snapshot(new File(checkpoint, "state"))
          val commitsBefore = snapshot(new File(checkpoint, "commits"))
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint),
            ExpectFailure[AnalysisException](assertFailure = error => {
              checkError(error.asInstanceOf[AnalysisException],
                condition = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
                parameters = Map("operatorId" -> "0", "stateFormatVersion" -> version.toString))
            }))
          assert(snapshot(new File(checkpoint, "state")) == before)
          assert(snapshot(new File(checkpoint, "commits")) == commitsBefore)
        }
      }
    }
  }
}

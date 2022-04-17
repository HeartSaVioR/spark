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

import java.io.File
import java.sql.Timestamp

import scala.collection.JavaConverters

import org.scalatest.time.{Minute, Span}

import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{count, expr}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._

class RocksDBStateStoreIntegrationSuite extends StreamTest {
  import testImplicits._

  test("RocksDBStateStore") {
    withTempDir { dir =>
      val input = MemoryStream[Int]
      val conf = Map(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName)

      testStream(input.toDF.groupBy().count(), outputMode = OutputMode.Update)(
        StartStream(checkpointLocation = dir.getAbsolutePath, additionalConfs = conf),
        AddData(input, 1, 2, 3),
        CheckAnswer(3),
        AssertOnQuery { q =>
          // Verify that RocksDBStateStore by verify the state checkpoints are [version].zip
          val storeCheckpointDir = StateStoreId(
            dir.getAbsolutePath + "/state", 0, 0).storeCheckpointLocation()
          val storeCheckpointFile = storeCheckpointDir + "/1.zip"
          new File(storeCheckpointFile).exists()
        }
      )
    }
  }

  test("SPARK-36236: query progress contains only the expected RocksDB store custom metrics") {
    // fails if any new custom metrics are added to remind the author of API changes
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        (SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10"),
        (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName),
        (SQLConf.CHECKPOINT_LOCATION.key -> dir.getCanonicalPath),
        (SQLConf.SHUFFLE_PARTITIONS.key, "1")) {
        val inputData = MemoryStream[Int]

        val query = inputData.toDS().toDF("value")
          .select('value)
          .groupBy($"value")
          .agg(count("*"))
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()
        try {
          inputData.addData(1, 2)
          inputData.addData(2, 3)
          query.processAllAvailable()

          val progress = query.lastProgress
          assert(progress.stateOperators.length > 0)
          // Should emit new progresses every 10 ms, but we could be facing a slow Jenkins
          eventually(timeout(Span(1, Minute))) {
            val nextProgress = query.lastProgress
            assert(nextProgress != null, "progress is not yet available")
            assert(nextProgress.stateOperators.length > 0, "state operators are missing in metrics")
            val stateOperatorMetrics = nextProgress.stateOperators(0)
            assert(JavaConverters.asScalaSet(stateOperatorMetrics.customMetrics.keySet) === Set(
              "rocksdbGetLatency", "rocksdbCommitCompactLatency", "rocksdbBytesCopied",
              "rocksdbPutLatency", "rocksdbCommitPauseLatency", "rocksdbFilesReused",
              "rocksdbCommitWriteBatchLatency", "rocksdbFilesCopied", "rocksdbSstFileSize",
              "rocksdbCommitCheckpointLatency", "rocksdbZipFileBytesUncompressed",
              "rocksdbCommitFlushLatency", "rocksdbCommitFileSyncLatencyMs", "rocksdbGetCount",
              "rocksdbPutCount", "rocksdbTotalBytesRead", "rocksdbTotalBytesWritten",
              "rocksdbReadBlockCacheHitCount", "rocksdbReadBlockCacheMissCount",
              "rocksdbTotalBytesReadByCompaction", "rocksdbTotalBytesWrittenByCompaction",
              "rocksdbTotalCompactionLatencyMs", "rocksdbWriterStallLatencyMs",
              "rocksdbTotalBytesReadThroughIterator"))
          }
        } finally {
          query.stop()
        }
      }
    }
  }

  testQuietly("SPARK-36519: store RocksDB format version in the checkpoint") {
    def getFormatVersion(query: StreamingQuery): Int = {
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.sparkSession
        .sessionState.conf.getConf(SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION)
    }

    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { dir =>
        val inputData = MemoryStream[Int]

        def startQuery(): StreamingQuery = {
          inputData.toDS().toDF("value")
            .select('value)
            .groupBy($"value")
            .agg(count("*"))
            .writeStream
            .format("console")
            .option("checkpointLocation", dir.getCanonicalPath)
            .outputMode("complete")
            .start()
        }

        // The format version should be 5 by default
        var query = startQuery()
        inputData.addData(1, 2)
        query.processAllAvailable()
        assert(getFormatVersion(query) == 5)
        query.stop()

        // Setting the format version manually should not overwrite the value in the checkpoint
        withSQLConf(SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION.key -> "4") {
          query = startQuery()
          inputData.addData(1, 2)
          query.processAllAvailable()
          assert(getFormatVersion(query) == 5)
          query.stop()
        }
      }
    }
  }

  testQuietly("SPARK-36519: RocksDB format version can be set by the SQL conf") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      // Set an unsupported RocksDB format version and the query should fail if it's passed down
      // into RocksDB
      SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION.key -> "100") {
      val inputData = MemoryStream[Int]
      val query = inputData.toDS().toDF("value")
        .select('value)
        .groupBy($"value")
        .agg(count("*"))
        .writeStream
        .format("console")
        .outputMode("complete")
        .start()
      inputData.addData(1, 2)
      val e = intercept[StreamingQueryException](query.processAllAvailable())
      assert(e.getCause.getCause.getMessage.contains("Unsupported BlockBasedTable format_version"))
    }
  }

  test("SPARK-38684: outer join works correctly even if processing input rows and " +
    "evicting state rows for same grouping key happens in the same micro-batch") {

    // The test is to demonstrate the correctness issue in outer join before SPARK-38684.
    withSQLConf(
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {

      val input1 = MemoryStream[(Timestamp, String, String)]
      val df1 = input1.toDF
        .selectExpr("_1 as eventTime", "_2 as id", "_3 as comment")
        .withWatermark("eventTime", "0 second")

      val input2 = MemoryStream[(Timestamp, String, String)]
      val df2 = input2.toDF
        .selectExpr("_1 as eventTime", "_2 as id", "_3 as comment")
        .withWatermark("eventTime", "0 second")

      val joined = df1.as("left")
        .join(df2.as("right"),
          expr("""
                 |left.id = right.id AND left.eventTime BETWEEN
                 |  right.eventTime - INTERVAL 30 seconds AND
                 |  right.eventTime + INTERVAL 30 seconds
             """.stripMargin),
          joinType = "leftOuter")

      testStream(joined)(
        MultiAddData(
          (input1, Seq((Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "left in batch 1"))),
          (input2, Seq((Timestamp.valueOf("2020-01-02 00:01:00"), "abc", "right in batch 1")))
        ),
        CheckNewAnswer(),
        MultiAddData(
          (input1, Seq((Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "left in batch 2"))),
          (input2, Seq((Timestamp.valueOf("2020-01-02 01:01:00"), "abc", "right in batch 2")))
        ),
        // watermark advanced to "2020-01-02 00:00:00"
        CheckNewAnswer(),
        AddData(input1, (Timestamp.valueOf("2020-01-02 01:30:00"), "abc", "left in batch 3")),
        // watermark advanced to "2020-01-02 01:00:00"
        CheckNewAnswer(
          (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "left in batch 1", null, null, null)
        ),
        // left side state should still contain "left in batch 2" and "left in batch 3"
        // we should see both rows in the left side since
        // - "left in batch 2" is going to be evicted in this batch
        // - "left in batch 3" is going to be matched with new row in right side
        AddData(input2,
          (Timestamp.valueOf("2020-01-02 01:30:10"), "abc", "match with left in batch 3")),
        // watermark advanced to "2020-01-02 01:01:00"
        CheckNewAnswer(
          (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "left in batch 2",
            null, null, null),
          (Timestamp.valueOf("2020-01-02 01:30:00"), "abc", "left in batch 3",
            Timestamp.valueOf("2020-01-02 01:30:10"), "abc", "match with left in batch 3")
        )
      )
    }
  }
}


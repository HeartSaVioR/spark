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

import java.sql.Timestamp

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions.{count, expr, sum, timestamp_seconds, window}

class ChainedStatefulOperatorsSuite
  extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  import testImplicits._

  before {
    SparkSession.setActiveSession(spark)  // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator   // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  test("stream deduplication -> aggregation, append mode") {
    withSQLConf("spark.sql.streaming.unsupportedOperationCheck" -> "false") {
      val inputData = MemoryStream[Int]

      val deduplication = inputData.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .dropDuplicates("value", "eventTime")

      val windowedAggregation = deduplication
        .groupBy(window($"eventTime", "5 seconds") as 'window)
        .agg(count("*") as 'count, sum("value") as 'sum)
        .select($"window".getField("start").cast("long").as[Long],
          $"count".as[Long])

      testStream(windowedAggregation)(
        // FIXME: we should revisit our watermark condition... we don't allow watermark to be
        //  set to negative and ensure it's at least 0, but then the inputs with timestamp 0
        //  as event time can be dropped.
        AddData(inputData, 1 to 15: _*),
        // op1 W (0, 0)
        // input: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        // deduplicated: None
        // output: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        // state: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        // op2 W (0, 0)
        // agg: [0, 5) 4, [5, 10) 5 [10, 15) 5, [15, 20) 1
        // output: None
        // state: [0, 5) 4, [5, 10) 5 [10, 15) 5, [15, 20) 1

        // no-data batch triggered

        // op1 W (5, 5)
        // agg: None
        // output: None
        // state: 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
        // op2 W (0, 5)
        // agg: None
        // output: [0, 5) 4
        // state: [5, 10) 5 [10, 15) 5, [15, 20) 1
        CheckNewAnswer((0, 4))
      )
    }
  }

  test("stream-stream time interval left outer join -> aggregation, append mode") {
    withSQLConf("spark.sql.streaming.unsupportedOperationCheck" -> "false") {
      val input1 = MemoryStream[(Timestamp, String, String)]
      val df1 = input1.toDF
        .selectExpr("_1 as eventTime", "_2 as id", "_3 as comment")
        .withWatermark(s"eventTime", "2 minutes")

      val input2 = MemoryStream[(Timestamp, String, String)]
      val df2 = input2.toDF
        .selectExpr("_1 as eventTime", "_2 as id", "_3 as name")
        .withWatermark(s"eventTime", "4 minutes")

      val joined = df1.as("left")
        .join(df2.as("right"),
          expr("""
                 |left.id = right.id AND left.eventTime BETWEEN
                 |  right.eventTime - INTERVAL 40 seconds AND
                 |  right.eventTime + INTERVAL 30 seconds
             """.stripMargin),
          joinType = "leftOuter")

      val windowAggregation = joined
        .groupBy(window($"left.eventTime", "30 seconds"))
        .agg(count("*").as("cnt"))
        .selectExpr("window.start AS window_start", "window.end AS window_end", "cnt")

      testStream(windowAggregation)(
        MultiAddData(
          (input1, Seq(
            (Timestamp.valueOf("2020-01-01 00:00:00"), "abc", "has no join partner"),
            (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "joined with A"),
            (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "joined with B"))),
          (input2, Seq(
            (Timestamp.valueOf("2020-01-02 00:00:10"), "abc", "A"),
            (Timestamp.valueOf("2020-01-02 00:59:59"), "abc", "B"),
            (Timestamp.valueOf("2020-01-02 02:00:00"), "abc", "C")))
        ),

        // joined result from data batch
        // (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "joined with A",
        //  Timestamp.valueOf("2020-01-02 00:00:10"), "abc", "A")
        // (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "joined with B",
        //  Timestamp.valueOf("2020-01-02 00:59:59"), "abc", "B")

        // state row in aggregation
        // (Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:30"), 1)
        // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)

        // no-data batch

        // watermark in left input: 2020-01-02 00:58:00
        // watermark in right input: 2020-01-02 01:56:00

        // state watermark on stream-stream join
        // min watermark on filter: 0
        // min watermark on eviction: 2020-01-02 00:58:00
        // state watermark on eviction: 2020-01-02 00:57:20
        //
        // joined result from no-data batch (eviction)
        // (Timestamp.valueOf("2020-01-01 00:00:00"), "abc", "has no join partner",
        //  null, null, null)

        // streaming aggregation
        // watermark: 2021-01-02 00:57:30
        //
        // state row in aggregation
        // (Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:30"), 1)
        // (Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:30"), 1)
        // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
        //
        // state row in aggregation after eviction
        // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
        CheckNewAnswer(
          (Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:30"), 1),
          (Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:30"), 1)
        ),

        // FIXME: check the join & aggregation nodes and see the watermark
        //  for filtering vs eviction
        MultiAddData(
          (input1, Seq((Timestamp.valueOf("2020-01-05 00:00:00"), "abc", "joined with D"))),
          (input2, Seq((Timestamp.valueOf("2020-01-05 00:00:10"), "abc", "D")))
        ),

        // joined result from data batch
        // (Timestamp.valueOf("2020-01-05 00:00:00"), "abc", "joined with D",
        //  Timestamp.valueOf("2020-01-05 00:00:10"), "abc", "D")

        // state row in aggregation
        // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
        // (Timestamp.valueOf("2020-01-05 00:00:00"), Timestamp.valueOf("2020-01-05 01:00:30"), 1)

        // no-data batch

        // watermark in left input: 2020-01-04 23:58:00 - 2 mins
        // watermark in right input: 2020-01-04 23:56:10 - 4 mins

        // state watermark on stream-stream join
        // min watermark on filter: 2020-01-02 00:57:30
        // min watermark on eviction: 2020-01-04 23:56:10
        // state watermark on eviction: 2020-01-04 23:55:30
        //
        // joined result from no-data batch (eviction)
        // none

        // streaming aggregation
        // watermark: 2020-01-04 23:55:40
        //
        // state row in aggregation
        // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
        // (Timestamp.valueOf("2020-01-05 00:00:00"), Timestamp.valueOf("2020-01-05 01:00:30"), 1)
        //
        // state row in aggregation after eviction
        // (Timestamp.valueOf("2020-01-05 00:00:00"), Timestamp.valueOf("2020-01-05 01:00:30"), 1)
        CheckNewAnswer(
          (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
        )
        // FIXME: check the join & aggregation nodes and see the watermark for
        //  filtering vs eviction
      )
    }
  }
}

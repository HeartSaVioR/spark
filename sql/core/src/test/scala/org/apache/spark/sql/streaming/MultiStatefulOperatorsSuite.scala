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
import org.apache.spark.sql.execution.streaming.{MemoryStream, StateStoreSaveExec, StreamingSymmetricHashJoinExec}
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

// Tests for the multiple stateful operators support.
class MultiStatefulOperatorsSuite
  extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  import testImplicits._

  before {
    SparkSession.setActiveSession(spark) // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  test("window agg -> window agg, append mode") {
    val inputData = MemoryStream[Int]

    val stream = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 seconds")
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .groupBy(window($"window", "10 seconds"))
      .agg(count("*").as("count"), sum("count").as("sum"))
      .select($"window".getField("start").cast("long").as[Long],
        $"count".as[Long], $"sum".as[Long])

    testStream(stream)(
      AddData(inputData, 10 to 21: _*),
      // op1 W (0, 0)
      // agg: [10, 15) 5, [15, 20) 5, [20, 25) 2
      // output: None
      // state: [10, 15) 5, [15, 20) 5, [20, 25) 2
      // op2 W (0, 0)
      // agg: None
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 21)
      // agg: None
      // output: [10, 15) 5, [15, 20) 5
      // state: [20, 25) 2
      // op2 W (0, 21)
      // agg: [10, 20) (2, 10)
      // output: [10, 20) (2, 10)
      // state: None
      CheckNewAnswer((10, 2, 10)),
      assertNumStateRows(Seq(0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0)),

      AddData(inputData, 10 to 29: _*),
      // op1 W (21, 21)
      // agg: [10, 15) 5 - late, [15, 20) 5 - late, [20, 25) 5, [25, 30) 5
      // output: None
      // state: [20, 25) 7, [25, 30) 5
      // op2 W (21, 21)
      // agg: None
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (21, 29)
      // agg: None
      // output: [20, 25) 7
      // state: [25, 30) 5
      // op2 W (21, 29)
      // agg: [20, 30) (1, 7)
      // output: None
      // state: [20, 30) (1, 7)
      CheckNewAnswer(),
      assertNumStateRows(Seq(1, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 2)),

      // Move the watermark.
      AddData(inputData, 30, 31),
      // op1 W (29, 29)
      // agg: [30, 35) 2
      // output: None
      // state: [25, 30) 5 [30, 35) 2
      // op2 W (29, 29)
      // agg: None
      // output: None
      // state: [20, 30) (1, 7)

      // no-data batch triggered

      // op1 W (29, 31)
      // agg: None
      // output: [25, 30) 5
      // state: [30, 35) 2
      // op2 W (29, 31)
      // agg: [20, 30) (2, 12)
      // output: [20, 30) (2, 12)
      // state: None
      CheckNewAnswer((20, 2, 12)),
      assertNumStateRows(Seq(0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("agg -> agg -> agg, append mode") {
    val inputData = MemoryStream[Int]

    val stream = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 seconds")
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .groupBy(window(window_time($"window"), "10 seconds"))
      .agg(count("*").as("count"), sum("count").as("sum"))
      .groupBy(window(window_time($"window"), "20 seconds"))
      .agg(count("*").as("count"), sum("sum").as("sum"))
      .select(
        $"window".getField("start").cast("long").as[Long],
        $"window".getField("end").cast("long").as[Long],
        $"count".as[Long], $"sum".as[Long])

    testStream(stream)(
      AddData(inputData, 0 to 37: _*),
      // op1 W (0, 0)
      // agg: [0, 5) 5, [5, 10) 5, [10, 15) 5, [15, 20) 5, [20, 25) 5, [25, 30) 5, [30, 35) 5,
      //   [35, 40) 3
      // output: None
      // state: [0, 5) 5, [5, 10) 5, [10, 15) 5, [15, 20) 5, [20, 25) 5, [25, 30) 5, [30, 35) 5,
      //   [35, 40) 3
      // op2 W (0, 0)
      // agg: None
      // output: None
      // state: None
      // op3 W (0, 0)
      // agg: None
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 37)
      // agg: None
      // output: [0, 5) 5, [5, 10) 5, [10, 15) 5, [15, 20) 5, [20, 25) 5, [25, 30) 5, [30, 35) 5
      // state: [35, 40) 3
      // op2 W (0, 37)
      // agg: [0, 10) (2, 10), [10, 20) (2, 10), [20, 30) (2, 10), [30, 40) (1, 5)
      // output: [0, 10) (2, 10), [10, 20) (2, 10), [20, 30) (2, 10)
      // state: [30, 40) (1, 5)
      // op3 W (0, 37)
      // agg: [0, 20) (2, 20), [20, 40) (1, 10)
      // output: [0, 20) (2, 20)
      // state: [20, 40) (1, 10)
      CheckNewAnswer((0, 20, 2, 20)),
      assertNumStateRows(Seq(1, 1, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0, 0)),

      AddData(inputData, 30 to 60: _*),
      // op1 W (37, 37)
      // dropped rows: [30, 35), 1 row <= note that 35, 36, 37 are still in effect
      // agg: [35, 40) 8, [40, 45) 5, [45, 50) 5, [50, 55) 5, [55, 60) 5, [60, 65) 1
      // output: None
      // state: [35, 40) 8, [40, 45) 5, [45, 50) 5, [50, 55) 5, [55, 60) 5, [60, 65) 1
      // op2 W (37, 37)
      // output: None
      // state: [30, 40) (1, 5)
      // op3 W (37, 37)
      // output: None
      // state: [20, 40) (1, 10)

      // no-data batch
      // op1 W (37, 60)
      // output: [35, 40) 8, [40, 45) 5, [45, 50) 5, [50, 55) 5, [55, 60) 5
      // state: [60, 65) 1
      // op2 W (37, 60)
      // agg: [30, 40) (2, 13), [40, 50) (2, 10), [50, 60), (2, 10)
      // output: [30, 40) (2, 13), [40, 50) (2, 10), [50, 60), (2, 10)
      // state: None
      // op3 W (37, 60)
      // agg: [20, 40) (2, 23), [40, 60) (2, 20)
      // output: [20, 40) (2, 23), [40, 60) (2, 20)
      // state: None

      CheckNewAnswer((20, 40, 2, 23), (40, 60, 2, 20)),
      assertNumStateRows(Seq(0, 0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0, 1))
    )
  }

  test("stream deduplication -> aggregation, append mode") {
    val inputData = MemoryStream[Int]

    val deduplication = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates("value", "eventTime")

    val windowedAggregation = deduplication
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"), sum("value").as("sum"))
      .select($"window".getField("start").cast("long").as[Long],
        $"count".as[Long])

    testStream(windowedAggregation)(
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

      // op1 W (0, 5)
      // agg: None
      // output: None
      // state: 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
      // op2 W (0, 5)
      // agg: None
      // output: [0, 5) 4
      // state: [5, 10) 5 [10, 15) 5, [15, 20) 1
      CheckNewAnswer((0, 4)),
      assertNumStateRows(Seq(3, 10)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("join -> window agg, append mode") {
    val input1 = MemoryStream[Int]
    val inputDF1 = input1.toDF()
      .withColumnRenamed("value", "value1")
      .withColumn("eventTime1", timestamp_seconds($"value1"))
      .withWatermark("eventTime1", "0 seconds")

    val input2 = MemoryStream[Int]
    val inputDF2 = input2.toDF()
      .withColumnRenamed("value", "value2")
      .withColumn("eventTime2", timestamp_seconds($"value2"))
      .withWatermark("eventTime2", "0 seconds")

    val stream = inputDF1.join(inputDF2, expr("eventTime1 = eventTime2"), "inner")
      .groupBy(window($"eventTime1", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(stream)(
      MultiAddData(input1, 1 to 4: _*)(input2, 1 to 4: _*),

      // op1 W (0, 0)
      // join output: (1, 1), (2, 2), (3, 3), (4, 4)
      // state: (1, 1), (2, 2), (3, 3), (4, 4)
      // op2 W (0, 0)
      // agg: [0, 5) 4
      // output: None
      // state: [0, 5) 4

      // no-data batch triggered

      // op1 W (0, 4)
      // join output: None
      // state: None
      // op2 W (0, 4)
      // agg: None
      // output: None
      // state: [0, 5) 4
      CheckNewAnswer(),
      assertNumStateRows(Seq(1, 0)),
      assertNumRowsDroppedByWatermark(Seq(0, 0)),

      // Move the watermark
      MultiAddData(input1, 5)(input2, 5),

      // op1 W (4, 4)
      // join output: (5, 5)
      // state: (5, 5)
      // op2 W (4, 4)
      // agg: [5, 10) 1
      // output: None
      // state: [0, 5) 4, [5, 10) 1

      // no-data batch triggered

      // op1 W (4, 5)
      // join output: None
      // state: None
      // op2 W (4, 5)
      // agg: None
      // output: [0, 5) 4
      // state: [5, 10) 1
      CheckNewAnswer((0, 4)),
      assertNumStateRows(Seq(1, 0)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("aggregation -> stream deduplication, append mode") {
    val inputData = MemoryStream[Int]

    val aggStream = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 seconds")
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .withColumn("windowEnd", expr("window.end"))

    // dropDuplicates from aggStream without event time column for dropDuplicates - the
    // state does not get trimmed due to watermark advancement.
    val dedupNoEventTime = aggStream
      .dropDuplicates("count", "windowEnd")
      .select(
        $"windowEnd".cast("long").as[Long],
        $"count".as[Long])

    testStream(dedupNoEventTime)(
      AddData(inputData, 1, 5, 10, 15),

      // op1 W (0, 0)
      // agg: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // output: None
      // state: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // op2 W (0, 0)
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 15)
      // agg: None
      // output: [0, 5) 1, [5, 10) 1, [10, 15) 1
      // state: [15, 20) 1
      // op2 W (0, 15)
      // output: (5, 1), (10, 1), (15, 1)
      // state: (5, 1), (10, 1), (15, 1)

      CheckNewAnswer((5, 1), (10, 1), (15, 1)),
      assertNumStateRows(Seq(3, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )

    // Similar to the above but add event time. The dedup state will get trimmed.
    val dedupWithEventTime = aggStream
      .withColumn("windowTime", expr("window_time(window)"))
      .withColumn("windowTimeMicros", expr("unix_micros(windowTime)"))
      .dropDuplicates("count", "windowEnd", "windowTime")
      .select(
        $"windowEnd".cast("long").as[Long],
        $"windowTimeMicros".cast("long").as[Long],
        $"count".as[Long])

    testStream(dedupWithEventTime)(
      AddData(inputData, 1, 5, 10, 15),

      // op1 W (0, 0)
      // agg: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // output: None
      // state: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // op2 W (0, 0)
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 15)
      // agg: None
      // output: [0, 5) 1, [5, 10) 1, [10, 15) 1
      // state: [15, 20) 1
      // op2 W (0, 15)
      // output: (5, 4999999, 1), (10, 9999999, 1), (15, 14999999, 1)
      // state: None - trimmed by watermark

      CheckNewAnswer((5, 4999999, 1), (10, 9999999, 1), (15, 14999999, 1)),
      assertNumStateRows(Seq(0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("join with range join on non-time intervals -> window agg, append mode, shouldn't fail") {
    val input1 = MemoryStream[Int]
    val inputDF1 = input1.toDF()
      .withColumnRenamed("value", "value1")
      .withColumn("eventTime1", timestamp_seconds($"value1"))
      .withColumn("v1", timestamp_seconds($"value1"))
      .withWatermark("eventTime1", "0 seconds")

    val input2 = MemoryStream[(Int, Int)]
    val inputDF2 = input2.toDS().toDF("start", "end")
      .withColumn("eventTime2Start", timestamp_seconds($"start"))
      .withColumn("start2", timestamp_seconds($"start"))
      .withColumn("end2", timestamp_seconds($"end"))
      .withWatermark("eventTime2Start", "0 seconds")

    val stream = inputDF1.join(inputDF2,
      expr("v1 >= start2 AND v1 < end2 " +
        "AND eventTime1 = start2"), "inner")
      .groupBy(window($"eventTime1", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(stream)(
      AddData(input1, 1, 2, 3, 4),
      AddData(input2, (1, 2), (2, 3), (3, 4), (4, 5)),
      CheckNewAnswer(),
      assertNumStateRows(Seq(1, 0)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("stream-stream time interval left outer join -> aggregation, append mode") {
    val input1 = MemoryStream[(String, Timestamp)]
    val input2 = MemoryStream[(String, Timestamp)]

    val s1 = input1.toDF()
      .selectExpr("_1 AS id1", "_2 AS timestamp1")
      .withWatermark("timestamp1", "0 seconds")
      .as("s1")

    val s2 = input2.toDF()
      .selectExpr("_1 AS id2", "_2 AS timestamp2")
      .withWatermark("timestamp2", "0 seconds")
      .as("s2")

    val s3 = s1.join(s2, expr("s1.id1 = s2.id2 AND (s1.timestamp1 BETWEEN " +
      "s2.timestamp2 - INTERVAL 1 hour AND s2.timestamp2 + INTERVAL 1 hour)"), "leftOuter")

    val agg = s3.groupBy(window($"timestamp1", "10 minutes"))
      .agg(count("*").as("cnt"))
      .selectExpr("CAST(window.start AS STRING) AS window_start",
        "CAST(window.end AS STRING) AS window_end", "cnt")

    // for ease of verification, we change the session timezone to UTC
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      testStream(agg)(
        MultiAddData(
          (input1, Seq(
            ("1", Timestamp.valueOf("2023-01-01 01:00:10")),
            ("2", Timestamp.valueOf("2023-01-01 01:00:30")))
          ),
          (input2, Seq(
            ("1", Timestamp.valueOf("2023-01-01 01:00:20"))))
        ),

        // < data batch >
        // global watermark (0, 0)
        // op1 (join)
        // -- IW (0, 0)
        // -- OW 0
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // -- result
        // ("1", "2023-01-01 01:00:10", "1", "2023-01-01 01:00:20")
        // op2 (aggregation)
        // -- IW (0, 0)
        // -- OW 0
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None

        // -- watermark calculation
        // watermark in left input: 2023-01-01 01:00:30
        // watermark in right input: 2023-01-01 01:00:20
        // origin watermark: 2023-01-01 01:00:20

        // < no-data batch >
        // global watermark (0, 2023-01-01 01:00:20)
        // op1 (join)
        // -- IW (0, 2023-01-01 01:00:20)
        // -- OW 2023-01-01 00:00:19.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (0, 2023-01-01 00:00:19.999999)
        // -- OW 2023-01-01 00:00:19.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None
        CheckAnswer(),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents === Some(0))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:00:20").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents === Some(0))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 00:00:20").getTime - 1))
        },

        MultiAddData(
          (input1, Seq(("5", Timestamp.valueOf("2023-01-01 01:15:00")))),
          (input2, Seq(("6", Timestamp.valueOf("2023-01-01 01:15:00"))))
        ),

        // < data batch >
        // global watermark (2023-01-01 01:00:20, 2023-01-01 01:00:20)
        // op1 (join)
        // -- IW (2023-01-01 01:00:20, 2023-01-01 01:00:20)
        // -- OW 2023-01-01 00:00:19.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // ("5", "2023-01-01 01:15:00", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // ("6", "2023-01-01 01:15:00")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (2023-01-01 00:00:19.999999, 2023-01-01 00:00:19.999999)
        // -- OW 2023-01-01 00:00:19.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None

        // -- watermark calculation
        // watermark in left input: 2023-01-01 01:15:00
        // watermark in right input: 2023-01-01 01:15:00
        // origin watermark: 2023-01-01 01:15:00

        // < no-data batch >
        // global watermark (2023-01-01 01:00:20, 2023-01-01 01:15:00)
        // op1 (join)
        // -- IW (2023-01-01 01:00:20, 2023-01-01 01:15:00)
        // -- OW 2023-01-01 00:14:59.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // ("5", "2023-01-01 01:15:00", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // ("6", "2023-01-01 01:15:00")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (2023-01-01 00:00:19.999999, 2023-01-01 00:14:59.999999)
        // -- OW 2023-01-01 00:14:59.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None
        CheckAnswer(),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 01:00:20").getTime))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:15:00").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 00:00:20").getTime - 1))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 00:15:00").getTime - 1))
        },

        MultiAddData(
          (input1, Seq(
            ("5", Timestamp.valueOf("2023-01-01 02:16:00")))),
          (input2, Seq(
            ("6", Timestamp.valueOf("2023-01-01 02:16:00"))))
        ),

        // < data batch >
        // global watermark (2023-01-01 01:15:00, 2023-01-01 01:15:00)
        // op1 (join)
        // -- IW (2023-01-01 01:15:00, 2023-01-01 01:15:00)
        // -- OW 2023-01-01 00:14:59.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // ("5", "2023-01-01 01:15:00", matched=false)
        // ("5", "2023-01-01 02:16:00", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // ("6", "2023-01-01 01:15:00")
        // ("6", "2023-01-01 02:16:00")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (2023-01-01 00:14:59.999999, 2023-01-01 00:14:59.999999)
        // -- OW 2023-01-01 00:14:59.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None

        // -- watermark calculation
        // watermark in left input: 2023-01-01 02:16:00
        // watermark in right input: 2023-01-01 02:16:00
        // origin watermark: 2023-01-01 02:16:00

        // < no-data batch >
        // global watermark (2023-01-01 01:15:00, 2023-01-01 02:16:00)
        // op1 (join)
        // -- IW (2023-01-01 01:15:00, 2023-01-01 02:16:00)
        // -- OW 2023-01-01 01:15:59.999999
        // -- left state
        // ("5", "2023-01-01 02:16:00", matched=false)
        // -- right state
        // ("6", "2023-01-01 02:16:00")
        // -- result
        // ("1", "2023-01-01 01:00:30", null, null)
        // ("5", "2023-01-01 01:15:00", null, null)
        // op2 (aggregation)
        // -- IW (2023-01-01 00:14:59.999999, 2023-01-01 01:15:59.999999)
        // -- OW 2023-01-01 01:15:59.999999
        // -- state row
        // ("2023-01-01 01:10:00", "2023-01-01 01:20:00", 1)
        // -- result
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 2)
        CheckAnswer(
          ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 2)
        ),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 01:15:00").getTime))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 02:16:00").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 00:15:00").getTime - 1))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:16:00").getTime - 1))
        }
      )
    }
  }

  // FIXME: construct a simpler test which may pass without phase 2, but strictly check for the
  //  value of watermark per operator. set watermark delay threshold differently.

  test("FIXME: stream-stream time interval left outer join -> aggregation, append mode") {
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

      // < data batch >
      // global watermark (0, 0)
      // op1 (join)
      // -- IW (0, 0)
      // -- OW 0
      // -- result
      // (Timestamp.valueOf("2020-01-02 00:00:00"), "abc", "joined with A",
      //  Timestamp.valueOf("2020-01-02 00:00:10"), "abc", "A")
      // (Timestamp.valueOf("2020-01-02 01:00:00"), "abc", "joined with B",
      //  Timestamp.valueOf("2020-01-02 00:59:59"), "abc", "B")
      // op2 (aggregation)
      // -- IW (0, 0)
      // -- OW 0
      // -- state row
      // (Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:30"), 1)
      // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)

      // -- watermark calculation
      // watermark in left input: 2020-01-02 00:58:00
      // watermark in right input: 2020-01-02 01:56:00
      // origin watermark: 2020-01-02 00:58:00

      // < no-data batch >
      // global watermark (0, 2020-01-02 00:58:00)
      // op1 (join)
      // -- IW (0, 2020-01-02 00:58:00)
      // -- OW 2020-01-02 00:57:19.999999
      // -- result (eviction)
      // (Timestamp.valueOf("2020-01-01 00:00:00"), "abc", "has no join partner",
      //  null, null, null)
      // op2 (aggregation)
      // -- IW (0, 2020-01-02 00:57:19.999999)
      // -- OW 2020-01-02 00:57:19.999999
      // -- state row
      // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
      // -- result
      // (Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:30"), 1)
      // (Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:30"), 1)

      CheckNewAnswer(
        (Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:30"), 1),
        (Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:30"), 1)
      ),

      Execute { query =>
        val lastExecution = query.lastExecution
        val joinOperator = lastExecution.executedPlan.collect {
          case j: StreamingSymmetricHashJoinExec => j
        }.head
        val aggSaveOperator = lastExecution.executedPlan.collect {
          case j: StateStoreSaveExec => j
        }.head

        assert(joinOperator.eventTimeWatermarkForLateEvents === Some(0))
        assert(joinOperator.eventTimeWatermarkForEviction ===
          Some(Timestamp.valueOf("2020-01-02 00:58:00").getTime))

        assert(aggSaveOperator.eventTimeWatermarkForLateEvents === Some(0))
        assert(aggSaveOperator.eventTimeWatermarkForEviction ===
          Some(Timestamp.valueOf("2020-01-02 00:57:20").getTime - 1))
      },

      MultiAddData(
        (input1, Seq((Timestamp.valueOf("2020-01-05 00:00:00"), "abc", "joined with D"))),
        (input2, Seq((Timestamp.valueOf("2020-01-05 00:00:10"), "abc", "D")))
      ),

      // < data batch >
      // global watermark (2020-01-02 00:58:00, 2020-01-02 00:58:00)
      // op1 (join)
      // -- IW (2020-01-02 00:58:00, 2020-01-02 00:58:00)
      // -- OW 2020-01-02 00:57:19.999999
      // -- result
      // (Timestamp.valueOf("2020-01-05 00:00:00"), "abc", "joined with D",
      //  Timestamp.valueOf("2020-01-05 00:00:10"), "abc", "D")
      // op2 (aggregation)
      // -- IW (2020-01-02 00:57:19.999999, 2020-01-02 00:57:19.999999)
      // -- OW 2020-01-02 00:57:19.999999
      // -- state row
      // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
      // (Timestamp.valueOf("2020-01-05 00:00:00"), Timestamp.valueOf("2020-01-05 01:00:30"), 1)

      // - watermark calculation
      // watermark in left input: 2020-01-04 23:58:00
      // watermark in right input: 2020-01-04 23:56:10
      // origin watermark: 2020-01-04 23:56:10

      // < no-data batch >
      // global watermark (2020-01-02 00:58:00, 2020-01-04 23:56:10)
      // op1 (join)
      // -- IW (2020-01-02 00:58:00, 2020-01-04 23:56:10)
      // -- OW 2020-01-04 23:55:29.999999
      // -- result
      // None
      // op2 (aggregation)
      // -- IW (2020-01-02 00:57:19.999999, 2020-01-04 23:55:29.999999)
      // -- OW 2020-01-04 23:55:29.999999
      // -- result
      // (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
      // -- state row
      // (Timestamp.valueOf("2020-01-05 00:00:00"), Timestamp.valueOf("2020-01-05 01:00:30"), 1)

      CheckNewAnswer(
        (Timestamp.valueOf("2020-01-02 01:00:00"), Timestamp.valueOf("2020-01-02 01:00:30"), 1)
      ),

      Execute { query =>
        val lastExecution = query.lastExecution
        val joinOperator = lastExecution.executedPlan.collect {
          case j: StreamingSymmetricHashJoinExec => j
        }.head
        val aggSaveOperator = lastExecution.executedPlan.collect {
          case j: StateStoreSaveExec => j
        }.head

        assert(joinOperator.eventTimeWatermarkForLateEvents ===
          Some(Timestamp.valueOf("2020-01-02 00:58:00").getTime))
        assert(joinOperator.eventTimeWatermarkForEviction ===
          Some(Timestamp.valueOf("2020-01-04 23:56:10").getTime))

        assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
          Some(Timestamp.valueOf("2020-01-02 00:57:20").getTime - 1))
        assert(aggSaveOperator.eventTimeWatermarkForEviction ===
          Some(Timestamp.valueOf("2020-01-04 23:55:30").getTime - 1))
      }
    )
  }

  private def assertNumStateRows(numTotalRows: Seq[Long]): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.lastOption.get
    val stateOperators = progressWithData.stateOperators
    assert(stateOperators.size === numTotalRows.size)
    assert(stateOperators.map(_.numRowsTotal).toSeq === numTotalRows)
    true
  }

  private def assertNumRowsDroppedByWatermark(
      numRowsDroppedByWatermark: Seq[Long]): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.filterNot { p =>
      // filter out batches which are falling into one of types:
      // 1) doesn't execute the batch run
      // 2) empty input batch
      p.numInputRows == 0
    }.lastOption.get
    val stateOperators = progressWithData.stateOperators
    assert(stateOperators.size === numRowsDroppedByWatermark.size)
    assert(stateOperators.map(_.numRowsDroppedByWatermark).toSeq === numRowsDroppedByWatermark)
    true
  }
}

object MultiStatefulOperatorsSuite {
  final case class RecordClass(id: Integer, ts: Timestamp)
}

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

import java.{util => ju}
import java.text.SimpleDateFormat

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, rowtime, timestamp_seconds, window}

class EventTimeWatermarkWithRowtimeFuncSuite
  extends StreamTest
  with BeforeAndAfter
  with Matchers
  with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("event time and watermark metrics with rowtime(), SQL string") {
    // All event time metrics where watermarking is set
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
    df.createOrReplaceTempView("stream_src")
    val aggWithWatermark = spark.sql(
      """
        |SELECT CAST(window.start AS LONG), CAST(count AS LONG)
        |FROM
        |(
        |    SELECT window, count(*) AS count
        |    FROM
        |    (
        |        SELECT *, rowtime(timestamp_seconds(value), '10 seconds') AS eventTime
        |        FROM stream_src
        |    )
        |    GROUP BY window(eventTime, '5 seconds')
        |)
        |""".stripMargin)
    /*
    val aggWithWatermark = inputData.toDF()
      .selectExpr("*", "rowtime(timestamp_seconds(value), '10 seconds') AS eventTime")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])
     */

    testStream(aggWithWatermark)(
      AddData(inputData, 15),
      CheckAnswer(),
      assertEventStats(min = 15, max = 15, avg = 15, wtrmark = 0),
      AddData(inputData, 10, 12, 14),
      CheckAnswer(),
      assertEventStats(min = 10, max = 14, avg = 12, wtrmark = 5),
      AddData(inputData, 25),
      CheckAnswer((10, 3)),
      assertEventStats(min = 25, max = 25, avg = 25, wtrmark = 5)
    )
  }

  test("event time and watermark metrics with rowtime(), function API") {
    // All event time metrics where watermarking is set
    val inputData = MemoryStream[Int]
    val aggWithWatermark = inputData.toDF()
      .select($"*", rowtime(timestamp_seconds($"value"), "10 seconds").as("eventTime"))
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(aggWithWatermark)(
      AddData(inputData, 15),
      CheckAnswer(),
      assertEventStats(min = 15, max = 15, avg = 15, wtrmark = 0),
      AddData(inputData, 10, 12, 14),
      CheckAnswer(),
      assertEventStats(min = 10, max = 14, avg = 12, wtrmark = 5),
      AddData(inputData, 25),
      CheckAnswer((10, 3)),
      assertEventStats(min = 25, max = 25, avg = 25, wtrmark = 5)
    )
  }

  /** Assert event stats generated on that last batch with data in it */
  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    Execute("AssertEventStats") { q =>
      body(q.recentProgress.filter(_.numInputRows > 0).lastOption.get.eventTime)
    }
  }

  /** Assert event stats generated on that last batch with data in it */
  private def assertEventStats(min: Long, max: Long, avg: Double, wtrmark: Long): AssertOnQuery = {
    assertEventStats { e =>
      assert(e.get("min") === formatTimestamp(min), s"min value mismatch")
      assert(e.get("max") === formatTimestamp(max), s"max value mismatch")
      assert(e.get("avg") === formatTimestamp(avg.toLong), s"avg value mismatch")
      assert(e.get("watermark") === formatTimestamp(wtrmark), s"watermark value mismatch")
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone(UTC))

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }
}

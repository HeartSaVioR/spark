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
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.scalatest.{Assertions, BeforeAndAfterAll}
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.types.StringType


class StreamingSessionWindowBaiduPatchSuite extends StateStoreMetricsTest
  with BeforeAndAfterAll with Assertions {

  import testImplicits._
  override val streamingTimeout = 30.seconds

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("session window with single key, complete mode") {
    val inputData = MemoryStream[(String, String, Int)]
    val aggregated =
      inputData.toDS().toDF("time", "key", "value")
        .groupBy(session($"time", "10 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .orderBy($"session.start".asc)
        .select($"session.start".cast(StringType),
          $"session.end".cast(StringType), $"key", $"res")

    aggregated.explain(true)

    testStream(aggregated, Complete)(
      // batch 1
      AddData(inputData, ("2018-08-22 19:39:27", "a", 4),
        ("2018-08-22 19:39:34", "a", 1),
        ("2018-08-22 19:39:56", "a", 3),
        ("2018-08-22 19:39:56", "b", 2)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 5),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 3),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 2)),
      // batch 2, add the same data as batch 1
      AddData(inputData, ("2018-08-22 19:39:27", "a", 4),
        ("2018-08-22 19:39:34", "a", 1),
        ("2018-08-22 19:39:56", "a", 3),
        ("2018-08-22 19:39:56", "b", 2)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 10),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 4)),
      // batch 3, extends one existed window, and add a new window of key 'a'
      AddData(inputData, ("2018-08-22 19:39:35", "a", 4),
        ("2018-08-22 19:40:10", "a", 1),
        ("2018-08-22 19:40:15", "a", 3)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:45", "a", 14),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:40:10", "2018-08-22 19:40:25", "a", 4),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 4)),
      // batch 4, extends one existed window of key 'b'
      AddData(inputData, ("2018-08-22 19:40:06", "b", 4)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:45", "a", 14),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:40:10", "2018-08-22 19:40:25", "a", 4),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:16", "b", 8)),
      StopStream
    )
  }

  test("session window with watermark and single key in append mode") {
    val inputData = MemoryStream[(String, String, Int)]
    val aggregated =
      inputData.toDS().toDF("time", "key", "value")
        .selectExpr("CAST(time as TIMESTAMP)", "key", "value")
        .withWatermark("time", "5 seconds")
        .groupBy(session($"time", "10 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .select($"session.start".cast(StringType),
          $"session.end".cast(StringType), $"key", $"res")

    aggregated.explain(true)
    val acc = new EventTimeStatsAccum()
    testStream(aggregated, Append)(
      AddData(inputData,
        ("2018-08-22 19:39:27", "a", 4),
        ("2018-08-22 19:39:34", "a", 1),
        ("2018-08-22 19:39:56", "a", 3),
        ("2018-08-22 19:39:56", "b", 2)),

      // Current sessions (before eviction):
      // (19:39:27, 19:39:44, a, 5)
      // (19:39:56, 19:40:06, a, 3)
      // (19:39:56, 19:40:06, b, 2)
      //
      // Watermark
      // '2018-08-22 19:39:56' - 5 seconds => 2018-08-22 19:39:51
      //
      // Evicted sessions
      // (19:39:27, 19:39:44, a, 5)
      //
      // Remained sessions (after eviction):
      // (19:39:56, 19:40:06, a, 3)
      // (19:39:56, 19:40:06, b, 2)

      CheckNewAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 5)),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:39:51"))
      },

      AddData(inputData, ("2018-08-22 19:39:33", "a", 1)),

      // row will be dropped since it is late then watermark
      //
      // Current sessions (before eviction):
      // (19:39:56, 19:40:06, a, 3)
      // (19:39:56, 19:40:06, b, 2)
      //
      // Watermark
      // 2018-08-22 19:39:51 ('2018-08-22 19:39:56' - 5 seconds)
      //
      // Remained sessions (after eviction):
      // (19:39:56, 19:40:06, a, 3)
      // (19:39:56, 19:40:06, b, 2)

      CheckNewAnswer(),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:39:51"))
      },

      AddData(inputData, ("2018-08-22 19:39:27", "a", 4), ("2018-08-22 19:39:34", "a", 1)),

      // row will be dropped since it is late then watermark
      //
      // Current sessions (before eviction):
      // (19:39:56, 19:40:06, a, 3)
      // (19:39:56, 19:40:06, b, 2)
      //
      // Watermark
      // 2018-08-22 19:39:51
      //
      // Remained sessions (after eviction):
      // (19:39:56, 19:40:06, a, 3)
      // (19:39:56, 19:40:06, b, 2)

      CheckNewAnswer(),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:39:51"))
      },

      AddData(inputData, ("2018-08-22 19:40:05", "a", 2), ("2018-08-22 19:40:21", "a", 2)),

      // Current sessions (before eviction):
      // (19:39:56, 19:40:15, a, 5)
      // (19:39:56, 19:40:06, b, 2)
      // (19:40:21, 19:40:31, a, 2)
      //
      // Watermark
      // 2018-08-22 19:40:16 ('2018-08-22 19:40:21' - 5 seconds)
      //
      // Evicted sessions
      // (19:39:56, 19:40:15, a, 5)
      // (19:39:56, 19:40:06, b, 2)
      //
      // Remained sessions (after eviction):
      // (19:40:21, 19:40:31, a, 2)

      CheckNewAnswer(
        ("2018-08-22 19:39:56", "2018-08-22 19:40:15", "a", 5),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 2)
      ),
      assertNumStateRows(1),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:16"))
      },

      AddData(inputData, ("2018-08-22 19:40:22", "b", 2)),

      // Current sessions (before eviction):
      // (19:40:21, 19:40:31, a, 2)
      // (19:40:22, 19:40:32, b, 2)
      //
      // Watermark
      // 2018-08-22 19:40:17 ('2018-08-22 19:40:22' - 5 seconds)
      //
      // Remained sessions (after eviction):
      // (19:40:21, 19:40:31, a, 2)
      // (19:40:22, 19:40:32, b, 2)

      CheckNewAnswer(),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:17"))
      },

      AddData(inputData, ("2018-08-22 19:40:40", "c", 1)),

      // Current sessions (before eviction):
      // (19:40:21, 19:40:31, a, 2)
      // (19:40:22, 19:40:32, b, 2)
      // (19:40:40, 19:40:50, c, 1)
      //
      // Watermark
      // 2018-08-22 19:40:35 ('2018-08-22 19:40:40' - 5 seconds)
      //
      // Evicted sessions
      // (19:40:21, 19:40:31, a, 2)
      // (19:40:22, 19:40:32, b, 2)
      //
      // Remained sessions (after eviction):
      // (19:40:40, 19:40:50, c, 1)

      CheckNewAnswer(
        ("2018-08-22 19:40:21", "2018-08-22 19:40:31", "a", 2),
        ("2018-08-22 19:40:22", "2018-08-22 19:40:32", "b", 2)
      ),
      assertNumStateRows(1),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:35"))
      },

      AddData(inputData, ("2018-08-22 19:40:41", "c", 1)),

      // Current sessions (before eviction):
      // (19:40:40, 19:40:50, c, 1)
      //
      // Watermark
      // 2018-08-22 19:40:36 ('2018-08-22 19:40:41' - 5 seconds)
      //
      // Remained sessions (after eviction):
      // (19:40:40, 19:40:51, c, 2)

      CheckNewAnswer(),
      assertNumStateRows(1),
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:36"))
      },
      StopStream
    )
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.last
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }

  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    AssertOnQuery { q =>
      body(q.recentProgress.filter(_.eventTime != null).lastOption.get.eventTime)
      true
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone("UTC"))

  private def formatTimestamp(str: String): String = {
    val ts = Timestamp.valueOf(str).getTime / 1000 * 1000
    timestampFormat.format(new ju.Date(ts))
  }

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }
}
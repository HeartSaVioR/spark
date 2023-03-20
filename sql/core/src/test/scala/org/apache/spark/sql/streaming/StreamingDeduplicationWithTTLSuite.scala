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

import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds

class StreamingDeduplicationWithTTLSuite extends StateStoreMetricsTest {

  import testImplicits._

  test("deduplicate without event time column") {
    def testAndVerify(df: Dataset[_]): Unit = {
      val exc = intercept[AnalysisException] {
        df.writeStream.format("noop").start()
      }

      assert(exc.getMessage.contains("dropDuplicatesWithTTL is not supported"))
      assert(exc.getMessage.contains("streaming DataFrames/DataSets without watermark"))
    }

    val inputData = MemoryStream[String]
    val result = inputData.toDS().dropDuplicatesWithTTL("1 hour")
    testAndVerify(result)

    val result2 = inputData.toDS().withColumn("newcol", $"value")
      .dropDuplicatesWithTTL(Seq("newcol"), "1 hour")
    testAndVerify(result2)
  }

  test("deduplicate with all columns with event time column") {
    val inputData = MemoryStream[Int]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithTTL("2 second")
      .select($"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckAnswer(10 to 15: _*),
      // expired times set to 12 to 17
      assertNumStateRows(total = 6, updated = 6),

      AddData(inputData, (13 to 17): _*),
      // 13 to 15 are duplicated.
      CheckNewAnswer(16, 17),
      // See updated - we don't update the state with the new expired time which is
      // equals or smaller.
      assertNumStateRows(total = 8, updated = 2),

      // Advance watermark to 15 secs, no-data-batch drops state rows having expired time <= 15
      AddData(inputData, 25),
      CheckNewAnswer(25),
      assertNumStateRows(total = 5, updated = 1),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(total = 5, updated = 0, droppedByWatermark = 1),

      // Advance watermark to 35 seconds, no-data-batch drops state rows having expired time <= 35
      AddData(inputData, 45),
      CheckNewAnswer(45),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("deduplicate with some columns with event time column") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"_2"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithTTL(Seq("_1"), "2 seconds")
      .select($"_1", $"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      AddData(inputData, "a" -> 11),
      CheckNewAnswer("a" -> 11),
      // expired time is set to 13
      assertNumStateRows(total = 1, updated = 1),

      AddData(inputData, "a" -> 5),
      // deduplicated. note that the range for deduplication is (-inf, 13)
      // expired time is not updated as 13 > 5 + 2
      CheckNewAnswer(),
      assertNumStateRows(total = 1, updated = 0),

      AddData(inputData, "a" -> 12),
      // deduplicated, expired time is updated to 14
      CheckNewAnswer(),
      assertNumStateRows(total = 1, updated = 1),

      // Advance watermark to 20 secs, no-data-batch drops state rows having expired time <= 20
      AddData(inputData, "b" -> 30),
      // expired time is set to 20
      CheckNewAnswer("b" -> 30),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("SPARK-39650: duplicate with specific keys should allow input to change schema") {
    withTempDir { checkpoint =>
      val dedupeInputData = MemoryStream[(String, Int)]
      val dedupe = dedupeInputData.toDS()
        .withColumn("eventTime", timestamp_seconds($"_2"))
        .withWatermark("eventTime", "0 second")
        .dropDuplicatesWithTTL(Seq("_1"), "10 seconds")
        .select($"_1", $"eventTime".cast("long").as[Long])

      testStream(dedupe, Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),

        AddData(dedupeInputData, "a" -> 1),
        CheckNewAnswer("a" -> 1),

        AddData(dedupeInputData, "a" -> 2, "b" -> 3),
        CheckNewAnswer("b" -> 3)
      )

      val dedupeInputData2 = MemoryStream[(String, Int, String)]
      val dedupe2 = dedupeInputData2.toDS()
        .withColumn("eventTime", timestamp_seconds($"_2"))
        .withWatermark("eventTime", "0 second")
        .dropDuplicatesWithTTL(Seq("_1"), "10 seconds")
        .select($"_1", $"eventTime".cast("long").as[Long], $"_3")

      // initialize new memory stream with previously executed batches
      dedupeInputData2.addData(("a", 1, "dummy"))
      dedupeInputData2.addData(Seq(("a", 2, "dummy"), ("b", 3, "dummy")))

      testStream(dedupe2, Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),

        AddData(dedupeInputData2, ("a", 5, "a"), ("b", 2, "b"), ("c", 9, "c")),
        CheckNewAnswer(("c", 9, "c"))
      )
    }
  }

  // FIXME: test running dropDuplicatesWithTTL with batch DataFrame
}

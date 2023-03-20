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

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds

class StreamingDeduplicationWithTTLSuite extends StateStoreMetricsTest {

  import testImplicits._

  // FIXME: This must fail with UnsupportedOperationChecker
  test("deduplicate with all columns without event time column") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS().dropDuplicatesWithTTL("1 hour")

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch("a"),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a"),
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0),
      AddData(inputData, "b"),
      CheckLastBatch("b"),
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("deduplicate with all columns with event time column") {
    // FIXME: better tests...
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

      AddData(inputData, 25), // Advance watermark to 15 secs, no-data-batch drops rows <= 15
      CheckNewAnswer(25),
      assertNumStateRows(total = 3, updated = 1),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(total = 3, updated = 0, droppedByWatermark = 1),

      AddData(inputData, 45), // Advance watermark to 35 seconds, no-data-batch drops rows <= 35
      CheckNewAnswer(45),
      assertNumStateRows(total = 1, updated = 1)
    )
  }
}

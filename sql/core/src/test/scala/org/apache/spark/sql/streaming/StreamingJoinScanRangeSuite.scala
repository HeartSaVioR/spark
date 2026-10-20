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

import java.time.Duration

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Between, Cast, EvalMode, Literal, Predicate, TimestampAddInterval}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.getJoinScanRange
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class StreamingJoinScanRangeSuite extends SparkFunSuite {
  private val leftTime = AttributeReference("leftTime", TimestampType)()
  private val expiry = AttributeReference("expiry", TimestampType)()
  private val rightTime = AttributeReference("rightTime", TimestampType)()

  test("scan bounds use the incoming expiry and constrain only the stored index column") {
    val condition = Some(rightTime >= leftTime && rightTime <= expiry)
    val scanRight = getJoinScanRange(
      Seq(leftTime, expiry), rightTime, condition).get
    val scanLeft = getJoinScanRange(
      Seq(rightTime), leftTime, condition).get

    assert(scanRight(InternalRow(100000000L, 110000000L)) == (100000000L, 110000000L))
    assert(scanRight(InternalRow(100000000L, 120000000L)) == (100000000L, 120000000L))
    assert(scanLeft(InternalRow(105000000L)) == (Long.MinValue, 105000000L))
  }

  test("strict and inclusive comparisons yield conservative one-sided bounds") {
    val lowerBounds = Seq(rightTime >= leftTime, rightTime > leftTime,
      leftTime <= rightTime, leftTime < rightTime)
    val upperBounds = Seq(rightTime <= leftTime, rightTime < leftTime,
      leftTime >= rightTime, leftTime > rightTime)
    for (time <- Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue)) {
      lowerBounds.foreach { condition =>
        val scan = getJoinScanRange(Seq(leftTime), rightTime, Some(condition)).get
        assert(scan(InternalRow(time)) == (time, Long.MaxValue))
      }
      upperBounds.foreach { condition =>
        val scan = getJoinScanRange(Seq(leftTime), rightTime, Some(condition)).get
        assert(scan(InternalRow(time)) == (Long.MinValue, time))
      }
    }
  }

  test("BETWEEN yields row-dependent bounds") {
    val scan = getJoinScanRange(
      Seq(leftTime, expiry), rightTime, Some(Between(rightTime, leftTime, expiry))).get
    assert(scan(InternalRow(100000001L, 110000002L)) == (100000001L, 110000002L))
  }

  test("conjunctive bounds use the tightest lower and upper endpoints") {
    val condition = rightTime >= leftTime && rightTime <= expiry &&
      rightTime >= Literal(5L, TimestampType) && rightTime <= Literal(15L, TimestampType)
    val scan = getJoinScanRange(Seq(leftTime, expiry), rightTime, Some(condition)).get
    assert(scan(InternalRow(0L, 20L)) == (5L, 15L))
    assert(scan(InternalRow(10L, 12L)) == (10L, 12L))
    assert(scan(InternalRow(20L, 0L)) == (20L, 0L))
  }

  test("null bounds leave an endpoint unbounded") {
    val condition = rightTime >= leftTime && rightTime <= expiry
    val scan = getJoinScanRange(Seq(leftTime, expiry), rightTime, Some(condition)).get
    assert(scan(InternalRow(null, 110000000L)) == (Long.MinValue, 110000000L))
    assert(scan(InternalRow(100000000L, null)) == (100000000L, Long.MaxValue))
    assert(scan(InternalRow(null, null)) == (Long.MinValue, Long.MaxValue))
  }

  test("failed bound evaluation leaves the full predicate responsible for errors") {
    val text = AttributeReference("text", StringType)()
    val condition = rightTime <= Cast(text, TimestampType, Some("UTC"), EvalMode.ANSI)
    val scan = getJoinScanRange(Seq(text), rightTime, Some(condition)).get
    assert(scan(InternalRow(UTF8String.fromString("invalid"))) ==
      (Long.MinValue, Long.MaxValue))
  }

  test("interval expressions on the incoming row retain microsecond precision") {
    val end = TimestampAddInterval(leftTime, Literal(Duration.ofSeconds(10)), Some("UTC"))
    val condition = rightTime >= leftTime && rightTime <= end
    val scan = getJoinScanRange(Seq(leftTime), rightTime, Some(condition)).get
    assert(scan(InternalRow(100000001L)) == (100000001L, 110000001L))
  }

  test("fixed intervals retain bounded scans in both directions") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 60000).build()
    val left = AttributeReference("leftTime", TimestampType, metadata = metadata)()
    val right = AttributeReference("rightTime", TimestampType, metadata = metadata)()
    def shift(time: AttributeReference, seconds: Long): TimestampAddInterval =
      TimestampAddInterval(time, Literal(Duration.ofSeconds(seconds)), Some("UTC"))

    val conditions = Seq(
      (right >= left && right <= shift(left, 10), (0L, 10L), (-10L, 0L)),
      (left >= shift(right, -10) && left <= right, (0L, 10L), (-10L, 0L)),
      (left >= shift(right, -5) && left <= shift(right, 5), (-5L, 5L), (-5L, 5L)),
      (shift(left, 2) >= shift(right, -3) && shift(left, 2) <= shift(right, 7),
        (-5L, 5L), (-5L, 5L)))

    conditions.foreach { case (condition, rightOffsets, leftOffsets) =>
      Seq((Seq(left), right, rightOffsets), (Seq(right), left, leftOffsets)).foreach {
        case (input, index, (lowerSeconds, upperSeconds)) =>
          val time = 100000001L
          val (lower, upper) = getJoinScanRange(
            input, index, Some(condition), useConstantOffsets = true)
            .getOrElse(fail(s"No scan bounds for ${condition.sql}, index ${index.name}"))(
              InternalRow(time))
          val expectedLower = time + lowerSeconds * 1000000L
          val expectedUpper = time + upperSeconds * 1000000L
          // The existing arithmetic extractor widens each endpoint by at most one millisecond.
          assert(lower >= expectedLower - 1000L && lower <= expectedLower,
            s"Lost lower bound for ${condition.sql}, index ${index.name}: $lower")
          assert(upper >= expectedUpper && upper <= expectedUpper + 1000L,
            s"Lost upper bound for ${condition.sql}, index ${index.name}: $upper")
      }
    }
  }

  test("unsupported expressions cannot narrow the indexed timestamp range") {
    val shiftedIndex = TimestampAddInterval(
      rightTime, Literal(Duration.ofSeconds(10)), Some("UTC"))
    val unsupported = Seq(
      rightTime >= leftTime || rightTime <= expiry,
      Cast(rightTime, LongType) >= Cast(leftTime, LongType),
      shiftedIndex >= leftTime,
      rightTime <= Cast(rand(9).expr, TimestampType),
      expiry <= leftTime)
    unsupported.foreach { condition =>
      assert(getJoinScanRange(Seq(leftTime), rightTime, Some(condition)).isEmpty)
    }
    assert(getJoinScanRange(Seq(leftTime), rightTime, None).isEmpty)
    val scan = getJoinScanRange(Seq(leftTime), rightTime,
      Some(rightTime >= leftTime && shiftedIndex <= leftTime)).get
    assert(scan(InternalRow(100000000L)) == (100000000L, Long.MaxValue))
  }

  test("window-struct indexes are not treated as scalar timestamps") {
    val window = AttributeReference("window",
      new StructType().add("start", TimestampType).add("end", TimestampType))()
    assert(getJoinScanRange(Seq(leftTime), window, Some(rightTime >= leftTime)).isEmpty)
  }

  test("every matching pair remains inside the derived scan bounds") {
    val conditions = Seq(
      rightTime >= leftTime && rightTime <= expiry,
      rightTime > leftTime && rightTime < expiry,
      rightTime >= leftTime && rightTime >= expiry,
      rightTime <= leftTime && rightTime <= expiry)
    val times = Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue)
    conditions.foreach { condition =>
      val predicate = Predicate.create(condition, Seq(leftTime, expiry, rightTime))
      val scanRight = getJoinScanRange(Seq(leftTime, expiry), rightTime, Some(condition))
      val scanLeft = getJoinScanRange(Seq(rightTime), leftTime, Some(condition))
      for (start <- times; end <- times; time <- times) {
        if (predicate.eval(InternalRow(start, end, time))) {
          val (rightMin, rightMax) = scanRight.get(InternalRow(start, end))
          val (leftMin, leftMax) = scanLeft.get(InternalRow(time))
          assert(time >= rightMin && time <= rightMax)
          assert(start >= leftMin && start <= leftMax)
        }
      }
    }
  }
}

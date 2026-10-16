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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Filter, LeafNode, Statistics}
import org.apache.spark.sql.types.{DoubleType, IntegerType, MetadataBuilder, TimestampType}

class StreamingJoinHelperSuite extends AnalysisTest {

  test("SPARK-58904: state timestamp selection is stable as the watermark advances") {
    val first = AttributeReference("first", TimestampType)()
    val second = AttributeReference("second", TimestampType)()
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    val condition = Some(first > right - 0.0001 && second > right + 0.0001)
    Seq(0L, 1L, 10000L).foreach { watermark =>
      val result = StreamingJoinHelper.getStateValueWatermarkWithAttribute(
        AttributeSet(Seq(first, second)), AttributeSet(Seq(right)), condition, Some(watermark))
      assert(result.exists(_._1.semanticEquals(first)))
    }
  }

  test("SPARK-58904: prefer the state side's event time in either conjunct order") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val eventTime = AttributeReference("eventTime", TimestampType, metadata = metadata)()
    val other = AttributeReference("other", TimestampType)()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    Seq(other > right && eventTime > right,
      eventTime > right && other > right).foreach { condition =>
      val result = StreamingJoinHelper.getStateValueWatermarkWithAttribute(
        AttributeSet(Seq(eventTime, other)), AttributeSet(Seq(right)), Some(condition),
        Some(20000L))
      assert(result.exists { case (attr, watermark) =>
        attr.semanticEquals(eventTime) && watermark == 20000L
      })
    }
  }

  test("SPARK-58904: legacy range bounds retain the pre-fix minimum") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val eventTime = AttributeReference("eventTime", TimestampType, metadata = metadata)()
    val other = AttributeReference("other", TimestampType)()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    val condition = Some(eventTime > right + 10 && other > right - 10)
    val stateAttrs = AttributeSet(Seq(eventTime, other))
    val otherAttrs = AttributeSet(Seq(right))
    val legacy = StreamingJoinHelper.getLegacyStateValueWatermark(
      stateAttrs, otherAttrs, condition, Some(0L))
    val corrected = StreamingJoinHelper.getStateValueWatermark(
      stateAttrs, otherAttrs, condition, Some(0L))
    assert(legacy.isDefined && corrected.isDefined && legacy.get < corrected.get)
  }

  test("SPARK-58904: legacy state bounds keep the prior extraction rules") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val stateValue = AttributeReference("stateValue", IntegerType)()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    val condition = Some(stateValue > right)
    assert(StreamingJoinHelper.getStateValueWatermark(
      AttributeSet(Seq(stateValue)), AttributeSet(Seq(right)), condition, Some(20000L)).isEmpty)
    assert(StreamingJoinHelper.getLegacyStateValueWatermark(
      AttributeSet(Seq(stateValue)), AttributeSet(Seq(right)), condition,
      Some(20000L)).contains(20000L))
  }

  test("SPARK-58904: legacy extraction preserves deduplication of unoptimized terms") {
    val left = AttributeReference("left", TimestampType)()
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val right = AttributeReference("right", TimestampType, metadata = metadata)()
    Seq((left > right + 2 + 2, 12000L), (left > right - 2 - 2, 8000L)).foreach {
      case (condition, expected) =>
        assert(StreamingJoinHelper.getLegacyStateValueWatermark(
          AttributeSet(Seq(left)), AttributeSet(Seq(right)), Some(condition), Some(10000L))
          .contains(expected))
    }
  }

  test("SPARK-58904: legacy extraction deduplicates terms after watermark substitution") {
    val stateValue = AttributeReference("stateValue", DoubleType)()
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val right = AttributeReference("right", DoubleType, metadata = metadata)()
    // The old extractor accepted numeric attributes carrying watermark metadata. At this
    // watermark, substituting right makes its term semantically equal to the literal term.
    val result = StreamingJoinHelper.getLegacyStateValueWatermark(
      AttributeSet(Seq(stateValue)), AttributeSet(Seq(right)),
      Some(stateValue > right + 1000.0), Some(1000000L))
    assert(result.contains(1000000L))
  }

  gridTest("extract watermark from time condition")(Seq(false, true)) { legacy =>
    val attributesToFindConstraintFor = Seq(
      AttributeReference("leftTime", TimestampType)(),
      AttributeReference("leftOther", IntegerType)())
    val metadataWithWatermark = new MetadataBuilder()
      .putLong(EventTimeWatermark.delayKey, 1000)
      .build()
    val attributesWithWatermark = Seq(
      AttributeReference("rightTime", TimestampType, metadata = metadataWithWatermark)(),
      AttributeReference("rightOther", IntegerType)())

    case class DummyLeafNode() extends LeafNode {
      override def output: Seq[Attribute] =
        attributesToFindConstraintFor ++ attributesWithWatermark
      // override computeStats to avoid UnsupportedOperationException.
      override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(0))
    }

    def watermarkFrom(
        conditionStr: String,
        rightWatermark: Option[Long] = Some(10000)): Option[Long] = {
      val conditionExpr = Some(conditionStr).map { str =>
        val plan =
          Filter(
            CatalystSqlParser.parseExpression(str),
            DummyLeafNode())
        val optimized = SimpleTestOptimizer.execute(SimpleAnalyzer.execute(plan))
        optimized.asInstanceOf[Filter].condition
      }
      val extract = if (legacy) {
        StreamingJoinHelper.getLegacyStateValueWatermark _
      } else {
        StreamingJoinHelper.getStateValueWatermark _
      }
      extract(
        AttributeSet(attributesToFindConstraintFor), AttributeSet(attributesWithWatermark),
        conditionExpr, rightWatermark)
    }

    // Test comparison directionality. E.g. if leftTime < rightTime and rightTime > watermark,
    // then cannot define constraint on leftTime.
    assert(watermarkFrom("leftTime > rightTime") === Some(10000))
    assert(watermarkFrom("leftTime >= rightTime") === Some(9999))
    assert(watermarkFrom("leftTime < rightTime") === None)
    assert(watermarkFrom("leftTime <= rightTime") === None)
    assert(watermarkFrom("rightTime > leftTime") === None)
    assert(watermarkFrom("rightTime >= leftTime") === None)
    assert(watermarkFrom("rightTime < leftTime") === Some(10000))
    assert(watermarkFrom("rightTime <= leftTime") === Some(9999))

    // Test type conversions
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) < CAST(rightTime AS LONG)") === None)
    assert(watermarkFrom("CAST(leftTime AS DOUBLE) > CAST(rightTime AS DOUBLE)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS DOUBLE)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS FLOAT)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS DOUBLE) > CAST(rightTime AS FLOAT)") === Some(10000))
    assert(watermarkFrom("CAST(leftTime AS STRING) > CAST(rightTime AS STRING)") === None)

    // Test with timestamp type + calendar interval on either side of equation
    // Note: timestamptype and calendar interval don't commute, so less valid combinations to test.
    assert(watermarkFrom("leftTime > rightTime + interval 1 second") === Some(11000))
    assert(watermarkFrom("leftTime + interval 2 seconds > rightTime ") === Some(8000))
    assert(watermarkFrom("leftTime > rightTime - interval 3 second") === Some(7000))
    assert(watermarkFrom("rightTime < leftTime - interval 3 second") === Some(13000))
    assert(watermarkFrom("rightTime - interval 1 second < leftTime - interval 3 second")
      === Some(12000))

    assert(watermarkFrom("leftTime > rightTime + interval '0 00:00:01' day to second")
      === Some(11000))
    assert(watermarkFrom("leftTime + interval '00:00:02' hour to second > rightTime ")
      === Some(8000))
    assert(watermarkFrom("leftTime > rightTime - interval '00:03' minute to second")
      === Some(7000))
    assert(watermarkFrom("rightTime < leftTime - interval '1 20:30:40' day to second")
      === Some(160250000))
    assert(watermarkFrom(
      "rightTime - interval 1 second < leftTime - interval '20:15:32' hour to second")
      === Some(72941000))

    // Test with casted long type + constants on either side of equation
    // Note: long type and constants commute, so more combinations to test.
    // -- Constants on the right
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) + 1") === Some(11000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 1") === Some(9000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST((rightTime + interval 1 second) AS LONG)")
      === Some(11000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > 2 + CAST(rightTime AS LONG)") === Some(12000))
    assert(watermarkFrom("CAST(leftTime AS LONG) > -0.5 + CAST(rightTime AS LONG)") === Some(9500))
    assert(watermarkFrom("CAST(leftTime AS LONG) - CAST(rightTime AS LONG) > 2") === Some(12000))
    assert(watermarkFrom("-CAST(rightTime AS DOUBLE) + CAST(leftTime AS LONG) > 0.1")
      === Some(10100))
    assert(watermarkFrom("0 > CAST(rightTime AS LONG) - CAST(leftTime AS LONG) + 0.2")
      === Some(10200))
    // -- Constants on the left
    assert(watermarkFrom("CAST(leftTime AS LONG) + 2 > CAST(rightTime AS LONG)") === Some(8000))
    assert(watermarkFrom("1 + CAST(leftTime AS LONG) > CAST(rightTime AS LONG)") === Some(9000))
    assert(watermarkFrom("CAST((leftTime  + interval 3 second) AS LONG) > CAST(rightTime AS LONG)")
      === Some(7000))
    assert(watermarkFrom("CAST(leftTime AS LONG) - 2 > CAST(rightTime AS LONG)") === Some(12000))
    assert(watermarkFrom("CAST(leftTime AS LONG) + 0.5 > CAST(rightTime AS LONG)") === Some(9500))
    assert(watermarkFrom("CAST(leftTime AS LONG) - CAST(rightTime AS LONG) - 2 > 0")
      === Some(12000))
    assert(watermarkFrom("-CAST(rightTime AS LONG) + CAST(leftTime AS LONG) - 0.1 > 0")
      === Some(10100))
    // -- Constants on both sides, mixed types
    assert(watermarkFrom("CAST(leftTime AS LONG) - 2.0 > CAST(rightTime AS LONG) + 1")
      === Some(13000))

    // Test multiple conditions, should return minimum watermark
    assert(watermarkFrom(
      "leftTime > rightTime - interval 3 second AND rightTime < leftTime + interval 2 seconds") ===
      Some(7000))  // first condition wins
    assert(watermarkFrom(
      "leftTime > rightTime - interval 3 second AND rightTime < leftTime + interval 4 seconds") ===
      Some(6000))  // second condition wins

    // Test invalid comparisons
    assert(watermarkFrom("cast(leftTime AS LONG) > leftOther") === None)      // non-time attributes
    assert(watermarkFrom("leftOther > rightOther") === None)                  // non-time attributes
    assert(watermarkFrom("leftOther > rightOther AND leftTime > rightTime") === Some(10000))
    assert(watermarkFrom("cast(rightTime AS DOUBLE) < rightOther") === None)  // non-time attributes
    assert(watermarkFrom("leftTime > rightTime + interval 1 month") === None) // month not allowed

    if (legacy) {
      // Preserve the original extractor's acceptance and ExpressionSet arithmetic, even
      // for shapes that corrected extraction deliberately rejects.
      assert(watermarkFrom("cast(leftTime AS LONG) > 10") === Some(10000))
      assert(watermarkFrom("cast(leftTime AS LONG) + cast(rightTime AS LONG) > 100") ===
        Some(90000))
      assert(watermarkFrom(
        "cast(leftTime AS LONG) + cast(leftTime AS LONG) > cast(rightTime AS LONG)") ===
        Some(10000))
      assert(watermarkFrom(
        "cast(leftTime AS LONG) > cast(rightTime AS LONG) + cast(rightTime AS LONG)") ===
        Some(10000))
      assert(watermarkFrom("leftOther > cast(rightTime AS LONG)") === Some(10000))
      // Keep the historical results after SQL optimization as well as for raw terms above.
      assert(watermarkFrom("cast(leftTime AS LONG) > cast(rightTime AS LONG) + 2 + 2") ===
        Some(14000))
      assert(watermarkFrom("cast(leftTime AS LONG) > cast(rightTime AS LONG) - 2 - 2") ===
        Some(8000))
    } else {
      // A state bound must advance with the opposite input's watermark.
      assert(watermarkFrom("cast(leftTime AS LONG) > 10") === None)
      assert(watermarkFrom("cast(leftTime AS LONG) + cast(rightTime AS LONG) > 100") === None)
      assert(watermarkFrom(
        "cast(leftTime AS LONG) + cast(leftTime AS LONG) > cast(rightTime AS LONG)") === None)
      assert(watermarkFrom(
        "cast(leftTime AS LONG) > cast(rightTime AS LONG) + cast(rightTime AS LONG)") === None)
      assert(watermarkFrom("leftOther > cast(rightTime AS LONG)") === None)
      assert(watermarkFrom("leftTime > rightTime AND cast(leftTime AS LONG) > 10") === Some(10000))
      assert(watermarkFrom(
        "leftTime > rightTime AND cast(leftTime AS LONG) + cast(rightTime AS LONG) > 100") ===
        Some(10000))
      assert(watermarkFrom("leftTime > rightTime OR cast(leftTime AS LONG) > 10") === None)

      // Equal literal terms must not be deduplicated during canonicalization.
      assert(watermarkFrom("cast(leftTime AS LONG) > cast(rightTime AS LONG) + 2 + 2") ===
        Some(14000))
      assert(watermarkFrom("cast(leftTime AS LONG) > cast(rightTime AS LONG) - 2 - 2") ===
        Some(6000))
    }

    // Test non-positive results
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 10") === Some(0))
    assert(watermarkFrom("CAST(leftTime AS LONG) > CAST(rightTime AS LONG) - 100") === Some(-90000))
  }
}

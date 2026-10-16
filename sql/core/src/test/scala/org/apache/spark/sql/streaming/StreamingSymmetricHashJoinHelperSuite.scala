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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinConditionSplitPredicates, JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate}
import org.apache.spark.sql.types._

class StreamingSymmetricHashJoinHelperSuite extends StreamTest {
  val leftAttributeA = AttributeReference("a", IntegerType)()
  val leftAttributeB = AttributeReference("b", IntegerType)()
  val rightAttributeC = AttributeReference("c", IntegerType)()
  val rightAttributeD = AttributeReference("d", IntegerType)()

  val left = new LocalTableScanExec(Seq(leftAttributeA, leftAttributeB), Seq(), None)
  val right = new LocalTableScanExec(Seq(rightAttributeC, rightAttributeD), Seq(), None)

  test("empty") {
    val split = JoinConditionSplitPredicates(None, left, right)
    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.isEmpty)
  }

  test("only literals") {
    // Literal-only conjuncts end up on the left side because that's the first bucket they fit in.
    // There's no semantic reason they couldn't be in any bucket.
    val predicate = Literal(1) < Literal(5) && Literal(6) < Literal(7) && Literal(0) === Literal(-1)
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only left") {
    val predicate =
      leftAttributeA > Literal(1) && leftAttributeB > Literal(5) && leftAttributeA < leftAttributeB
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(predicate))
    assert(split.rightSideOnly.isEmpty)
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("only right") {
    val predicate = rightAttributeC > Literal(1) && rightAttributeD > Literal(5) &&
      rightAttributeD < rightAttributeC
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.isEmpty)
    assert(split.rightSideOnly.contains(predicate))
    assert(split.bothSides.isEmpty)
    assert(split.full.contains(predicate))
  }

  test("mixed conjuncts") {
    val predicate =
      (leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains((leftAttributeA === rightAttributeC)))
    assert(split.full.contains(predicate))
  }

  test("conjuncts after nondeterministic") {
    val predicate =
      (rand(9) > Literal(0)
        && leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains(
      leftAttributeA === rightAttributeC && rand(9).expr > Literal(0)))
    assert(split.full.contains(predicate))
  }


  test("conjuncts before nondeterministic") {
    val randAttribute = rand(0)
    val predicate =
      (leftAttributeA > leftAttributeB
        && rightAttributeC > rightAttributeD
        && leftAttributeA === rightAttributeC
        && Literal(1) === Literal(1)
        && randAttribute > Literal(0))
    val split = JoinConditionSplitPredicates(Some(predicate), left, right)

    assert(split.leftSideOnly.contains(
      leftAttributeA > leftAttributeB && Literal(1) === Literal(1)))
    assert(split.rightSideOnly.contains(
      rightAttributeC > rightAttributeD && Literal(1) === Literal(1)))
    assert(split.bothSides.contains(
      leftAttributeA === rightAttributeC && randAttribute > Literal(0)))
    assert(split.full.contains(predicate))
  }

  test("SPARK-58904: equality eviction uses the opposite input's watermark") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(lt), Seq(rt), Seq(lt), Seq(rt), None, Some(20000), Some(10000), false)
    assert(predicates.right.isEmpty)
    assert(predicates.left.exists {
      case JoinStateKeyWatermarkPredicate(_, 20000, None) => true
      case _ => false
    })
    assert(StreamingSymmetricHashJoinHelper.getStateWatermark(
      Seq(lt), Seq(rt), Seq(lt), Seq(rt), None, Some(20000), false) === (None, Some(0L)))

    val watermarkedLeft = lt.withMetadata(metadata)
    val both = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(watermarkedLeft), Seq(rt), Seq(watermarkedLeft), Seq(rt), None,
      Some(20000), Some(10000), false)
    assert(both.left.exists {
      case JoinStateKeyWatermarkPredicate(_, 20000, Some(10000)) => true
      case _ => false
    })
    assert(both.right.isDefined)
  }

  test("SPARK-58904: range eviction must include newly admitted rows below the previous bound") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType, metadata = metadata)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    Seq(-5, 5).foreach { offset =>
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(lt), Seq(rt), Nil, Nil, Some(lt > rt + offset),
        Some(20000), Some(10000), false)
      val expectedPrevious = if (offset < 0) Some(5000L) else Some(10000L)
      assert(predicates.left.exists {
        case JoinStateValueWatermarkPredicate(_, watermark, previous) =>
          watermark == 20000 + offset * 1000 && previous == expectedPrevious
        case _ => false
      })
    }
  }

  test("SPARK-58904: a bound on another timestamp does not advance the output watermark") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType)()
    val other = AttributeReference("other", TimestampType, metadata = metadata)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val condition = Some(lt > rt - 5)
    val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(lt, other), Seq(rt), Nil, Nil, condition, Some(20000), Some(10000), false)
    assert(predicates.left.exists {
      case JoinStateValueWatermarkPredicate(expr, 15000, None) => expr.references.contains(lt)
      case _ => false
    })
    assert(StreamingSymmetricHashJoinHelper.getStateWatermark(
      Seq(lt, other), Seq(rt), Nil, Nil, condition, Some(20000), false) ===
      (Some(0L), Some(0L)))
  }

  test("SPARK-58904: a fixed bound can mask a legacy difference at an earlier watermark") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType, metadata = metadata)()
    val li = AttributeReference("li", IntegerType)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val condition = Some(lt > rt && li > Literal(10))

    def leftWatermark(watermark: Long, legacy: Boolean): Long = {
      val predicates = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
        Seq(lt, li), Seq(rt), Nil, Nil, condition, Some(watermark), None, false, legacy)
      predicates.left.get.asInstanceOf[JoinStateValueWatermarkPredicate].stateWatermark
    }

    assert(leftWatermark(0L, legacy = true) == leftWatermark(0L, legacy = false))
    assert(leftWatermark(20000L, legacy = true) == 10000L)
    assert(leftWatermark(20000L, legacy = false) == 20000L)
  }

  test("SPARK-58904: legacy warning identifies one-sided watermark shapes") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val range = Some(lt > rt - 5)

    def changes(
        leftKeys: Seq[AttributeReference] = Nil,
        rightKeys: Seq[AttributeReference] = Nil,
        condition: Option[Expression] = range): Seq[String] = {
      StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
        Seq(lt), Seq(rt), leftKeys, rightKeys, condition)
    }

    assert(changes() == Seq("left-state cleanup"))
    assert(changes(Seq(lt), Seq(rt), None) == Seq("right-state cleanup"))
    val watermarkedLeft = lt.withMetadata(metadata)
    assert(StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
      Seq(watermarkedLeft), Seq(rt), Seq(watermarkedLeft), Seq(rt), None).isEmpty)
    assert(StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
      Seq(watermarkedLeft), Seq(lt), Seq(watermarkedLeft), Seq(lt), None) ==
      Seq("left-state cleanup"))
  }

  test("SPARK-58904: legacy warning identifies an additional state-column bound") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType, metadata = metadata)()
    val li = AttributeReference("li", LongType)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()

    def changes(bound: Long): Seq[String] = {
      StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
        Seq(lt, li), Seq(rt), Nil, Nil, Some(lt > rt && li > Literal(bound)))
    }

    assert(changes(300000000000L) == Seq("left-state cleanup"))
    assert(changes(10000000000000L) == Seq("left-state cleanup"))
    assert(StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
      Seq(lt, li), Seq(rt), Nil, Nil, Some(li > Literal(10L))) ==
      Seq("left-state cleanup"))
    assert(StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
      Seq(lt), Seq(rt), Nil, Nil, Some(lt > rt)).isEmpty)
  }

  test("SPARK-58904: legacy warning identifies changed event-time bounds") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType, metadata = metadata)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()

    Seq(lt > Literal(10L), lt + rt > Literal(100L), lt > rt + 2 + 2).foreach { condition =>
      assert(StreamingSymmetricHashJoinHelper.legacyWatermarkChangeSides(
        Seq(lt), Seq(rt), Nil, Nil, Some(condition)).contains("left-state cleanup"))
    }
  }

  test("SPARK-58904: legacy V4 keeps its watermark predicates") {
    val metadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 1000).build()
    val lt = AttributeReference("lt", TimestampType)()
    val rt = AttributeReference("rt", TimestampType, metadata = metadata)()
    val condition = Some(lt > rt - 5)
    val legacy = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(lt), Seq(rt), Nil, Nil, condition, Some(20000), None, false,
      useLegacyWatermarkSemantics = true)
    assert(legacy.left.isEmpty && legacy.right.isEmpty)
    assert(StreamingSymmetricHashJoinHelper.getStateWatermark(
      Seq(lt), Seq(rt), Nil, Nil, condition, Some(20000), false,
      useLegacyWatermarkSemantics = true) === (None, None))

    val legacyKey = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(lt), Seq(rt), Seq(lt), Seq(rt), None, Some(20000), None, false,
      useLegacyWatermarkSemantics = true)
    assert(legacyKey.left.isDefined && legacyKey.right.isDefined)

    val watermarkedLeft = lt.withMetadata(metadata)
    val legacyRange = StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
      Seq(watermarkedLeft), Seq(rt), Nil, Nil, Some(watermarkedLeft > rt + 5),
      Some(20000), Some(10000), false, useLegacyWatermarkSemantics = true)
    assert(legacyRange.left.exists {
      case JoinStateValueWatermarkPredicate(_, 25000, Some(10000)) => true
      case _ => false
    })
  }
}

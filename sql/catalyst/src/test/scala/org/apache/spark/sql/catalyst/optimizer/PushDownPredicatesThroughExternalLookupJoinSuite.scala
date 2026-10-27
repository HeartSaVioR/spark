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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.FormExternalLookupJoin
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Rand}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PushDownPredicatesThroughExternalLookupJoinSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Push down predicates", FixedPoint(10),
      PushDownPredicatesThroughExternalLookupJoin) :: Nil
  }

  private val stream = LocalRelation(Seq($"a".int), isStreaming = true)
  private val lookup = DataSourceV2Relation.create(
    LookupOnlyTable, None, None, CaseInsensitiveStringMap.empty)
  private val a = stream.output.head
  private val Seq(id, x) = lookup.output
  private val key = a === id
  // A filter conjunct that is not deterministic never moves.
  private val random = Rand(1L) > 0.5

  /** An external lookup join, as the optimizer forms it. */
  private def lookupJoin(joinType: JoinType, conjuncts: Expression*): ExternalLookupJoin =
    FormExternalLookupJoin(
      Join(stream, lookup, joinType, Some(conjuncts.reduce(_ && _)), JoinHint.NONE))
      .asInstanceOf[ExternalLookupJoin]

  private def expect(
      join: ExternalLookupJoin,
      left: LogicalPlan = stream,
      right: LogicalPlan = lookup,
      conjuncts: Seq[Expression]): ExternalLookupJoin =
    join.copy(left = left, right = right, condition = Some(conjuncts.reduce(_ && _)))

  test("push a filter above the join into the stream side and the join condition") {
    Seq[(JoinType, Expression, ExternalLookupJoin => LogicalPlan)](
      // The lookup-side conjunct goes through the join condition onto the lookup side, and the
      // conjunct on both sides into the join condition.
      (Inner, a > 1 && x > 2 && a + x > 0 && random, join =>
        expect(join, stream.where(a > 1), lookup.where(x > 2), Seq(key, a + x > 0)).where(random)),
      // Conjuncts on the lookup side of a left outer join stay above it.
      (LeftOuter, a > 1 && x > 2, join =>
        expect(join, stream.where(a > 1), conjuncts = Seq(key)).where(x > 2)),
      (LeftSemi, a > 1 && random, join =>
        expect(join, stream.where(a > 1), conjuncts = Seq(key)).where(random)),
      (LeftAnti, a > 1 && random, join =>
        expect(join, stream.where(a > 1), conjuncts = Seq(key)).where(random))).foreach {
      case (joinType, filter, expected) =>
        val join = lookupJoin(joinType, key)
        comparePlans(Optimize.execute(join.where(filter)), expected(join))
    }
  }

  test("push join condition conjuncts on one side into that side") {
    val intoBothSides: ExternalLookupJoin => LogicalPlan = join =>
      expect(join, stream.where(a > 1), lookup.where(x > 2), Seq(key, a + x > 0))
    // A stream-side conjunct of a left outer or left anti join decides which stream rows find a
    // match, so it stays in the condition.
    val intoLookupSide: ExternalLookupJoin => LogicalPlan = join =>
      expect(join, right = lookup.where(x > 2), conjuncts = Seq(key, a > 1, a + x > 0))
    Seq(
      Inner -> intoBothSides,
      LeftSemi -> intoBothSides,
      LeftOuter -> intoLookupSide,
      LeftAnti -> intoLookupSide).foreach { case (joinType, expected) =>
      val join = lookupJoin(joinType, key, a > 1, x > 2, a + x > 0)
      comparePlans(Optimize.execute(join), expected(join))
    }
  }

  test("leave a join without movable conjuncts unchanged") {
    Seq(
      lookupJoin(Inner, key),
      lookupJoin(LeftOuter, key, a + x > 0),
      lookupJoin(LeftAnti, key, a > 1),
      // A literal belongs to neither side.
      lookupJoin(Inner, key, Literal(false))).foreach { join =>
      assert(Optimize.execute(join) eq join)
    }
    val filtered = lookupJoin(LeftOuter, key).where(x > 2 && random)
    assert(Optimize.execute(filtered) eq filtered)
  }
}

private case object LookupOnlyTable extends Table {
  override def name(): String = "LookupOnlyTable"
  override def schema(): StructType = StructType.fromDDL("id INT, x INT")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.EXTERNAL_LOOKUP)
}

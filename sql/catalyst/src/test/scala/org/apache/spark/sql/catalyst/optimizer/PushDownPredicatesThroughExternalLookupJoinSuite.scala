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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.analysis.FormExternalLookupJoin
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PythonUDF, Rand,
  ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{BooleanType, StructType}
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
  // A lookup-side conjunct that is not a lookup candidate, so it can move onto the lookup side.
  private val lookupFilter = x + 1 > 2
  private val joinTypes = Seq(Inner, LeftOuter, LeftSemi, LeftAnti)
  private val pythonUDF = (args: Seq[Expression]) =>
    PythonUDF("udf", null, BooleanType, args, PythonEvalType.SQL_BATCHED_UDF, true)
  private val subquery = ScalarSubquery(LocalRelation($"s".int))

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
      (Inner, a > 1 && lookupFilter && a + x > 0 && random, join =>
        expect(join, stream.where(a > 1), lookup.where(lookupFilter), Seq(key, a + x > 0))
          .where(random)),
      // Conjuncts on the lookup side of a left outer join stay above it.
      (LeftOuter, a > 1 && lookupFilter, join =>
        expect(join, stream.where(a > 1), conjuncts = Seq(key)).where(lookupFilter)),
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
      expect(join, stream.where(a > 1), lookup.where(lookupFilter), Seq(key, a + x > 0))
    // A stream-side conjunct of a left outer or left anti join decides which stream rows find a
    // match, so it stays in the condition.
    val intoLookupSide: ExternalLookupJoin => LogicalPlan = join =>
      expect(join, right = lookup.where(lookupFilter), conjuncts = Seq(key, a > 1, a + x > 0))
    Seq(
      Inner -> intoBothSides,
      LeftSemi -> intoBothSides,
      LeftOuter -> intoLookupSide,
      LeftAnti -> intoLookupSide).foreach { case (joinType, expected) =>
      val join = lookupJoin(joinType, key, a > 1, lookupFilter, a + x > 0)
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
    val filtered = lookupJoin(LeftOuter, key).where(lookupFilter && random)
    assert(Optimize.execute(filtered) eq filtered)
  }

  test("keep a lookup candidate in the join condition") {
    // The connector can select these, so they must stay where it is offered them, as for a join
    // without this rule. Spark evaluates the ones it declines.
    val candidates = Seq[Expression](
      x === 5, x <=> 5, x < 5, x <= 5, x > 5, x >= 5,
      Literal(5) === x, Literal(5) <=> x, Literal(5) < x, Literal(5) >= x)
    for (joinType <- joinTypes; candidate <- candidates) {
      val join = lookupJoin(joinType, key, candidate)
      assert(Optimize.execute(join) eq join)
    }
    // A filter above an inner join moves into the condition and is offered there as well; under
    // a left outer join it stays above.
    val inner = lookupJoin(Inner, key)
    comparePlans(
      Optimize.execute(inner.where(x === 5)), expect(inner, conjuncts = Seq(key, x === 5)))
    val outer = lookupJoin(LeftOuter, key).where(x === 5)
    assert(Optimize.execute(outer) eq outer)
    // The shape is exact: only a bare column compared with a stream-side value or a constant is
    // a candidate.
    val notCandidates = Seq[Expression](x =!= 5, x + 1 > 5, x.in(1, 2), x.isNull)
    for (conjunct <- notCandidates) {
      val join = lookupJoin(Inner, key, conjunct)
      comparePlans(Optimize.execute(join), expect(join, right = lookup.where(conjunct),
        conjuncts = Seq(key)))
    }
  }

  test("keep a conjunct that a lookup-side stage cannot evaluate where it is") {
    // The conjuncts read lookup columns, so a lookup-side stage would have to evaluate them, and
    // a stage with a Python UDF or a subquery cannot be planned.
    val pythonUDFs = Seq[Expression](pythonUDF(Seq(x)), pythonUDF(Seq(a, x)))
    // A subquery cannot be in the condition of a lookup join, only in a filter above it.
    val unsupported = pythonUDFs :+ (x > subquery)
    for (joinType <- joinTypes; conjunct <- pythonUDFs) {
      val inCondition = lookupJoin(joinType, key, conjunct)
      assert(Optimize.execute(inCondition) eq inCondition)
    }
    for (joinType <- Seq(Inner, LeftOuter); conjunct <- unsupported) {
      val above = lookupJoin(joinType, key).where(conjunct)
      assert(Optimize.execute(above) eq above)
    }
    // Over the stream side alone, they are ordinary conjuncts.
    val streamUDF = pythonUDF(Seq(a))
    val join = lookupJoin(Inner, key, streamUDF)
    comparePlans(
      Optimize.execute(join), expect(join, left = stream.where(streamUDF), conjuncts = Seq(key)))
  }
}

private case object LookupOnlyTable extends Table {
  override def name(): String = "LookupOnlyTable"
  override def schema(): StructType = StructType.fromDDL("id INT, x INT")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.EXTERNAL_LOOKUP)
}

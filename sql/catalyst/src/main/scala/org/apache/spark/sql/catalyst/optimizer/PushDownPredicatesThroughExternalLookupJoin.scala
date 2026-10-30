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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, ExternalLookupJoinShapes,
  Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXTERNAL_LOOKUP_JOIN

/**
 * Pushes predicates through an [[ExternalLookupJoin]], which [[PushPredicateThroughJoin]] does not
 * handle because it is not a `Join`. It moves conjuncts the way that rule does for the same join
 * types, and only deterministic conjuncts that cannot throw:
 *  - A filter above the join: conjuncts on the stream side alone go into the stream side. Under
 *    an inner join, the other conjuncts that a join can evaluate go into the join condition. The
 *    rest stay above; in particular, conjuncts on the lookup side of a left outer join stay above.
 *  - The join condition: conjuncts on the lookup side alone go onto the lookup side, as a filter
 *    that `PrepareExternalLookupJoin` pushes to the connector or evaluates on its results. Under
 *    an inner or left semi join, conjuncts on the stream side alone go into the stream side;
 *    under a left outer or left anti join they stay in the condition.
 *
 * Two kinds of conjuncts never move onto the lookup side, so that this rule cannot make a query
 * fail that runs without it:
 *  - A comparison that `PrepareExternalLookupJoin` can offer as a lookup candidate, such as
 *    `t.dept = 'x'`, stays in the condition. The connector still sees it as a candidate, and
 *    Spark evaluates it if the connector declines it.
 *  - A conjunct over lookup columns that a lookup-side stage cannot evaluate, such as one with a
 *    Python UDF, does not move at all. It stays where the join planning handles it.
 */
object PushDownPredicatesThroughExternalLookupJoin extends Rule[LogicalPlan]
  with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsPattern(EXTERNAL_LOOKUP_JOIN)) {
      case filter @ Filter(condition, join: ExternalLookupJoin) =>
        pushFilter(filter, splitConjunctivePredicates(condition), join)
      case join: ExternalLookupJoin =>
        pushJoinCondition(join)
    }

  private def pushFilter(
      filter: Filter,
      conjuncts: Seq[Expression],
      join: ExternalLookupJoin): LogicalPlan = {
    val (streamOnly, lookupOnly, others) = split(conjuncts, join)
    val intoCondition = join.joinType match {
      case Inner =>
        (lookupOnly ++ others).filter { c =>
          canMove(c) && !isPinned(c, join) && canEvaluateWithinJoin(c)
        }
      case LeftOuter | LeftSemi | LeftAnti => Nil
      case _ => return filter
    }
    if (streamOnly.isEmpty && intoCondition.isEmpty) return filter
    val newJoin = join.copy(
      left = withFilter(streamOnly, join.left),
      condition = (join.condition.toSeq ++ intoCondition).reduceLeftOption(And))
    val stayAbove = conjuncts.filterNot(c => streamOnly.contains(c) || intoCondition.contains(c))
    withFilter(stayAbove, newJoin)
  }

  private def pushJoinCondition(join: ExternalLookupJoin): LogicalPlan = {
    val conjuncts = join.condition.toSeq.flatMap(splitConjunctivePredicates)
    val (streamOnly, lookupOnly, _) = split(conjuncts, join)
    val intoStreamSide = join.joinType match {
      case Inner | LeftSemi => streamOnly
      case LeftOuter | LeftAnti => Nil
      case _ => return join
    }
    if (intoStreamSide.isEmpty && lookupOnly.isEmpty) return join
    join.copy(
      left = withFilter(intoStreamSide, join.left),
      right = withFilter(lookupOnly, join.right),
      condition = conjuncts.filterNot(c => intoStreamSide.contains(c) || lookupOnly.contains(c))
        .reduceLeftOption(And))
  }

  /**
   * Splits conjuncts the way [[PushPredicateThroughJoin]] does: (movable conjuncts on the stream
   * side alone, movable conjuncts on the lookup side alone, the rest). Unlike there, a conjunct
   * without references, such as a literal, belongs to neither side and stays where it is, so that
   * a condition that folds to false keeps its result without a lookup. A lookup candidate belongs
   * to the rest.
   */
  private def split(
      conjuncts: Seq[Expression],
      join: ExternalLookupJoin): (Seq[Expression], Seq[Expression], Seq[Expression]) = {
    val (movable, stayUp) = conjuncts.partition { c =>
      canMove(c) && c.references.nonEmpty && !isPinned(c, join)
    }
    val (streamOnly, rest) = movable.partition(_.references.subsetOf(join.left.outputSet))
    val (lookupSide, common) = rest.partition(_.references.subsetOf(join.right.outputSet))
    val (candidates, lookupOnly) = lookupSide.partition(isLookupCandidate(_, join))
    (streamOnly, lookupOnly, common ++ candidates ++ stayUp)
  }

  /** Whether `PrepareExternalLookupJoin` can offer `conjunct` as a lookup candidate. */
  private def isLookupCandidate(conjunct: Expression, join: ExternalLookupJoin): Boolean =
    ExternalLookupJoinShapes.lookupComparison(
      conjunct, join.left.outputSet, join.right.outputSet).isDefined

  /**
   * Whether `conjunct` reads lookup columns and could not be evaluated by a lookup-side stage. It
   * never moves: a stage with it would fail to plan.
   */
  private def isPinned(conjunct: Expression, join: ExternalLookupJoin): Boolean =
    conjunct.references.exists(join.right.outputSet.contains) &&
      !ExternalLookupJoinShapes.canEvaluateInLookupStage(conjunct)

  private def canMove(conjunct: Expression): Boolean =
    conjunct.deterministic && !conjunct.throwable

  private def withFilter(conjuncts: Seq[Expression], plan: LogicalPlan): LogicalPlan =
    conjuncts.reduceLeftOption(And).map(Filter(_, plan)).getOrElse(plan)
}

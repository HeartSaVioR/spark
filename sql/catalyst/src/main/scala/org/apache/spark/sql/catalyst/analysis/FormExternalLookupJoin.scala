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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{CommonExpressionRef, Expression,
  SubqueryExpression, With}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupHint, ExternalLookupJoin,
  Filter, Join, JoinStrategyHint, LogicalPlan, Project, ResolvedHint, SubqueryAlias,
  UnresolvedHint, UnresolvedWith, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMON_EXPR_REF, EXTERNAL_LOOKUP_HINT, JOIN,
  WITH_EXPRESSION}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.v2.ExternalLookupRelations
import org.apache.spark.sql.internal.SQLConf

/** Resolves the `external_lookup` option hint to an occurrence-local marker. */
object ResolveExternalLookupHint extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    ExternalLookupJoinEligibility.resolveHints(plan)
}

/**
 * Classifies a join for external lookup. [[CheckExternalLookupJoin]] and [[FormExternalLookupJoin]]
 * share it, so a join that passes analysis is exactly a join that the optimizer converts.
 *
 * A join is an external lookup join when its right side reads a table that supports external
 * lookup but not batch read, or when the right side carries an `EXTERNAL_LOOKUP` hint. A table
 * that supports both keeps the ordinary join strategies unless the hint opts in. It must be a
 * stream-static join: the left side is a streaming query, whose micro-batches tolerate lookup
 * results that change between requests, and the lookup table is on the right side.
 */
object ExternalLookupJoinEligibility extends QueryErrorsBase {
  sealed trait Eligibility
  /** The join neither reads a lookup-only table nor has the hint, and stays an ordinary join. */
  case object NotLookupJoin extends Eligibility
  /** The join runs as an external lookup join. */
  case object Eligible extends Eligibility
  /** The join must run as an external lookup join but has no join condition. */
  case object MissingCondition extends Eligibility
  /** The join must run as an external lookup join but cannot. */
  case class Ineligible(error: Throwable) extends Eligibility

  private val HintName = "EXTERNAL_LOOKUP"
  private val SupportedJoinTypes = Seq(Inner, LeftOuter)

  private[analysis] def resolveHints(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUp {
      case UnresolvedHint(name, parameters, child)
          if name.toUpperCase(Locale.ROOT) == HintName =>
        resolveHint(parameters, child)
    }

  def apply(join: Join): Eligibility = {
    if (lookupHints(join.left).nonEmpty) {
      return Ineligible(QueryCompilationErrors.invalidExternalLookupHintError("NOT_ON_LOOKUP_SIDE"))
    }
    // The lookup-only table must be on the right side; the sides are never swapped.
    val leftLookupOnlyTable = ExternalLookupRelations.lookupOnlyTable(join.left)
    if (leftLookupOnlyTable.isDefined) {
      return ineligible("LOOKUP_TABLE_ON_LEFT", toSQLId(leftLookupOnlyTable.get.name()))
    }
    val markers = lookupHints(join.right)
    // TODO: look through projections, filters and aliases above the lookup relation.
    val lookupTable = ExternalLookupRelations.table(accessPath(join.right).last)
    val lookupOnly = lookupTable.exists(ExternalLookupRelations.isLookupOnly)
    // TODO: allow the EXTERNAL_LOOKUP hint once it is introduced, and apply these checks to the
    //   joins it opts in.
    if (markers.isEmpty && !lookupOnly) return NotLookupJoin
    if (markers.length > 1) {
      return Ineligible(QueryCompilationErrors.invalidExternalLookupHintError("MULTIPLE_HINTS"))
    }
    val relation = lookupTable.map(table => toSQLId(table.name())).getOrElse("the lookup side")
    if (!join.left.isStreaming) return ineligible("STATIC_LEFT_SIDE", relation)
    // A join hint the user wrote on a lookup-only join is never silently dropped: an external
    // lookup join cannot run a physical join strategy, and a lookup-only table has no batch scan
    // for one.
    joinStrategyHint(join) match {
      case Some(strategy) if lookupOnly =>
        ineligible("CONFLICTING_JOIN_STRATEGY", relation,
          Map("strategy" -> toSQLStmt(strategy.displayName)))
      // A lookup-only table has no batch scan, so a physical strategy hint cannot apply to it.
      // For a table that also supports batch reads, the strategy hint wins.
      case Some(_) => NotLookupJoin
      case None if join.condition.isEmpty => MissingCondition
      case None if !SupportedJoinTypes.contains(join.joinType) =>
        ineligible("JOIN_TYPE", relation, Map(
          "joinType" -> toSQLStmt(join.joinType.sql),
          "supportedJoinTypes" -> SupportedJoinTypes.map(t => toSQLStmt(t.sql)).mkString(", ")))
      // The optimizer rewrites a subquery in a join condition only for a Join, which a lookup join
      // no longer is by then.
      case None if join.condition.exists(SubqueryExpression.hasSubquery) =>
        ineligible("SUBQUERY_IN_JOIN_CONDITION", relation)
      case None if join.right.isStreaming && markers.nonEmpty =>
        Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "STREAMING_LOOKUP_SIDE"))
      case None if join.right.isStreaming => NotLookupJoin
      case None => Eligible
    }
  }

  /** The lookup side of an eligible join, without its `EXTERNAL_LOOKUP` hint. */
  def lookupSide(join: Join): LogicalPlan = removeLookupHints(join.right)

  private def ineligible(
      subClass: String,
      relation: String,
      parameters: Map[String, String] = Map.empty): Ineligible =
    Ineligible(QueryCompilationErrors.unsupportedExternalLookupJoinError(
      subClass, parameters + ("relation" -> relation)))

  /**
   * The operators from a join input down to the relation that a lookup would read. The path stops
   * at the first operator that cannot be part of a lookup side, such as another join, so a table
   * read by a nested join does not make the outer join a lookup candidate.
   */
  private def accessPath(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case node @ (_: ExternalLookupHint | _: ResolvedHint | _: SubqueryAlias | _: View |
        _: Project | _: Filter) =>
      node +: accessPath(node.children.head)
    case other => Seq(other)
  }

  private def lookupHints(plan: LogicalPlan): Seq[ExternalLookupHint] =
    accessPath(plan).collect { case marker: ExternalLookupHint => marker }

  private def removeLookupHints(plan: LogicalPlan): LogicalPlan = plan match {
    case marker: ExternalLookupHint => removeLookupHints(marker.child)
    case node @ (_: ResolvedHint | _: SubqueryAlias | _: View | _: Project | _: Filter) =>
      node.withNewChildren(Seq(removeLookupHints(node.children.head)))
    case other => other
  }

  private def joinStrategyHint(join: Join): Option[JoinStrategyHint] = {
    // Only the hints that EliminateResolvedHint will attach to this join count; a strategy hint
    // on a nested join belongs to that join.
    val attached = join.hint.leftHint.toSeq ++ join.hint.rightHint.toSeq ++
      Seq(join.left, join.right).flatMap(EliminateResolvedHint.extractHintsFromPlan(_)._2)
    attached.flatMap(_.strategy).headOption
  }

  private def resolveHint(parameters: Seq[Expression], child: LogicalPlan): LogicalPlan = {
    val relationNames = mutable.ArrayBuffer.empty[Seq[String]]
    parameters.foreach {
      case attr: UnresolvedAttribute =>
        relationNames += attr.nameParts
      case other =>
        throw QueryCompilationErrors.invalidExternalLookupHintError(
          "UNSUPPORTED_PARAMETER", Map("parameter" -> toSQLExpr(other)))
    }
    relationNames.diff(relationNames.distinct).headOption.foreach { duplicate =>
      throw QueryCompilationErrors.invalidExternalLookupHintError(
        "DUPLICATE_RELATION", Map("relation" -> toSQLId(duplicate)))
    }

    if (relationNames.isEmpty) {
      var wrappedJoin = false
      val rewritten = child.transformDown {
        case join: Join if !wrappedJoin =>
          wrappedJoin = true
          join.copy(right = ExternalLookupHint(join.right))
      }
      if (wrappedJoin) rewritten else ExternalLookupHint(child)
    } else {
      val matched = mutable.HashSet.empty[Seq[String]]
      def matches(identifier: Seq[String]): Boolean = relationNames.find { name =>
        !matched.contains(name) &&
          ResolveHints.matchedIdentifier(name, identifier, SQLConf.get.resolver)
      }.map(matched.add).nonEmpty
      // Mirrors the target scoping of ResolveHints.ResolveJoinStrategyHints: a name matches an
      // unaliased (possibly multipart) table or an alias, and does not reach into a view,
      // a WITH clause, or an aliased subquery.
      def applyHint(plan: LogicalPlan): LogicalPlan = plan match {
        case relation: UnresolvedRelation if matches(relation.multipartIdentifier) =>
          ExternalLookupHint(relation)
        case alias: SubqueryAlias
            if matches(alias.identifier.qualifier :+ alias.identifier.name) =>
          ExternalLookupHint(alias)
        case _: View | _: UnresolvedWith | _: SubqueryAlias => plan
        case _ => plan.mapChildren(applyHint)
      }
      val rewritten = applyHint(child)
      val missing = relationNames.filterNot(matched.contains)
      if (missing.nonEmpty) {
        throw QueryCompilationErrors.invalidExternalLookupHintError(
          "RELATION_NOT_FOUND", Map("relations" -> missing.map(toSQLId).mkString(", ")))
      }
      rewritten
    }
  }
}

/**
 * Reports, while a query is analyzed, why a join cannot run as the external lookup join it must
 * be. It does not change the plan: the join stays a [[Join]] until [[FormExternalLookupJoin]]
 * converts it, so the APIs and rules that expect an analyzed join keep working. A join that is not
 * resolved, for example one whose condition is not boolean, is left to the analyzer's own join
 * checks.
 *
 * A missing join condition is not reported here, because the classic Dataset API analyzes a USING
 * join first as a join without a condition.
 */
object CheckExternalLookupJoin extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {
    if (plan.containsPattern(JOIN)) {
      plan.foreachUp {
        case join: Join if join.resolved =>
          ExternalLookupJoinEligibility(join) match {
            case ExternalLookupJoinEligibility.Ineligible(error) => throw error
            case _ =>
          }
        case _ =>
      }
    }
  }
}

/**
 * Replaces each join that runs as an external lookup join with an [[ExternalLookupJoin]], and
 * removes the `EXTERNAL_LOOKUP` hints that no join consumed.
 *
 * It must run at the start of optimization, before Finish Analysis and operator optimization, so
 * that it sees the join and its lookup side as analysis left them: no hint or alias eliminated,
 * no predicate pushed into the join and no column pruned. These are the shapes that
 * [[CheckExternalLookupJoin]] validated.
 */
object FormExternalLookupJoin extends Rule[LogicalPlan] {
  import ExternalLookupJoinEligibility._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val formed = plan.transformUpWithPruning(_.containsPattern(JOIN), ruleId) {
      case join: Join =>
        ExternalLookupJoinEligibility(join) match {
          case Eligible =>
            val lookupPlan = lookupSide(join)
            ExternalLookupJoin(
              join.left,
              lookupPlan,
              join.joinType,
              join.condition.map(inlineCommonExpressions),
              lookupPlan.output)
          // Operator optimization could push a filter's predicate into the join later, but the
          // condition must be written in the join.
          case MissingCondition =>
            throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
              "MISSING_JOIN_CONDITION")
          case Ineligible(error) => throw error
          case NotLookupJoin => join
        }
    }
    // A join with the hint on its lookup side has been converted or has failed, so a hint that is
    // left is not on the lookup side of any join.
    formed.transformUpWithPruning(_.containsPattern(EXTERNAL_LOOKUP_HINT)) {
      case hint: ExternalLookupHint =>
        logWarning("An EXTERNAL_LOOKUP hint is not on the lookup side of a join and is ignored.")
        hint.child
    }
  }

  /**
   * Inlines the common expressions of every `With` in a lookup join condition, such as the input of
   * `BETWEEN`, at each of their references. `RewriteWithExpression` would otherwise evaluate one
   * that is not cheap in a projection below the join, over the lookup side, which the lookup join
   * cannot read. A definition can be evaluated more than once: analysis allows only deterministic
   * join conditions.
   */
  private def inlineCommonExpressions(condition: Expression): Expression =
    condition.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case With(child, defs) =>
        val definitions = defs.map(definition => definition.id -> definition.child).toMap
        // A reference to an enclosing `With` is inlined when the transformation reaches it.
        child.transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          case ref: CommonExpressionRef if definitions.contains(ref.id) => definitions(ref.id)
        }
    }
}

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

package org.apache.spark.sql.execution.datasources.v2

import java.util.IdentityHashMap

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet,
  EqualNullSafe, EqualTo, Expression, ExprId, GreaterThan, GreaterThanOrEqual, LessThan,
  LessThanOrEqual, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupFilter, ExternalLookupJoin,
  ExternalLookupProject, ExternalLookupTransform, Filter, LogicalPlan, PreparedExternalLookupJoin,
  Project, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.{LookupParameter, SupportsExternalLookup,
  SupportsPushDownRequiredColumns}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.SQLConf

/** Finalizes an optimized external lookup side immediately before ordinary V2 scan preparation. */
object PrepareExternalLookupJoin extends Rule[LogicalPlan] with PredicateHelper
  with QueryErrorsBase {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case join: ExternalLookupJoin => prepare(join)
  }.transformUp {
    // Prepared lookup joins no longer contain their lookup relation, so any lookup-only relation
    // left here would have to be scanned.
    case relation: ExternalLookupRelation =>
      throw QueryCompilationErrors.unsupportedBatchReadError(relation.table)
    case relation: DataSourceV2Relation
        if !relation.table.capabilities().contains(TableCapability.BATCH_READ) =>
      throw QueryCompilationErrors.unsupportedBatchReadError(relation.table)
  }

  private def prepare(join: ExternalLookupJoin): PreparedExternalLookupJoin = {
    val extracted = extractLookupPlan(join.right).getOrElse {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError("LOOKUP_PLAN")
    }
    val relation = extracted.relation
    val tableName = relation.table.name()
    val supportsRead = relation.table match {
      case table: SupportsRead
          if table.capabilities().contains(TableCapability.EXTERNAL_LOOKUP) => table
      case _ =>
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "TABLE", Map("table" -> toSQLId(tableName)))
    }
    val scanBuilder = supportsRead.newScanBuilder(relation.options)
    val lookupBuilder = scanBuilder match {
      case builder: SupportsExternalLookup => builder
      case _ =>
        throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
          "SCAN_BUILDER", Map("table" -> toSQLId(tableName)))
    }

    val remainingTransforms = pushBaseFilters(scanBuilder, relation, extracted.transforms)
    val conjuncts = join.condition.toSeq.flatMap(splitConjunctivePredicates)
    val offeredCandidates = mutable.ArrayBuffer.empty[OfferedLookupPredicate]
    val residual = mutable.ArrayBuffer.empty[Expression]
    conjuncts.foreach {
      case equality @ EqualTo(leftExpr, rightAttr: Attribute)
          if leftExpr.references.subsetOf(join.left.outputSet) &&
            join.right.outputSet.contains(rightAttr) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, equality, "=", false)
      case equality @ EqualTo(rightAttr: Attribute, leftExpr)
          if leftExpr.references.subsetOf(join.left.outputSet) &&
            join.right.outputSet.contains(rightAttr) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, equality, "=", false)
      case equality @ EqualNullSafe(leftExpr, rightAttr: Attribute)
          if leftExpr.references.subsetOf(join.left.outputSet) &&
            join.right.outputSet.contains(rightAttr) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, equality, "<=>", true)
      case equality @ EqualNullSafe(rightAttr: Attribute, leftExpr)
          if leftExpr.references.subsetOf(join.left.outputSet) &&
            join.right.outputSet.contains(rightAttr) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, equality, "<=>", true)
      case comparison @ LessThan(leftExpr, rightAttr: Attribute)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, ">", false)
      case comparison @ LessThan(rightAttr: Attribute, leftExpr)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, "<", false)
      case comparison @ LessThanOrEqual(leftExpr, rightAttr: Attribute)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, ">=", false)
      case comparison @ LessThanOrEqual(rightAttr: Attribute, leftExpr)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, "<=", false)
      case comparison @ GreaterThan(leftExpr, rightAttr: Attribute)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, "<", false)
      case comparison @ GreaterThan(rightAttr: Attribute, leftExpr)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, ">", false)
      case comparison @ GreaterThanOrEqual(leftExpr, rightAttr: Attribute)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, "<=", false)
      case comparison @ GreaterThanOrEqual(rightAttr: Attribute, leftExpr)
          if isLookupComparison(leftExpr, rightAttr, join) =>
        offeredCandidates += OfferedLookupPredicate(leftExpr, rightAttr, comparison, ">=", false)
      case other => residual += other
    }

    val projectedToInput = traceToRelation(relation.output, remainingTransforms)
    val candidates = offeredCandidates.flatMap { candidate =>
      projectedToInput.get(candidate.projectedField.exprId).flatten.map { input =>
        candidate.copy(projectedField = input)
      }
    }
    val candidateConditions = candidates.map(_.condition).toSet
    offeredCandidates.foreach { candidate =>
      if (!candidateConditions.contains(candidate.condition)) residual += candidate.condition
    }
    val offeredPredicates = candidates.map { candidate =>
      new V2Predicate(candidate.predicateName, Array[V2Expression](
        FieldReference.column(candidate.projectedField.name),
        new LookupParameter(candidate.parameter.dataType)))
    }.toArray
    val selectedCandidateIndexes = selectedPredicateIndexes(
      tableName, offeredPredicates, lookupBuilder.selectLookupPredicates(offeredPredicates.clone()))
    val selectedPredicates = selectedCandidateIndexes.map(candidates.apply)
    val selectedCandidateIndexSet = selectedCandidateIndexes.toSet
    candidates.zipWithIndex.foreach { case (candidate, index) =>
      if (!selectedCandidateIndexSet.contains(index)) residual += candidate.condition
    }

    val demandedLookupOutput = join.joinType match {
      case LeftSemi | LeftAnti => Seq.empty
      case _ => join.demandedRightOutput
    }
    val residualLookupReferences = AttributeSet(
      residual.flatMap(_.references).filter(join.right.outputSet.contains))
    val (prunedTransforms, requiredReferences) = pruneLookupTransforms(
      demandedLookupOutput ++ residualLookupReferences, remainingTransforms)
    val requiredInput = relation.output.filter(requiredReferences.contains)
    scanBuilder match {
      case builder: SupportsPushDownRequiredColumns =>
        builder.pruneColumns(requiredInput.toStructType)
      case _ =>
    }
    val scan = scanBuilder.build()
    val resolver = SQLConf.get.resolver
    val lookupInput = scan.readSchema().fields.map { field =>
      relation.output.find { attr =>
        resolver(attr.name, field.name) && attr.dataType == field.dataType
      }.getOrElse {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "UNKNOWN_SCAN_FIELD", Map("field" -> toSQLId(Seq(field.name))))
      }
    }.toSeq
    lookupInput.map(_.name).diff(lookupInput.map(_.name).distinct).headOption.foreach { name =>
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName, "DUPLICATE_SCAN_FIELD", Map("field" -> toSQLId(Seq(name))))
    }
    val externalLookup = scan.toExternalLookup()
    val maxParameterRowsPerRequest = externalLookup.maxParameterRowsPerRequest()
    if (maxParameterRowsPerRequest <= 0) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName,
        "NON_POSITIVE_MAX_PARAMETER_ROWS",
        Map("value" -> maxParameterRowsPerRequest.toString))
    }
    val readerFactory = externalLookup.createReaderFactory()
    val (reboundTransforms, lookupOutput) =
      rebindLookupTransforms(prunedTransforms, lookupInput)

    PreparedExternalLookupJoin(
      join.left,
      scan,
      readerFactory,
      maxParameterRowsPerRequest,
      lookupInput,
      lookupOutput,
      reboundTransforms,
      selectedPredicates.map(_.parameter).toSeq,
      selectedPredicates.map(_.nullSafe).toSeq,
      join.joinType,
      residual.reduceOption(And),
      join.cacheOptions)
  }

  /**
   * Maps the connector's selection back to candidate positions. Selections are identified by
   * object identity rather than by equality, so two candidates of the same shape, for example the
   * same lookup field compared with two different join-side values, remain distinguishable.
   */
  private def selectedPredicateIndexes(
      tableName: String,
      offered: Array[V2Predicate],
      selected: Array[V2Predicate]): Array[Int] = {
    if (selected == null || selected.contains(null)) {
      throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
        tableName, "NULL_PREDICATE_SELECTION")
    }
    if (selected.isEmpty) {
      throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
        "NO_LOOKUP_PREDICATE", Map("table" -> toSQLId(tableName)))
    }
    val offeredIndexes = new IdentityHashMap[V2Predicate, Integer]()
    offered.zipWithIndex.foreach { case (predicate, index) => offeredIndexes.put(predicate, index) }
    val seen = new IdentityHashMap[V2Predicate, java.lang.Boolean]()
    selected.map { predicate =>
      val index = offeredIndexes.get(predicate)
      if (index == null) {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "UNOFFERED_SELECTED_PREDICATE", Map("predicate" -> predicate.describe()))
      }
      if (seen.put(predicate, java.lang.Boolean.TRUE) != null) {
        throw QueryCompilationErrors.invalidDataSourceExternalLookupError(
          tableName, "DUPLICATE_SELECTED_PREDICATE", Map("predicate" -> predicate.describe()))
      }
      index.intValue()
    }
  }

  private case class OfferedLookupPredicate(
      parameter: Expression,
      projectedField: Attribute,
      condition: Expression,
      predicateName: String,
      nullSafe: Boolean)

  private def isLookupComparison(
      parameter: Expression,
      lookupField: Attribute,
      join: ExternalLookupJoin): Boolean = {
    parameter.references.subsetOf(join.left.outputSet) &&
      join.right.outputSet.contains(lookupField)
  }

  private case class ExtractedLookupPlan(
      relation: DataSourceV2Relation,
      transforms: Seq[ExternalLookupTransform])

  private def extractLookupPlan(plan: LogicalPlan): Option[ExtractedLookupPlan] = plan match {
    case relation: DataSourceV2Relation =>
      Some(ExtractedLookupPlan(relation, Seq.empty))
    case relation: ExternalLookupRelation =>
      Some(ExtractedLookupPlan(relation.relation, Seq.empty))
    case SubqueryAlias(_, child) => extractLookupPlan(child)
    case View(_, _, child, _) => extractLookupPlan(child)
    case Project(project, child) =>
      extractLookupPlan(child).map { extracted =>
        extracted.copy(
          transforms = extracted.transforms :+ ExternalLookupProject(project, child.output))
      }
    case Filter(condition, child) =>
      extractLookupPlan(child).map { extracted =>
        extracted.copy(
          transforms = extracted.transforms :+ ExternalLookupFilter(condition, child.output))
      }
    case _ => None
  }

  private def pushBaseFilters(
      scanBuilder: org.apache.spark.sql.connector.read.ScanBuilder,
      relation: DataSourceV2Relation,
      transforms: Seq[ExternalLookupTransform]): Seq[ExternalLookupTransform] = {
    val remaining = mutable.ArrayBuffer.empty[ExternalLookupTransform]
    var canPush = true
    transforms.foreach {
      case ExternalLookupFilter(condition, input) if canPush && input == relation.output =>
        val normalized = DataSourceStrategy.normalizeExprs(
          splitConjunctivePredicates(condition), relation.output)
        val postScan = PushDownUtils.pushFilters(
          scanBuilder, normalized, partitionFields = None)._2
        postScan.reduceOption(And).foreach { filter =>
          remaining += ExternalLookupFilter(filter, relation.output)
        }
      case transform =>
        canPush = false
        remaining += transform
    }
    remaining.toSeq
  }

  private def traceToRelation(
      relationOutput: Seq[Attribute],
      transforms: Seq[ExternalLookupTransform]): Map[ExprId, Option[Attribute]] = {
    var lineage: Map[ExprId, Option[Attribute]] =
      relationOutput.map(attr => attr.exprId -> Some(attr)).toMap
    transforms.foreach {
      case ExternalLookupFilter(_, _) =>
      case ExternalLookupProject(projectList, _) =>
        lineage = projectList.map { expression =>
          val source = expression match {
            case attr: Attribute => lineage.getOrElse(attr.exprId, None)
            case Alias(attr: Attribute, _) => lineage.getOrElse(attr.exprId, None)
            case _ => None
          }
          expression.toAttribute.exprId -> source
        }.toMap
    }
    lineage
  }

  private def pruneLookupTransforms(
      finalOutput: Seq[Attribute],
      transforms: Seq[ExternalLookupTransform]): (Seq[ExternalLookupTransform], AttributeSet) = {
    var required = AttributeSet(finalOutput)
    val retained = mutable.ArrayBuffer.empty[ExternalLookupTransform]
    transforms.reverseIterator.foreach {
      case filter @ ExternalLookupFilter(condition, _) =>
        required ++= condition.references
        retained.prepend(filter)
      case project @ ExternalLookupProject(projectList, _) =>
        val retainedProjects = projectList.filter(expr => required.contains(expr.toAttribute))
        required = AttributeSet(retainedProjects.flatMap(_.references))
        retained.prepend(project.copy(projectList = retainedProjects))
    }
    retained.toSeq -> required
  }

  private def rebindLookupTransforms(
      transforms: Seq[ExternalLookupTransform],
      lookupInput: Seq[Attribute]): (Seq[ExternalLookupTransform], Seq[Attribute]) = {
    var currentInput = lookupInput
    val rebound = transforms.map {
      case project: ExternalLookupProject =>
        val rebound = project.copy(input = currentInput)
        currentInput = project.projectList.map(_.toAttribute)
        rebound
      case filter: ExternalLookupFilter =>
        filter.copy(input = currentInput)
    }
    rebound -> currentInput
  }
}

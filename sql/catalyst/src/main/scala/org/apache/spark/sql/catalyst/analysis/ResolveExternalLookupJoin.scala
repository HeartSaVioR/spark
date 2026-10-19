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
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, StringLiteral}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupCacheOptions,
  ExternalLookupHint, ExternalLookupJoin, Join, JoinHint, LogicalPlan, SubqueryAlias,
  UnresolvedHint, UnresolvedWith, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExternalLookupRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/** Resolves the `external_lookup` option hint to an occurrence-local marker. */
object ResolveExternalLookupHint extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    ResolveExternalLookupJoin.resolveHints(plan)
}

/**
 * Replaces an eligible join with a lookup join holder.
 *
 * A join is planned as an external lookup join when its right side reads a table that supports
 * external lookup but not batch read, or when the right side carries an `EXTERNAL_LOOKUP` hint.
 * A table that supports both keeps the ordinary join strategies unless the hint opts in.
 */
object ResolveExternalLookupJoin extends Rule[LogicalPlan] with QueryErrorsBase {
  private val HintName = "EXTERNAL_LOOKUP"
  private val SupportedJoinTypes = Seq(Inner, LeftOuter, LeftSemi, LeftAnti)
  private val CacheEnabledOption = "cache_enabled"
  private val CacheMaxEntriesOption = "cache_max_entries"
  private val CacheTtlOption = "cache_ttl"
  private val CacheMaxRowsPerKeyOption = "cache_max_rows_per_key"
  private val SupportedOptions = Set(
    CacheEnabledOption,
    CacheMaxEntriesOption,
    CacheTtlOption,
    CacheMaxRowsPerKeyOption)
  private val NonNegativeDuration = "a non-negative duration, such as 500ms, 5s, or 10m"

  private[analysis] def resolveHints(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUp {
      case UnresolvedHint(name, parameters, child)
          if name.toUpperCase(Locale.ROOT) == HintName =>
        resolveHint(parameters, child)
    }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case Join(left, _, _, _, _) if containsLookupHint(left) =>
      throw QueryCompilationErrors.invalidExternalLookupHintError("NOT_ON_LOOKUP_SIDE")

    case join @ Join(left, right, joinType, condition, hint)
        if left.resolved && right.resolved && shouldConsiderLookup(right) =>
      val markers = right.collect { case marker: ExternalLookupHint => marker }
      if (markers.length > 1) {
        throw QueryCompilationErrors.invalidExternalLookupHintError("MULTIPLE_HINTS")
      }
      val hasLookupOptions = markers.nonEmpty
      val lookupPlan = right.transformDown {
        case marker: ExternalLookupHint => marker.child
      }
      if (hasJoinStrategyHint(hint, left, right)) {
        join.copy(right = lookupPlan)
      } else if (condition.isEmpty) {
        if (hasLookupOptions) {
          throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
            "MISSING_JOIN_CONDITION")
        }
        join
      } else if (!SupportedJoinTypes.contains(joinType)) {
        if (hasLookupOptions) {
          throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
            "JOIN_TYPE",
            Map(
              "joinType" -> toSQLStmt(joinType.sql),
              "supportedJoinTypes" -> SupportedJoinTypes.map(t => toSQLStmt(t.sql)).mkString(", ")))
        }
        join.copy(right = lookupPlan)
      } else if (right.isStreaming) {
        if (hasLookupOptions) {
          throw QueryCompilationErrors.unsupportedExternalLookupJoinError("STREAMING_LOOKUP_SIDE")
        }
        join.copy(right = lookupPlan)
      } else {
        val cacheOptions = markers.headOption.map(_.cacheOptions)
          .getOrElse(defaultCacheOptions)
        ExternalLookupJoin(
          left,
          lookupPlan,
          joinType,
          condition,
          cacheOptions,
          lookupPlan.output)
      }
  }

  private def containsLookupHint(plan: LogicalPlan): Boolean =
    plan.exists(_.isInstanceOf[ExternalLookupHint])

  private def shouldConsiderLookup(plan: LogicalPlan): Boolean =
    containsLookupHint(plan) || plan.exists {
      case relation: DataSourceV2Relation => isLookupOnly(relation.table)
      case relation: ExternalLookupRelation => isLookupOnly(relation.table)
      case _ => false
    }

  private def isLookupOnly(table: Table): Boolean = {
    val capabilities = table.capabilities()
    capabilities.contains(EXTERNAL_LOOKUP) && !capabilities.contains(BATCH_READ)
  }

  private def hasJoinStrategyHint(
      hint: JoinHint,
      left: LogicalPlan,
      right: LogicalPlan): Boolean = {
    // Only the hints that EliminateResolvedHint will attach to this join count; a strategy hint
    // on a nested join belongs to that join.
    hint.leftHint.exists(_.strategy.isDefined) ||
      hint.rightHint.exists(_.strategy.isDefined) ||
      Seq(left, right).exists { side =>
        EliminateResolvedHint.extractHintsFromPlan(side)._2.exists(_.strategy.isDefined)
      }
  }

  private def resolveHint(parameters: Seq[Expression], child: LogicalPlan): LogicalPlan = {
    val relationNames = mutable.ArrayBuffer.empty[Seq[String]]
    var cacheOptions = defaultCacheOptions
    val seenOptions = mutable.HashSet.empty[String]
    parameters.foreach {
      case StringLiteral(value) =>
        cacheOptions = parseOption(value, cacheOptions, seenOptions)
      case Literal(value: String, _) =>
        cacheOptions = parseOption(value, cacheOptions, seenOptions)
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
          join.copy(right = ExternalLookupHint(join.right, cacheOptions))
      }
      if (wrappedJoin) rewritten else ExternalLookupHint(child, cacheOptions)
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
          ExternalLookupHint(relation, cacheOptions)
        case alias: SubqueryAlias
            if matches(alias.identifier.qualifier :+ alias.identifier.name) =>
          ExternalLookupHint(alias, cacheOptions)
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

  private def defaultCacheOptions: ExternalLookupCacheOptions = {
    val conf = SQLConf.get
    ExternalLookupCacheOptions(
      enabled = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_ENABLED),
      maxEntries = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_MAX_ENTRIES),
      ttlMillis = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_TTL),
      maxRowsPerKey = conf.getConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_MAX_ROWS_PER_KEY))
  }

  private def parseOption(
      option: String,
      cacheOptions: ExternalLookupCacheOptions,
      seenOptions: mutable.Set[String]): ExternalLookupCacheOptions = {
    val separator = option.indexOf('=')
    if (separator <= 0 || separator == option.length - 1) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(option, "INVALID_FORMAT")
    }
    val name = option.substring(0, separator).trim.toLowerCase(Locale.ROOT)
    val value = option.substring(separator + 1).trim
    if (!SupportedOptions.contains(name)) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(
        name,
        "UNSUPPORTED",
        Map("supportedOptions" -> SupportedOptions.toSeq.sorted.map(toDSOption).mkString(", ")))
    }
    if (!seenOptions.add(name)) {
      throw QueryCompilationErrors.invalidExternalLookupOptionError(name, "DUPLICATE")
    }

    name match {
      case CacheEnabledOption =>
        value.toLowerCase(Locale.ROOT) match {
          case "true" => cacheOptions.copy(enabled = true)
          case "false" => cacheOptions.copy(enabled = false)
          case _ => invalidOptionValue(CacheEnabledOption, "true or false")
        }
      case CacheMaxEntriesOption =>
        cacheOptions.copy(maxEntries = parsePositiveInt(CacheMaxEntriesOption, value))
      case CacheTtlOption =>
        cacheOptions.copy(ttlMillis = parseNonNegativeDurationMillis(CacheTtlOption, value))
      case CacheMaxRowsPerKeyOption =>
        cacheOptions.copy(maxRowsPerKey = parsePositiveInt(CacheMaxRowsPerKeyOption, value))
    }
  }

  private def parsePositiveInt(optionName: String, value: String): Int = {
    val parsed = try {
      value.toInt
    } catch {
      case NonFatal(_) => invalidOptionValue(optionName, "a positive integer")
    }
    if (parsed <= 0) invalidOptionValue(optionName, "a positive integer")
    parsed
  }

  private def parseNonNegativeDurationMillis(optionName: String, value: String): Long = {
    val parsed = try {
      Utils.timeStringAsMs(value)
    } catch {
      case NonFatal(_) => invalidOptionValue(optionName, NonNegativeDuration)
    }
    if (parsed < 0) invalidOptionValue(optionName, NonNegativeDuration)
    parsed
  }

  private def invalidOptionValue(optionName: String, expected: String): Nothing = {
    throw QueryCompilationErrors.invalidExternalLookupOptionError(
      optionName, "INVALID_VALUE", Map("expected" -> expected))
  }
}

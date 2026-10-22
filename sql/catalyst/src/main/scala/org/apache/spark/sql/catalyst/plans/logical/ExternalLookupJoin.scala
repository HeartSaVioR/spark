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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression,
  NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.connector.read.{ExternalLookupReaderFactory, Scan}

case class ExternalLookupCacheOptions(
    enabled: Boolean,
    maxEntries: Int,
    ttlMillis: Long,
    maxRowsPerKey: Int)

/** Occurrence-local external lookup options produced by resolving the `external_lookup` hint. */
case class ExternalLookupHint(
    child: LogicalPlan,
    cacheOptions: ExternalLookupCacheOptions) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): ExternalLookupHint =
    copy(child = newChild)
}

/** A lookup join retained through ordinary expression and operator optimization. */
case class ExternalLookupJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression],
    cacheOptions: ExternalLookupCacheOptions,
    demandedRightOutput: Seq[Attribute]) extends BinaryNode {

  override def output: Seq[Attribute] = joinType match {
    case Inner => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case LeftSemi | LeftAnti => left.output
    case _ => left.output ++ right.output
  }

  override def maxRows: Option[Long] = joinType match {
    case LeftSemi | LeftAnti => left.maxRows
    case _ => None
  }

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): ExternalLookupJoin = copy(left = newLeft, right = newRight)
}

/** One optimized lookup-side operation, retained in evaluation order. */
sealed trait ExternalLookupTransform {
  def input: Seq[Attribute]
  def output: Seq[Attribute]
}

case class ExternalLookupProject(
    projectList: Seq[NamedExpression],
    input: Seq[Attribute]) extends ExternalLookupTransform {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class ExternalLookupFilter(
    condition: Expression,
    input: Seq[Attribute]) extends ExternalLookupTransform {
  override def output: Seq[Attribute] = input
}

/** A lookup join after its optimized lookup side has been converted to an external operation. */
case class PreparedExternalLookupJoin(
    child: LogicalPlan,
    @transient scan: Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    lookupTransforms: Seq[ExternalLookupTransform],
    lookupParameters: Seq[Expression],
    nullSafeParameters: Seq[Boolean],
    joinType: JoinType,
    residualCondition: Option[Expression],
    cacheOptions: ExternalLookupCacheOptions) extends UnaryNode {

  override def output: Seq[Attribute] = joinType match {
    case Inner => child.output ++ lookupOutput
    case LeftOuter => child.output ++ lookupOutput.map(_.withNullability(true))
    case LeftSemi | LeftAnti => child.output
    case _ => child.output ++ lookupOutput
  }

  override def references: AttributeSet =
    AttributeSet(lookupParameters.flatMap(_.references)) ++
      residualCondition.map(_.references.intersect(child.outputSet))
        .getOrElse(AttributeSet.empty)

  override protected def withNewChildInternal(newChild: LogicalPlan): PreparedExternalLookupJoin =
    copy(child = newChild)

  // Connector descriptions may contain secrets. The physical node renders a redacted description.
  override protected def stringArgs: Iterator[Any] = Iterator(child)
}

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

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXTERNAL_LOOKUP_JOIN

/**
 * Prunes the columns of an [[ExternalLookupJoin]], which [[ColumnPruning]] does not handle
 * because it is not a `Join`. It runs while the lookup side is still an ordinary logical plan,
 * and narrows `requiredLookupOutput` to what the parent needs, so that
 * `PrepareExternalLookupJoin` prunes the lookup scan to it.
 */
object PruneExternalLookupJoinColumns extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsPattern(EXTERNAL_LOOKUP_JOIN)) {
      case p @ Project(_, j: ExternalLookupJoin) =>
        val required = p.references ++ j.references
        val requiredLookupOutput = j.requiredLookupOutput.filter(p.references.contains)
        p.copy(child = j.copy(
          left = prunedChild(j.left, required),
          right = prunedChild(j.right, required),
          requiredLookupOutput = requiredLookupOutput))
      case j: ExternalLookupJoin if j.joinType == LeftSemi || j.joinType == LeftAnti =>
        j.copy(
          right = prunedChild(j.right, j.references),
          requiredLookupOutput = Seq.empty)
    }

  private def prunedChild(child: LogicalPlan, references: AttributeSet): LogicalPlan =
    if (!child.outputSet.subsetOf(references)) {
      Project(child.output.filter(references.contains), child)
    } else {
      child
    }
}

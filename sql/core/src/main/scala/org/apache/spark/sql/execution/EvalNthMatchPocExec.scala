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
package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, JoinedRow, NthMatch, Predicate, Projection, SafeProjection}
import org.apache.spark.sql.catalyst.trees.TreePattern.NTH_MATCH
import org.apache.spark.unsafe.types.UTF8String

case class EvalNthMatchPocExec(
    predicate: Expression,
    child: SparkPlan)
  extends UnaryExecNode {

  import EvalNthMatchPocExec._

  // In actual implementation, this should be state store.
  private val matchedRows: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer[InternalRow]()

  // FIXME: added testing data for now
  private def addTestData(employeeName: String, organization: String): Unit = {
    matchedRows.append(
      InternalRow(
        UTF8String.fromString(employeeName),
        UTF8String.fromString(organization)
      )
    )
  }

  addTestData("Hello", "lakestore")
  addTestData("World", "streaming")
  addTestData("Joe", "pyspark")
  addTestData("Hello", "sql")

  private val referenceToMatchedRows = mutable.HashMap[MatchedRowsReference, ReferenceResolution]()

  private val predicateResolved = predicate.transformUpWithPruning(
    _.containsAnyPattern(NTH_MATCH)) {

    case NthMatch(input: AttributeReference, offset) =>
      val resolution = referenceToMatchedRows.getOrElseUpdate(
        MatchedRowsReference(input, offset), {

          val attr = AttributeReference(s"__${offset}_th_${input.name}", input.dataType,
            input.nullable)()
          ReferenceResolution(None, attr)
      })

      resolution.attribute
    }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      // Have to create projection inside each partition as it's not serializable, but should not
      // create the projection per row as it could be heavyweight (may incur codegen)
      referenceToMatchedRows.foreach { case (reference, resolution) =>
        val proj = SafeProjection.create(Seq(reference.input), child.output)
        referenceToMatchedRows.put(reference, resolution.copy(projection = Some(proj)))
      }

      logWarning(s"DEBUG: predicateResolved = $predicateResolved")
      logWarning(s"DEBUG: referenceToMatchedRows = $referenceToMatchedRows")

      def constructAttrToValue(references: Seq[MatchedRowsReference]): Seq[(Attribute, AnyRef)] = {
        references.map { reference =>
          val resolution = referenceToMatchedRows(reference)
          // index in SQL starts from 1
          val matchedRow = matchedRows(reference.offset - 1)
          val colValue = resolution.projection.get.apply(matchedRow)
            .get(0, reference.input.dataType)

          resolution.attribute -> colValue
        }
      }

      // Need to build a consistent order of elements, so that we can just update the lookup row
      // without changing predicate, which could be also heavyweight on initializing.
      val references = referenceToMatchedRows.keys.toList

      // NOTE: attributes does not change, only values could change, depending on updates of
      // matchedRows
      val attrToValue = constructAttrToValue(references)
      val lookupAttrs = attrToValue.map(_._1)
      val fullSchema = child.output ++ lookupAttrs
      val finalPredicate = Predicate.create(predicateResolved, fullSchema)

      logWarning(s"DEBUG: final predicate: $finalPredicate")

      // purposed to reuse per partition
      val joinRow = new JoinedRow()

      var lookupRow = InternalRow(attrToValue.map(_._2): _*)

      iter.filter { row =>
        joinRow.withLeft(row)

        logWarning(s"DEBUG: attrToValue = $attrToValue")
        logWarning(s"DEBUG: fullSchema = $fullSchema")

        // TODO: We should update lookupRow if matchedRows has changed in the loop. Either simply
        //  invalidate whenever matchedRows has changed, or smart enough to detect whether the
        //  matched rows in offsets are changed and invalidate if any of them is changed.

        joinRow.withRight(lookupRow)

        finalPredicate.eval(joinRow)
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object EvalNthMatchPocExec {
  private case class MatchedRowsReference(input: Expression, offset: Int)

  private case class ReferenceResolution(projection: Option[Projection], attribute: Attribute)
}

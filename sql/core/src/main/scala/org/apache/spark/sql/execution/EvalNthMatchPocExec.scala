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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, NthMatch, SafeProjection}
import org.apache.spark.sql.catalyst.trees.TreePattern.NTH_MATCH
import org.apache.spark.unsafe.types.UTF8String

case class EvalNthMatchPocExec(
    predicate: Expression,
    child: SparkPlan)
  extends UnaryExecNode {

  private val matchedRows: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer[InternalRow]()

  // FIXME: added testing data
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

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      logWarning(s"DEBUG: predicate: $predicate")

      val predicateResolved = predicate
        .transformUpWithPruning(_.containsAnyPattern(NTH_MATCH)) {

        case NthMatch(input, offset) =>
          val proj = SafeProjection.create(Seq(input), child.output)
          // TODO: this should refer to the N-th matched row, probably reading from state store.
          // TODO: Need to decide if there is less matched rows than the offset. null?
          // offset - 1, as index in SQL function mostly starts from 1
          val matchedRow = matchedRows(offset - 1)

          val colVal = proj.apply(matchedRow).get(0, input.dataType)

          // replace the function call with column value as literal
          Literal(colVal)
      }

      logWarning(s"DEBUG: predicateResolved = $predicateResolved")

      iter.filter { row =>
        // FIXME: This is just a simplified example - in practice, predicateResolved has to be
        //  actually changed whenever we add the match here...
        //  Could we make the predicate as parameterized? Then we could build an association
        //  between param name and the offset and column name we need to retrieve, and do the
        //  thing for every input without having to replace the function call with literal
        //  in the predicate.
        predicateResolved.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

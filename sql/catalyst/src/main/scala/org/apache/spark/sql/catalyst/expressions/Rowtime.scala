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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.TreePattern.{ROWTIME, TreePattern}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyTimestampType, DataType, StringType}
import org.apache.spark.unsafe.types.CalendarInterval

case class Rowtime(rowtimeExpr: Expression, delay: CalendarInterval)
  extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  def this(rowtimeExpr: Expression, delay: Expression) = {
    this(rowtimeExpr, Rowtime.parseExpression(delay))
  }

  def this(rowtimeExpr: Expression, delay: String) = {
    this(rowtimeExpr, Rowtime.parseExpression(delay))
  }

  override def child: Expression = rowtimeExpr

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyTimestampType)

  override def dataType: DataType = child.dataType

  override def prettyName: String = "rowtime"

  final override val nodePatterns: Seq[TreePattern] = Seq(ROWTIME)

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  override protected def withNewChildInternal(newChild: Expression): Rowtime =
    copy(rowtimeExpr = newChild)
}

object Rowtime {
  /**
   * Parses the duration expression to generate the long value for the original constructor so
   * that we can use `window` in SQL.
   */
  def parseExpression(expr: Expression): CalendarInterval = expr match {
    case NonNullLiteral(s, StringType) => parseExpression(s.toString)
    case _ =>
      // FIXME: another error message
      throw QueryCompilationErrors.invalidLiteralForWindowDurationError()
  }

  def parseExpression(expr: String): CalendarInterval = IntervalUtils.fromIntervalString(expr)
}

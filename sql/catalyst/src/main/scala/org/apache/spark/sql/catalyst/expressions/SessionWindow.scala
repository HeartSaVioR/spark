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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.util.{DateTimeConstants, IntervalUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class SessionWindow(timeColumn: Expression, gapDuration: Long) extends UnaryExpression
  with ImplicitCastInputTypes
  with Unevaluable
  with NonSQLExpression {

  //////////////////////////
  // SQL Constructors
  //////////////////////////

  def this(timeColumn: Expression, gapDuration: Expression) = {
    this(timeColumn, SessionWindow.parseExpression(gapDuration))
  }

  override def child: Expression = timeColumn
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
  override def dataType: DataType = new StructType()
    .add(StructField("start", TimestampType))
    .add(StructField("end", TimestampType))

  // This expression is replaced in the analyzer.
  override lazy val resolved = false

  /** Validate the inputs for the gap duration in addition to the input data type. */
  override def checkInputDataTypes(): TypeCheckResult = {
    val dataTypeCheck = super.checkInputDataTypes()
    if (dataTypeCheck.isSuccess) {
      if (gapDuration <= 0) {
        return TypeCheckFailure(s"The window duration ($gapDuration) must be greater than 0.")
      }
    }
    dataTypeCheck
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(timeColumn = newChild)
}

object SessionWindow {
  val marker = "spark.sessionWindow"

  /**
   * Parses the interval string for a valid time duration. CalendarInterval expects interval
   * strings to start with the string `interval`. For usability, we prepend `interval` to the string
   * if the user omitted it.
   *
   * @param interval The interval string
   * @return The interval duration in microseconds. SparkSQL casts TimestampType has microsecond
   *         precision.
   */
  private def getIntervalInMicroSeconds(interval: String): Long = {
    val cal = IntervalUtils.stringToInterval(UTF8String.fromString(interval))
    if (cal.months != 0) {
      throw new IllegalArgumentException(
        s"Intervals greater than a month is not supported ($interval).")
    }
    cal.days * DateTimeConstants.MICROS_PER_DAY + cal.microseconds
  }

  /**
   * Parses the duration expression to generate the long value for the original constructor so
   * that we can use `window` in SQL.
   */
  private def parseExpression(expr: Expression): Long = expr match {
    case NonNullLiteral(s, StringType) => getIntervalInMicroSeconds(s.toString)
    case IntegerLiteral(i) => i.toLong
    case NonNullLiteral(l, LongType) => l.toString.toLong
    case _ => throw new AnalysisException("The duration and time inputs to window must be " +
      "an integer, long or string literal.")
  }

  def apply(
      timeColumn: Expression,
      gapDuration: String): SessionWindow = {
    SessionWindow(timeColumn,
      getIntervalInMicroSeconds(gapDuration))
  }
}

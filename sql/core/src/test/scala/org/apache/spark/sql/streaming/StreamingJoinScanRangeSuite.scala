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

package org.apache.spark.sql.streaming

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper
import org.apache.spark.sql.types.TimestampType

class StreamingJoinScanRangeSuite extends SparkFunSuite {
  private val leftTime = AttributeReference("leftTime", TimestampType)()
  private val expiry = AttributeReference("expiry", TimestampType)()
  private val rightTime = AttributeReference("rightTime", TimestampType)()

  test("scan bounds use the incoming expiry and constrain only the stored index column") {
    val condition = Some(rightTime >= leftTime && rightTime <= expiry)
    val scanRight = StreamingSymmetricHashJoinHelper.getJoinScanRange(
      Seq(leftTime, expiry), rightTime, condition).get
    val scanLeft = StreamingSymmetricHashJoinHelper.getJoinScanRange(
      Seq(rightTime), leftTime, condition).get

    assert(scanRight(InternalRow(100000000L, 110000000L)) == (100000000L, 110000000L))
    assert(scanRight(InternalRow(100000000L, 120000000L)) == (100000000L, 120000000L))
    assert(scanLeft(InternalRow(105000000L)) == (Long.MinValue, 105000000L))
  }
}

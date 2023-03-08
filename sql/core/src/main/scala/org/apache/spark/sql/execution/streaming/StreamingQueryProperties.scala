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

package org.apache.spark.sql.execution.streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.util.Clock

/**
 * Class containing properties which are available at the start of streaming query run, and does
 * not change during query run.
 */
case class StreamingQueryProperties(
    id: UUID,
    runId: UUID,
    name: String,
    triggerClock: Clock,
    sparkSession: SparkSession,
    sink: Table)

/**
 * Class containing properties which are available after initialization of the streaming query,
 * and does not change during query run.
 */
case class StreamingQueryPlanProperties(logicalPlan: LogicalPlan, sources: Seq[SparkDataStream])

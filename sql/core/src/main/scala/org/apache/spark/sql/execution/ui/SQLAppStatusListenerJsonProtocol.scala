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

package org.apache.spark.sql.execution.ui

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.JobExecutionStatus
import org.apache.spark.util.JsonUtils._

/**
 * Serializes LiveEntity instances to/from JSON. This protocol provides strong backwards-
 * and forwards-compatibility guarantees: any version of Spark should be able to read JSON output
 * written by any other version, including newer versions.
 *
 * SQLAppStatusListenerJsonProtocolSuite contains backwards-compatibility tests which check that
 * the current version of SQLLiveEntityJsonProtocol is able to read output written by
 * earlier versions.
 * We do not currently have tests for reading newer JSON output with older Spark versions.
 *
 * To ensure that we provide these guarantees, follow these rules when modifying these methods:
 *
 *  - Never delete any JSON fields.
 *  - Any new JSON fields should be optional; use `jsonOption` when reading these fields
 *    in `*FromJson` methods.
 */
private[sql] object SQLAppStatusListenerJsonProtocol {
  private implicit val format = DefaultFormats

  // FIXME: check how JsonProtocolSuite tests JsonProtocol and do the same thing

  def stateToJson(listener: SQLAppStatusListener): JValue = {
    val liveExecutions = mapToImmutableMap(listener.liveExecutions) { case (key, value) =>
      key.toString -> liveExecutionDataToJson(value)
    }
    val stageMetrics = mapToImmutableMap(listener.stageMetrics) { case (key, value) =>
      key.toString -> liveStageMetricsToJson(value)
    }
    ("liveExecutions" -> liveExecutions) ~ ("stageMetrics" -> stageMetrics)
  }

  def restoreStateFromJson(json: JValue, listener: SQLAppStatusListener): Unit = {
    listener.liveExecutions.clear()
    listener.stageMetrics.clear()

    (json \ "liveExecutions").extract[Map[String, JValue]].foreach { case (key, value) =>
      listener.liveExecutions.put(key.toLong, liveExecutionDataFromJson(value))
    }
    (json \ "stageMetrics").extract[Map[String, JValue]].foreach { case (key, value) =>
      listener.stageMetrics.put(key.toInt, liveStageMetricsFromJson(value))
    }
  }

  def liveExecutionDataToJson(execution: LiveExecutionData): JValue = {
    val completionTime = optionDateToJson(execution.completionTime)
    val jobs = execution.jobs.map { entry => entry._1.toString -> entry._2.name() }
    val driverAccumUpdates = mapToStringKeyImmutableMap(execution.driverAccumUpdates)
    val metricsValues = Option(execution.metricsValues).map(mapToStringKeyImmutableMap)
    ("executionId" -> execution.executionId) ~
      ("description" -> execution.description) ~
      ("details" -> execution.details) ~
      ("physicalPlanDescription" -> execution.physicalPlanDescription) ~
      ("metrics" -> execution.metrics.map(sqlPlanMetricToJson)) ~
      ("submissionTime" -> execution.submissionTime) ~
      ("completionTime" -> completionTime) ~
      ("jobs" -> jobs) ~
      ("stages" -> execution.stages) ~
      ("driverAccumUpdates" -> driverAccumUpdates) ~
      ("metricsValues" -> metricsValues) ~
      ("endEvents" -> execution.endEvents)
  }

  def liveExecutionDataFromJson(json: JValue): LiveExecutionData = {
    val execution = new LiveExecutionData((json \ "executionId").extract[Long])
    execution.description = (json \ "description").extract[String]
    execution.details = (json \ "details").extract[String]
    execution.physicalPlanDescription = (json \ "physicalPlanDescription").extract[String]
    execution.metrics = (json \ "metrics").extract[Seq[JValue]].map(sqlPlanMetricFromJson)
    execution.submissionTime = (json \ "submissionTime").extract[Long]
    execution.completionTime = jsonOption(json \ "completionTime").map(_.extract[Long])
      .map(new Date(_))

    val jobs = (json \ "jobs").extract[Map[String, String]]
        .map { entry => entry._1.toInt -> JobExecutionStatus.fromString(entry._2) }
    execution.jobs = jobs

    execution.stages = (json \ "stages").extract[Set[String]].map(_.toInt)

    val driverAccumUpdates = (json \ "driverAccumUpdates").extract[Map[String, Long]]
      .map { entry => entry._1.toLong -> entry._2 }
    execution.driverAccumUpdates = driverAccumUpdates

    val metricsValue = (json \ "metricsValues").extract[Option[Map[String, String]]]
      .map { (m: Map[String, String]) => m.map { case (key, value) =>
        key.toLong -> value
      }
      }.orNull
    execution.metricsValues = metricsValue
    execution.endEvents = (json \ "endEvents").extract[Int]

    execution
  }

  def sqlPlanMetricToJson(metric: SQLPlanMetric): JValue = {
    ("name" -> metric.name) ~
      ("accumulatorId" -> metric.accumulatorId) ~
      ("metricType" -> metric.metricType)
  }

  def sqlPlanMetricFromJson(json: JValue): SQLPlanMetric = {
    SQLPlanMetric(
      (json \ "name").extract[String],
      (json \ "accumulatorId").extract[Long],
      (json \ "metricType").extract[String])
  }

  def liveStageMetricsToJson(metrics: LiveStageMetrics): JValue = {
    val taskMetrics = mapToImmutableMap(metrics.taskMetrics) { case (key, value) =>
      key.toString -> liveTaskMetricsToJson(value)
    }
    ("stageId" -> metrics.stageId) ~
      ("attemptId" -> metrics.attemptId) ~
      ("accumulatorIds" -> metrics.accumulatorIds) ~
      ("taskMetrics" -> taskMetrics)
  }

  def liveStageMetricsFromJson(json: JValue): LiveStageMetrics = {
    val taskMetrics = new ConcurrentHashMap[Long, LiveTaskMetrics]()
    (json \ "taskMetrics").extract[Map[String, JValue]].foreach { case (key, value) =>
      taskMetrics.put(key.toInt, liveTaskMetricsFromJson(value))
    }
    new LiveStageMetrics(
      (json \ "stageId").extract[Int],
      (json \ "attemptId").extract[Int],
      (json \ "accumulatorIds").extract[Set[Long]],
      taskMetrics)
  }

  def liveTaskMetricsToJson(metrics: LiveTaskMetrics): JValue = {
    ("ids" -> metrics.ids.toSeq) ~
      ("values" -> metrics.values.toSeq) ~
      ("succeeded" -> metrics.succeeded)
  }

  def liveTaskMetricsFromJson(json: JValue): LiveTaskMetrics = {
    new LiveTaskMetrics(
      (json \ "ids").extract[Seq[Long]].toArray,
      (json \ "values").extract[Seq[Long]].toArray,
      (json \ "succeeded").extract[Boolean])
  }
}

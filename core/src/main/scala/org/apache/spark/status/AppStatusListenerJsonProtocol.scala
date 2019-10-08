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

package org.apache.spark.status

import java.util.Date

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.JobExecutionStatus
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.status.api.v1
import org.apache.spark.util.JsonProtocol

/**
 * Serializes LiveEntity instances to/from JSON. This protocol provides strong backwards-
 * and forwards-compatibility guarantees: any version of Spark should be able to read JSON output
 * written by any other version, including newer versions.
 *
 * LiveEntityJsonProtocolSuite contains backwards-compatibility tests which check that the current
 * version of LiveEntityJsonProtocol is able to read output written by earlier versions.
 * We do not currently have tests for reading newer JSON output with older Spark versions.
 *
 * To ensure that we provide these guarantees, follow these rules when modifying these methods:
 *
 *  - Never delete any JSON fields.
 *  - Any new JSON fields should be optional; use `jsonOption` when reading these fields
 *    in `*FromJson` methods.
 */
private[spark] object AppStatusListenerJsonProtocol {
  private implicit val format = DefaultFormats

  // FIXME: check how JsonProtocolSuite tests JsonProtocol and do the same thing

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def appStatusListenerStateToJson(listener: AppStatusListener): JValue = {
    import scala.collection.JavaConverters._
    val liveStages = listener.liveStages.entrySet().asScala.map { entry =>
      s"${entry.getKey._1}:${entry.getKey._2}" -> liveStageToJson(entry.getValue())
    }.toMap
    val liveJobs = listener.liveJobs.map { case (key, value) =>
      key.toString -> liveJobToJson(value)
    }.toMap
    val liveExecutors = listener.liveExecutors.mapValues(liveExecutorToJson).toMap
    val deadExecutors = listener.deadExecutors.mapValues(liveExecutorToJson).toMap
    val liveTasks = listener.liveTasks.map { case (key, value) =>
      key.toString -> liveTaskToJson(value)
    }.toMap
    val liveRDDs = listener.liveRDDs.map { case (key, value) =>
      key.toString -> liveRddToJson(value)
    }.toMap
    val pools = listener.pools.mapValues(schedulerPoolToJson).toMap

    ("liveStages" -> liveStages) ~
      ("liveJobs" -> liveJobs) ~
      ("liveExecutors" -> liveExecutors) ~
      ("deadExecutors" -> deadExecutors) ~
      ("liveTasks" -> liveTasks) ~
      ("liveRDDs" -> liveRDDs) ~
      ("pools" -> pools) ~
      ("activeExecutorCount" -> listener.activeExecutorCount)
  }

  def restoreAppStatusListenerStateFromJson(json: JValue, listener: AppStatusListener): Unit = {
    listener.liveStages.clear()
    listener.liveJobs.clear()
    listener.liveExecutors.clear()
    listener.deadExecutors.clear()
    listener.liveTasks.clear()
    listener.liveRDDs.clear()
    listener.pools.clear()

    (json \ "liveStages").extract[Map[String, JValue]].foreach { case (key, value) =>
      val keys = key.split(":")
      listener.liveStages.put((keys(0).toInt, keys(1).toInt), liveStageFromJson(value))
    }
    listener.liveJobs ++= (json \ "liveJobs").extract[Map[String, JValue]]
      .map { case (key, value) => key.toInt -> liveJobFromJson(value) }
    listener.liveExecutors ++= (json \ "liveExecutors").extract[Map[String, JValue]]
      .map { case (key, value) => key -> liveExecutorFromJson(value) }
    listener.deadExecutors ++= (json \ "deadExecutors").extract[Map[String, JValue]]
      .map { case (key, value) => key -> liveExecutorFromJson(value) }
    listener.liveTasks ++= (json \ "liveTasks").extract[Map[String, JValue]]
      .map { case (key, value) => key.toLong -> liveTaskFromJson(value) }
    listener.liveRDDs ++= (json \ "liveRDDs").extract[Map[String, JValue]]
      .map { case (key, value) => key.toInt -> liveRddFromJson(value) }
    listener.pools ++= (json \ "pools").extract[Map[String, JValue]]
      .map { case (key, value) => key -> schedulerPoolFromJson(value) }
    listener.activeExecutorCount = (json \ "activeExecutorCount").extract[Int]
  }

  def liveJobToJson(job: LiveJob): JValue = {
    val submissionTime = job.submissionTime.map(_.getTime).map(JLong(_)).getOrElse(JNothing)
    val jobGroup = job.jobGroup.map(JString(_)).getOrElse(JNothing)
    val sqlExecutionId = job.sqlExecutionId.map(JLong(_)).getOrElse(JNothing)
    val completedIndices = mutable.HashSet[Long]()
    completedIndices ++= job.completedIndices.iterator
    val completionTime = job.completionTime.map(_.getTime).map(JLong(_)).getOrElse(JNothing)

    ("jobId" -> job.jobId) ~
      ("name" -> job.name) ~
      ("submissionTime" -> submissionTime) ~
      ("stageIds" -> job.stageIds) ~
      ("jobGroup" -> jobGroup) ~
      ("numTasks" -> job.numTasks) ~
      ("sqlExecutionId" -> sqlExecutionId) ~
      ("activeTasks" -> job.activeTasks) ~
      ("completedTasks" -> job.completedTasks) ~
      ("failedTasks" -> job.failedTasks) ~
      ("completedIndices" -> completedIndices) ~
      ("killedTasks" -> job.killedTasks) ~
      ("killedSummary" -> job.killedSummary) ~
      ("skippedTasks" -> job.skippedTasks) ~
      ("skippedStages" -> job.skippedStages) ~
      ("status" -> job.status.name()) ~
      ("completionTime" -> completionTime) ~
      ("completedStages" -> job.completedStages) ~
      ("activeStages" -> job.activeStages) ~
      ("failedStages" -> job.failedStages)
  }

  def liveJobFromJson(json: JValue): LiveJob = {
    val liveJob = new LiveJob(
      (json \ "jobId").extract[Int],
      (json \ "name").extract[String],
      jsonOption(json \ "submissionTime").map(_.extract[Long]).map(new Date(_)),
      (json \ "stageIds").extract[Seq[Int]],
      jsonOption(json \ "jobGroup").map(_.extract[String]),
      (json \ "numTasks").extract[Int],
      jsonOption(json \ "sqlExecutionId").map(_.extract[Long])
    )
    liveJob.activeTasks = (json \ "activeTasks").extract[Int]
    liveJob.completedTasks = (json \ "completedTasks").extract[Int]
    liveJob.failedTasks = (json \ "failedTasks").extract[Int]
    val completedIndices = (json \ "completedIndices").extract[Set[Long]]
    completedIndices.foreach(liveJob.completedIndices.add)
    liveJob.killedTasks = (json \ "killedTasks").extract[Int]
    liveJob.killedSummary = (json \ "killedSummary").extract[Map[String, Int]]
    liveJob.skippedTasks = (json \ "skippedTasks").extract[Int]
    liveJob.skippedStages = (json \ "skippedStages").extract[Set[Int]]
    liveJob.status = JobExecutionStatus.fromString((json \ "status").extract[String])
    liveJob.completionTime = jsonOption(json \ "completionTime").map(_.extract[Long])
      .map(new Date(_))
    liveJob.completedStages = (json \ "completedStages").extract[Set[Int]]
    liveJob.activeStages = (json \ "activeStages").extract[Int]
    liveJob.failedStages = (json \ "failedStages").extract[Int]
    liveJob
  }

  def liveStageToJson(stage: LiveStage): JValue = {
    val description = stage.description.map(JString(_)).getOrElse(JNothing)
    val completedIndices = mutable.HashSet[Int]()
    completedIndices ++= stage.completedIndices.iterator
    val executorSummaries = stage.executorSummaries.mapValues(liveExecutorStageSummaryToJson).toMap

    ("jobs" -> stage.jobs.map(liveJobToJson)) ~
      ("jobIds" -> stage.jobIds) ~
      ("info" -> JsonProtocol.stageInfoToJson(stage.info)) ~
      ("status" -> stage.status.name()) ~
      ("description" -> description) ~
      ("schedulingPool" -> stage.schedulingPool) ~
      ("activeTasks" -> stage.activeTasks) ~
      ("completedTasks" -> stage.completedTasks) ~
      ("failedTasks" -> stage.failedTasks) ~
      ("completedIndices" -> completedIndices) ~
      ("killedTasks" -> stage.killedTasks) ~
      ("killedSummary" -> stage.killedSummary) ~
      ("firstLaunchTime" -> stage.firstLaunchTime) ~
      ("localitySummary" -> stage.localitySummary) ~
      ("metrics" -> apiTaskMetricsToJson(stage.metrics)) ~
      ("executorSummaries" -> executorSummaries) ~
      ("activeTasksPerExecutor" -> stage.activeTasksPerExecutor.toMap) ~
      ("blackListedExecutors" -> stage.blackListedExecutors.toSet) ~
      ("cleaning" -> stage.cleaning) ~
      ("savedTasks" -> stage.savedTasks.get())
  }

  def liveStageFromJson(json: JValue): LiveStage = {
    val liveStage = new LiveStage()
    liveStage.jobs = (json \ "jobs").extract[Seq[JValue]].map(liveJobFromJson)
    liveStage.jobIds = (json \ "jobIds").extract[Set[Int]]
    liveStage.info = JsonProtocol.stageInfoFromJson(json \ "info")
    liveStage.status = v1.StageStatus.fromString((json \ "status").extract[String])
    liveStage.description = jsonOption(json \ "description").map(_.extract[String])
    liveStage.schedulingPool = (json \ "schedulingPool").extract[String]
    liveStage.activeTasks = (json \ "activeTasks").extract[Int]
    liveStage.completedTasks = (json \ "completedTasks").extract[Int]
    liveStage.failedTasks = (json \ "failedTasks").extract[Int]
    val completedIndices = (json \ "completedIndices").extract[Set[Int]]
    completedIndices.foreach(liveStage.completedIndices.add)
    liveStage.killedTasks = (json \ "killedTasks").extract[Int]
    liveStage.killedSummary = (json \ "killedSummary").extract[Map[String, Int]]
    liveStage.firstLaunchTime = (json \ "firstLaunchTime").extract[Long]
    liveStage.localitySummary = (json \ "localitySummary").extract[Map[String, Long]]
    liveStage.metrics = apiTaskMetricsFromJson(json \ "metrics")

    val executorSummaries = (json \ "executorSummaries").extract[Map[String, JValue]]
      .mapValues(liveExecutorStageSummaryFromJson)
    liveStage.executorSummaries ++= executorSummaries

    val activeTasksPerExecutor = (json \ "activeTasksPerExecutor").extract[Map[String, Int]]
    liveStage.activeTasksPerExecutor ++= activeTasksPerExecutor

    val blackListedExecutors = (json \ "blackListedExecutors").extract[Set[String]]
    liveStage.blackListedExecutors ++= blackListedExecutors

    liveStage.cleaning = (json \ "cleaning").extract[Boolean]
    liveStage.savedTasks.set((json \ "savedTasks").extract[Int])

    liveStage
  }

  def liveExecutorToJson(executor: LiveExecutor): JValue = {
    val resources = executor.resources.mapValues(_.toJson)
    val peakExecutorMetrics = JsonProtocol.executorMetricsToJson(executor.peakExecutorMetrics)

    ("executorId" -> executor.executorId) ~
      ("hostPort" -> executor.hostPort) ~
      ("host" -> executor.host) ~
      ("isActive" -> executor.isActive) ~
      ("totalCores" -> executor.totalCores) ~
      ("addTime" -> executor.addTime.getTime) ~
      ("removeTime" -> Option(executor.removeTime).map(_.getTime).map(JLong(_)).orNull) ~
      ("removeReason" -> executor.removeReason) ~
      ("rddBlocks" -> executor.rddBlocks) ~
      ("memoryUsed" -> executor.memoryUsed) ~
      ("diskUsed" -> executor.diskUsed) ~
      ("maxTasks" -> executor.maxTasks) ~
      ("maxMemory" -> executor.maxMemory) ~
      ("totalTasks" -> executor.totalTasks) ~
      ("activeTasks" -> executor.activeTasks) ~
      ("completedTasks" -> executor.completedTasks) ~
      ("failedTasks" -> executor.failedTasks) ~
      ("totalDuration" -> executor.totalDuration) ~
      ("totalGcTime" -> executor.totalGcTime) ~
      ("totalInputBytes" -> executor.totalInputBytes) ~
      ("totalShuffleRead" -> executor.totalShuffleRead) ~
      ("totalShuffleWrite" -> executor.totalShuffleWrite) ~
      ("isBlacklisted" -> executor.isBlacklisted) ~
      ("blacklistedInStages" -> executor.blacklistedInStages) ~
      ("executorLogs" -> executor.executorLogs) ~
      ("attributes" -> executor.attributes) ~
      ("resources" -> resources) ~
      ("totalOnHeap" -> executor.totalOnHeap) ~
      ("totalOffHeap" -> executor.totalOffHeap) ~
      ("usedOnHeap" -> executor.usedOnHeap) ~
      ("usedOffHeap" -> executor.usedOffHeap) ~
      ("peakExecutorMetrics" -> peakExecutorMetrics)
  }

  def liveExecutorFromJson(json: JValue): LiveExecutor = {
    val executor = new LiveExecutor(
      (json \ "executorId").extract[String],
      (json \ "addTime").extract[Long])

    executor.hostPort = (json \ "hostPort").extract[String]
    executor.host = (json \ "host").extract[String]
    executor.isActive = (json \ "isActive").extract[Boolean]
    executor.totalCores = (json \ "totalCores").extract[Int]
    // addTime is set by passing _addTime, second parameter of constructor
    executor.removeTime = Option((json \ "removeTime").extract[Long]).map(new Date(_)).orNull
    executor.removeReason = (json \ "removeReason").extract[String]
    executor.rddBlocks = (json \ "rddBlocks").extract[Int]
    executor.memoryUsed = (json \ "memoryUsed").extract[Long]
    executor.diskUsed = (json \ "diskUsed").extract[Long]
    executor.maxTasks = (json \ "maxTasks").extract[Int]
    executor.maxMemory = (json \ "maxMemory").extract[Long]
    executor.totalTasks = (json \ "totalTasks").extract[Int]
    executor.activeTasks = (json \ "activeTasks").extract[Int]
    executor.completedTasks = (json \ "completedTasks").extract[Int]
    executor.failedTasks = (json \ "failedTasks").extract[Int]
    executor.totalDuration = (json \ "totalDuration").extract[Long]
    executor.totalGcTime = (json \ "totalGcTime").extract[Long]
    executor.totalInputBytes = (json \ "totalInputBytes").extract[Long]
    executor.totalShuffleRead = (json \ "totalShuffleRead").extract[Long]
    executor.totalShuffleWrite = (json \ "totalShuffleWrite").extract[Long]
    executor.isBlacklisted = (json \ "isBlacklisted").extract[Boolean]

    executor.blacklistedInStages = (json \ "blacklistedInStages").extract[Set[Int]]
    executor.executorLogs = (json \ "executorLogs").extract[Map[String, String]]
    executor.attributes = (json \ "attributes").extract[Map[String, String]]
    executor.resources = (json \ "resources").extract[Map[String, JValue]]
      .mapValues(ResourceInformation.parseJson)
    executor.totalOnHeap = (json \ "totalOnHeap").extract[Long]
    executor.totalOffHeap = (json \ "totalOffHeap").extract[Long]
    executor.usedOnHeap = (json \ "usedOnHeap").extract[Long]
    executor.usedOffHeap = (json \ "usedOffHeap").extract[Long]
    val peakExecutorMetrics = JsonProtocol.executorMetricsFromJson(
      json \ "peakExecutorMetrics")
    executor.peakExecutorMetrics.compareAndUpdatePeakValues(peakExecutorMetrics)
    executor
  }

  def liveExecutorStageSummaryToJson(summary: LiveExecutorStageSummary): JValue = {
    ("stageId" -> summary.stageId) ~
      ("attemptId" -> summary.attemptId) ~
      ("executorId" -> summary.executorId) ~
      ("taskTime" -> summary.taskTime) ~
      ("succeededTasks" -> summary.succeededTasks) ~
      ("failedTasks" -> summary.failedTasks) ~
      ("killedTasks" -> summary.killedTasks) ~
      ("isBlacklisted" -> summary.isBlacklisted) ~
      ("metrics" -> apiTaskMetricsToJson(summary.metrics))
  }

  def liveExecutorStageSummaryFromJson(json: JValue): LiveExecutorStageSummary = {
    val summary = new LiveExecutorStageSummary(
      (json \ "stageId").extract[Int],
      (json \ "attemptId").extract[Int],
      (json \ "executorId").extract[String])
    summary.taskTime = (json \ "taskTime").extract[Long]
    summary.succeededTasks = (json \ "succeededTasks").extract[Int]
    summary.failedTasks = (json \ "failedTasks").extract[Int]
    summary.killedTasks = (json \ "killedTasks").extract[Int]
    summary.isBlacklisted = (json \ "isBlacklisted").extract[Boolean]
    summary.metrics = apiTaskMetricsFromJson(json \ "metrics")
    summary
  }

  def liveTaskToJson(task: LiveTask): JValue = {
    ("info" -> JsonProtocol.taskInfoToJson(task.info)) ~
      ("stageId" -> task.stageId) ~
      ("stageAttemptId" -> task.stageAttemptId) ~
      ("lastUpdateTime" -> task.lastUpdateTime.map(JLong(_)).getOrElse(JNothing)) ~
      ("metrics" -> apiTaskMetricsToJson(task.metrics)) ~
      ("errorMessage" -> task.errorMessage.map(JString(_)).getOrElse(JNothing))
  }

  def liveTaskFromJson(json: JValue): LiveTask = {
    val task = new LiveTask(
      JsonProtocol.taskInfoFromJson(json \ "info"),
      (json \ "stageId").extract[Int],
      (json \ "stageAttemptId").extract[Int],
      jsonOption(json \ "lastUpdateTime").map(_.extract[Long]))
    task.metrics = apiTaskMetricsFromJson(json \ "metrics")
    task.errorMessage = jsonOption(json \ "errorMessage").map(_.extract[String])
    task
  }

  def liveRddToJson(rdd: LiveRDD): JValue = {
    val partitions = rdd.getPartitions().mapValues(liveRDDPartitionToJson)
    val distributions = rdd.getDistributions().mapValues(liveRDDDistributionToJson)
    ("info" -> JsonProtocol.rddInfoToJson(rdd.info)) ~
      ("storageLevel" -> rdd.storageLevel) ~
      ("memoryUsed" -> rdd.memoryUsed) ~
      ("diskUsed" -> rdd.diskUsed) ~
      ("partitions" -> partitions) ~
      ("distributions" -> distributions)
  }

  def liveRddFromJson(json: JValue): LiveRDD = {
    val rdd = new LiveRDD(JsonProtocol.rddInfoFromJson(json \ "info"))
    rdd.storageLevel = (json \ "storageLevel").extract[String]
    rdd.memoryUsed = (json \ "memoryUsed").extract[Long]
    rdd.diskUsed = (json \ "diskUsed").extract[Long]

    val partitions = (json \ "partitions").extract[Map[String, JValue]]
      .mapValues(liveRDDPartitionFromJson)
    rdd.addPartitions(partitions)

    val distributions = (json \ "distributions").extract[Map[String, JValue]]
      .mapValues(liveRDDDistributionFromJson)
    rdd.addDistributions(distributions)

    rdd
  }

  def liveRDDPartitionToJson(partition: LiveRDDPartition): JValue = {
    ("blockName" -> partition.blockName) ~
      ("value" -> apiRDDPartitionInfoToJson(partition.value))
  }

  def liveRDDPartitionFromJson(json: JValue): LiveRDDPartition = {
    val partition = new LiveRDDPartition((json \ "blockName").extract[String])
    partition.value = apiRDDPartitionInfoFromJson(json)
    partition
  }

  def liveRDDDistributionToJson(distribution: LiveRDDDistribution): JValue = {
    ("exec" -> liveExecutorToJson(distribution.exec)) ~
      ("memoryUsed" -> distribution.memoryUsed) ~
      ("diskUsed" -> distribution.diskUsed) ~
      ("onHeapUsed" -> distribution.onHeapUsed) ~
      ("offHeapUsed" -> distribution.offHeapUsed) ~
      ("lastUpdate" -> apiRDDDataDistributionToJson(distribution.lastUpdate))
  }

  def liveRDDDistributionFromJson(json: JValue): LiveRDDDistribution = {
    val distribution = new LiveRDDDistribution(liveExecutorFromJson(json \ "exec"))
    distribution.memoryUsed = (json \ "memoryUsed").extract[Long]
    distribution.diskUsed = (json \ "diskUsed").extract[Long]
    distribution.onHeapUsed = (json \ "onHeapUsed").extract[Long]
    distribution.offHeapUsed = (json \ "offHeapUsed").extract[Long]
    distribution.lastUpdate = apiRDDDataDistributionFromJson(json \ "lastUpdate")
    distribution
  }

  def schedulerPoolToJson(pool: SchedulerPool): JValue = {
    ("name" -> pool.name) ~ ("stageIds" -> pool.stageIds)
  }

  def schedulerPoolFromJson(json: JValue): SchedulerPool = {
    val pool = new SchedulerPool((json \ "name").extract[String])
    val stageIds = (json \ "stageIds").extract[Set[Int]]
    pool.stageIds ++= stageIds
    pool
  }

  private def apiTaskMetricsToJson(metrics: v1.TaskMetrics): JValue = {
    parse(mapper.writeValueAsString(metrics))
  }

  private def apiTaskMetricsFromJson(json: JValue): v1.TaskMetrics = {
    json.extract[v1.TaskMetrics]
  }

  private def apiRDDDataDistributionToJson(distribution: v1.RDDDataDistribution): JValue = {
    parse(mapper.writeValueAsString(distribution))
  }

  private def apiRDDDataDistributionFromJson(json: JValue): v1.RDDDataDistribution = {
    json.extract[v1.RDDDataDistribution]
  }

  private def apiRDDPartitionInfoToJson(partition: v1.RDDPartitionInfo): JValue = {
    parse(mapper.writeValueAsString(partition))
  }

  private def apiRDDPartitionInfoFromJson(json: JValue): v1.RDDPartitionInfo = {
    json.extract[v1.RDDPartitionInfo]
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }
}

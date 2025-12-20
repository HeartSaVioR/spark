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

package org.apache.spark.sql.execution.benchmark

import java.util.UUID

import scala.collection.mutable
import scala.util.Random

// FIXME: temporary
import one.profiler.AsyncProfiler
// import one.profiler.Events
import org.apache.hadoop.conf.Configuration

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.operators.stateful.join.{JoinStateManagerStoreGenerator, SupportsEvictByTimestamp, SymmetricHashJoinStateManager}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.LeftSide
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreConf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StructField, StructType, TimestampType}
import org.apache.spark.util.Utils


/**
 * Synthetic benchmark for Stream-Stream join operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to:
 *      "sql/core/benchmarks/StreamStreamJoinOneSideOperationsBenchmark-results.txt".
 * }}}
 */
object StreamStreamJoinOneSideOperationsBenchmark extends SqlBasedBenchmark with Logging {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runTestWithTimeWindowJoin(numTotalRows = 1000000)

    val numTotalRows = 1000000
    Seq(
      (1000, 1) // ,
      // (500, 2),
      // (200, 5),
      // (100, 10),
      // (10, 100)
    ).foreach { case (numTimestamps, numValuesPerTimestamp) =>
      runTestWithTimeIntervalJoin(
        numTotalRows = numTotalRows,
        numTimestamps = numTimestamps,
        numValuesPerTimestamp = numValuesPerTimestamp
      )
    }
  }

  case class StateOpInfo(
      queryRunId: UUID,
      checkpointLocation: String,
      operatorId: Int)

  private def runTestWithTimeWindowJoin(numTotalRows: Int): Unit = {
    val (joinKeys, inputAttributes) = getAttributesForTimeWindowJoin()
    val stateFormatVersions = Seq(1, 2, 3, 4)
    // FIXME: testing...
    // val stateFormatVersions = Seq(4)
    val changelogCheckpointOptions = Seq(true, false)

    testWithTimeWindowJoin(
      inputAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateFormatVersions = stateFormatVersions,
      changelogCheckpointOptions = changelogCheckpointOptions,
      numTotalRows = numTotalRows
    )
  }

  private def runTestWithTimeIntervalJoin(
      numTotalRows: Int,
      numTimestamps: Int,
      numValuesPerTimestamp: Int): Unit = {
    val (joinKeys, inputAttributes) = getAttributesForTimeIntervalJoin()
    val stateFormatVersions = Seq(1, 2, 3, 4)
    // FIXME: testing...
    // val stateFormatVersions = Seq(4)
    val changelogCheckpointOptions = Seq(true, false)

    testWithTimeIntervalJoin(
      inputAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateFormatVersions = stateFormatVersions,
      changelogCheckpointOptions = changelogCheckpointOptions,
      numTotalRows = numTotalRows,
      numTimestamps = numTimestamps,
      numValuesPerTimestamp = numValuesPerTimestamp
    )
  }

  private def testWithTimeWindowJoin(
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      numTotalRows: Int): Unit = {
    val inputData = prepareInputDataForTimeWindowJoin(
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      numRows = numTotalRows
    )

    val stateFormatVersionToStateOpInfo = mutable.HashMap[(Int, Boolean), StateOpInfo]()
    for {
      stateFormatVersion <- stateFormatVersions
      useChangelogCheckpoint <- changelogCheckpointOptions
    } {
      val queryRunId = UUID.randomUUID()
      val checkpointDir = newDir()
      val operatorId = 0

      stateFormatVersionToStateOpInfo.put(
        (stateFormatVersion, useChangelogCheckpoint),
        StateOpInfo(
          queryRunId = queryRunId,
          checkpointLocation = checkpointDir,
          operatorId = operatorId
        )
      )
    }

    testAppendWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      changelogCheckpointOptions = changelogCheckpointOptions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo
    )

    testGetJoinedRowsWithTimeWindowJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      changelogCheckpointOptions = changelogCheckpointOptions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      numKeysToGet = numTotalRows / 10
    )

    Seq(0.0001, 0.1, 0.3, 0.6, 0.9).foreach { evictionRate =>
      testEvictionRowsWithTimeWindowJoin(
        inputData = inputData,
        joinKeys = joinKeys,
        inputAttributes = inputAttributes,
        stateFormatVersions = stateFormatVersions,
        changelogCheckpointOptions = changelogCheckpointOptions,
        stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
        evictionRate = evictionRate
      )
    }

    Seq(0.0001, 0.1, 0.3, 0.6, 0.9).foreach { evictionRate =>
      testEvictionAndReturnRowsWithTimeWindowJoin(
        inputData = inputData,
        joinKeys = joinKeys,
        inputAttributes = inputAttributes,
        stateFormatVersions = stateFormatVersions,
        changelogCheckpointOptions = changelogCheckpointOptions,
        stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
        evictionRate = evictionRate
      )
    }
  }

  private def testAppendWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo]): Unit = {
    val numInputRows = inputData.size

    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForPut = new Benchmark(
        s"[Time-window Join] Append $numInputRows rows (changelog checkpoint: " +
          s"$useChangelogCheckpoint)",
        numInputRows,
        minNumIters = 3,
        output = output)

      stateFormatVersions.foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForPut.addTimerCase(
          s"state format version: $stateFormatVersion",
          // enforce single iteration to avoid re-committing the same version multiple times
          numIters = 1) { timer =>

          ProfilingHelper.profile(
            s"time-window-append-$numInputRows-" +
              s"state-version-$stateFormatVersion") {

            val joinStateManager = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = stateOpInfo.checkpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 0,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            timer.startTiming()

            inputData.foreach { case (keyRow, valueRow) =>
              joinStateManager.append(keyRow, valueRow, matched = false)
            }
            joinStateManager.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForPut.run()
    }
  }

  private def testGetJoinedRowsWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      numKeysToGet: Int): Unit = {

    val numRowsInStateStore = inputData.size
    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForGetJoinedRows = new Benchmark(
        s"[Time-window Join] GetJoinedRows $numKeysToGet from $numRowsInStateStore rows " +
          s"(changelog checkpoint: $useChangelogCheckpoint)",
        numKeysToGet,
        minNumIters = 3,
        output = output)

      val shuffledInputDataForJoinedRows = Random.shuffle(inputData).take(numKeysToGet)

      // FIXME: state format version 3 raises an exception
      //  Exception in thread "main" java.lang.NullPointerException:
      //  Cannot invoke "org.apache.spark.sql.catalyst.expressions.SpecializedGetters.getInt(int)"
      //  because "<parameter1>" is null
      //  Need to fix this after testing.
      stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForGetJoinedRows.addTimerCase(
          s"state format version: $stateFormatVersion", numIters = 3) { timer =>

          ProfilingHelper.profile(
            s"time-window-getjoinedrows-$numKeysToGet-" +
              s"state-version-$stateFormatVersion") {

            val cloneCheckpointLocation = newDir()
            Utils.copyDirectory(
              new java.io.File(stateOpInfo.checkpointLocation),
              new java.io.File(cloneCheckpointLocation))

            val joinStateManagerVer1 = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = cloneCheckpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 1,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            val joinedRow = new JoinedRow()

            timer.startTiming()

            var joinedRowsCount = 0

            shuffledInputDataForJoinedRows.foreach { case (keyRow, valueRow) =>
              val joinedRows = joinStateManagerVer1.getJoinedRows(
                keyRow,
                thatRow => joinedRow.withLeft(valueRow).withRight(thatRow),
                _ => true,
                excludeRowsAlreadyMatched = false)
              joinedRows.foreach { _ =>
                joinedRowsCount += 1
              }
            }

            assert(joinedRowsCount == numKeysToGet,
              s"Expected $numKeysToGet joined rows, but got $joinedRowsCount")

            joinStateManagerVer1.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForGetJoinedRows.run()
    }
  }

  private def testEvictionRowsWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      evictionRate: Double): Unit = {

    assert(evictionRate >= 0.0 && evictionRate < 1.0,
      s"Eviction rate must be between 0.0 and 1.0, but got $evictionRate")

    val totalStateRows = inputData.size
    val numTargetEvictRows = (totalStateRows * evictionRate).toInt

    val orderedInputDataByTime = inputData.sortBy { case (keyRow, _) =>
      // extract window end time from the key row
      val windowRow = keyRow.getStruct(1, 2)
      windowRow.getLong(1)
    }

    val windowEndForEviction = orderedInputDataByTime.take(numTargetEvictRows).last
      ._1.getStruct(1, 2).getLong(1)

    val actualNumRowsToExpectEviction = inputData.count { case (keyRow, _) =>
      val windowRow = keyRow.getStruct(1, 2)
      val windowEnd = windowRow.getLong(1)
      windowEnd <= windowEndForEviction
    }

    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForEviction = new Benchmark(
        s"[Time-window Join] Eviction with $actualNumRowsToExpectEviction " +
          s"from $totalStateRows rows " +
        s"(changelog checkpoint: $useChangelogCheckpoint)",
        actualNumRowsToExpectEviction,
        minNumIters = 3,
        output = output)

      stateFormatVersions.foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForEviction.addTimerCase(
          s"state format version: $stateFormatVersion", numIters = 3) { timer =>

          ProfilingHelper.profile(
            s"time-window-eviction-$actualNumRowsToExpectEviction-" +
              s"state-version-$stateFormatVersion") {

            val cloneCheckpointLocation = newDir()
            Utils.copyDirectory(
              new java.io.File(stateOpInfo.checkpointLocation),
              new java.io.File(cloneCheckpointLocation))

            val joinStateManagerVer1 = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = cloneCheckpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 1,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            timer.startTiming()

            var evictedRowsCount = 0

            joinStateManagerVer1 match {
              case m: SupportsEvictByTimestamp =>
                evictedRowsCount = m.evictByTimestamp(windowEndForEviction)

              case _ =>
                val evictedRows = joinStateManagerVer1.removeByKeyCondition {
                  keyRow =>
                    val windowRow = keyRow.getStruct(1, 2)
                    val windowEnd = windowRow.getLong(1)
                    windowEnd <= windowEndForEviction
                }
                evictedRows.foreach { _ =>
                  evictedRowsCount += 1
                }
            }

            assert(evictedRowsCount == actualNumRowsToExpectEviction,
              s"Expected $actualNumRowsToExpectEviction joined rows, but got $evictedRowsCount")

            joinStateManagerVer1.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForEviction.run()
    }
  }

  private def testEvictionAndReturnRowsWithTimeWindowJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      evictionRate: Double): Unit = {

    assert(evictionRate >= 0.0 && evictionRate < 1.0,
      s"Eviction rate must be between 0.0 and 1.0, but got $evictionRate")

    val totalStateRows = inputData.size
    val numTargetEvictRows = (totalStateRows * evictionRate).toInt

    val orderedInputDataByTime = inputData.sortBy { case (keyRow, _) =>
      // extract window end time from the key row
      val windowRow = keyRow.getStruct(1, 2)
      windowRow.getLong(1)
    }

    val windowEndForEviction = orderedInputDataByTime.take(numTargetEvictRows).last
      ._1.getStruct(1, 2).getLong(1)

    val actualNumRowsToExpectEviction = inputData.count { case (keyRow, _) =>
      val windowRow = keyRow.getStruct(1, 2)
      val windowEnd = windowRow.getLong(1)
      windowEnd <= windowEndForEviction
    }

    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForEviction = new Benchmark(
        s"[Time-window Join] Eviction And Return Rows with $actualNumRowsToExpectEviction " +
          s"from $totalStateRows rows " +
          s"(changelog checkpoint: $useChangelogCheckpoint)",
        actualNumRowsToExpectEviction,
        minNumIters = 3,
        output = output)

      stateFormatVersions.foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForEviction.addTimerCase(
          s"state format version: $stateFormatVersion", numIters = 3) { timer =>

          ProfilingHelper.profile(
            s"time-window-eviction-and-return-$actualNumRowsToExpectEviction-" +
                s"state-version-$stateFormatVersion") {

            val cloneCheckpointLocation = newDir()
            Utils.copyDirectory(
              new java.io.File(stateOpInfo.checkpointLocation),
              new java.io.File(cloneCheckpointLocation))

            val joinStateManagerVer1 = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = cloneCheckpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 1,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            timer.startTiming()

            var evictedRowsCount = 0

            joinStateManagerVer1 match {
              case m: SupportsEvictByTimestamp =>
                m.evictAndReturnByTimestamp(windowEndForEviction).foreach { _ =>
                  evictedRowsCount += 1
                }

              case _ =>
                val evictedRows = joinStateManagerVer1.removeByKeyCondition {
                  keyRow =>
                    val windowRow = keyRow.getStruct(1, 2)
                    val windowEnd = windowRow.getLong(1)
                    windowEnd <= windowEndForEviction
                }
                evictedRows.foreach { _ =>
                  evictedRowsCount += 1
                }
            }

            assert(evictedRowsCount == actualNumRowsToExpectEviction,
              s"Expected $actualNumRowsToExpectEviction joined rows, but got $evictedRowsCount")

            joinStateManagerVer1.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForEviction.run()
    }
  }

  private def prepareInputDataForTimeWindowJoin(
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      numRows: Int): List[(UnsafeRow, UnsafeRow)] = {
    val rowsToAppend = mutable.ArrayBuffer[(UnsafeRow, UnsafeRow)]()
    val keyProj = UnsafeProjection.create(joinKeys, inputAttributes)
    val valueProj = UnsafeProjection.create(inputAttributes, inputAttributes)

    0.until(numRows).foreach { i =>
      val deptId = i % 10
      val windowStart = (i / 10) * 1000L
      val windowEnd = (i / 10) * 1000L + 1000L

      val sum = Random.nextInt(100)
      val count = Random.nextInt(10)

      val row = new GenericInternalRow(
        Array[Any](
          deptId,
          new GenericInternalRow(
            Array[Any](windowStart, windowEnd)
          ),
          sum.toLong,
          count.toLong)
      )

      val keyUnsafeRow = keyProj(row).copy()
      val valueUnsafeRow = valueProj(row).copy()
      rowsToAppend.append((keyUnsafeRow, valueUnsafeRow))
    }

    Random.shuffle(rowsToAppend.toList)
  }

  private def testWithTimeIntervalJoin(
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      numTotalRows: Int,
      numTimestamps: Int,
      numValuesPerTimestamp: Int): Unit = {

    val inputData = prepareInputDataForTimeIntervalJoin(
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      numTotalRows = numTotalRows,
      numTimestamps = numTimestamps,
      numValuesPerTimestamp = numValuesPerTimestamp
    )

    val stateFormatVersionToStateOpInfo = mutable.HashMap[(Int, Boolean), StateOpInfo]()
    for {
      stateFormatVersion <- stateFormatVersions
      useChangelogCheckpoint <- changelogCheckpointOptions
    } {
      val queryRunId = UUID.randomUUID()
      val checkpointDir = newDir()
      val operatorId = 0

      stateFormatVersionToStateOpInfo.put(
        (stateFormatVersion, useChangelogCheckpoint),
        StateOpInfo(
          queryRunId = queryRunId,
          checkpointLocation = checkpointDir,
          operatorId = operatorId
        )
      )
    }

    testAppendWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      changelogCheckpointOptions = changelogCheckpointOptions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      numTimestamps = numTimestamps,
      numValuesPerTimestamp = numValuesPerTimestamp
    )

    testGetJoinedRowsWithTimeIntervalJoin(
      inputData = inputData,
      joinKeys = joinKeys,
      inputAttributes = inputAttributes,
      stateFormatVersions = stateFormatVersions,
      changelogCheckpointOptions = changelogCheckpointOptions,
      stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
      numTimestamps = numTimestamps,
      numValuesPerTimestamp = numValuesPerTimestamp,
      retrievalRate = 0.1
    )

    Seq(0.0001, 0.1, 0.3, 0.6, 0.9).foreach { evictionRate =>
      testEvictionRowsWithTimeIntervalJoin(
        inputData = inputData,
        joinKeys = joinKeys,
        inputAttributes = inputAttributes,
        stateFormatVersions = stateFormatVersions,
        changelogCheckpointOptions = changelogCheckpointOptions,
        stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
        numTimestamps = numTimestamps,
        numValuesPerTimestamp = numValuesPerTimestamp,
        evictionRate = evictionRate
      )
    }

    Seq(0.0001, 0.1, 0.3, 0.6, 0.9).foreach { evictionRate =>
      testEvictionAndReturnRowsWithTimeIntervalJoin(
        inputData = inputData,
        joinKeys = joinKeys,
        inputAttributes = inputAttributes,
        stateFormatVersions = stateFormatVersions,
        changelogCheckpointOptions = changelogCheckpointOptions,
        stateFormatVersionToStateOpInfo = stateFormatVersionToStateOpInfo,
        numTimestamps = numTimestamps,
        numValuesPerTimestamp = numValuesPerTimestamp,
        evictionRate = evictionRate
      )
    }
  }

  private def testAppendWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      numTimestamps: Int,
      numValuesPerTimestamp: Int): Unit = {
    val numInputRows = inputData.size

    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForPut = new Benchmark(
        s"[Time-interval Join] Append $numInputRows rows (numTimestamp: $numTimestamps, " +
          s"numValuesPerTimestamp: $numValuesPerTimestamp) (changelog checkpoint: " +
          s"$useChangelogCheckpoint)",
        numInputRows,
        minNumIters = 3,
        output = output)

      stateFormatVersions.foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForPut.addTimerCase(
          s"state format version: $stateFormatVersion",
          // enforce single iteration to avoid re-committing the same version multiple times
          numIters = 1) { timer =>

          ProfilingHelper.profile(
            s"time-interval-append-$numInputRows-" +
              s"state-version-$stateFormatVersion") {

            val joinStateManager = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = stateOpInfo.checkpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 0,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            timer.startTiming()

            inputData.foreach { case (keyRow, valueRow) =>
              joinStateManager.append(keyRow, valueRow, matched = false)
            }
            joinStateManager.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForPut.run()
    }
  }

  private def testGetJoinedRowsWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      numTimestamps: Int,
      numValuesPerTimestamp: Int,
      retrievalRate: Double): Unit = {

    val numRowsInStateStore = inputData.size
    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>

      val targetNumValuesToRetrieve = (numRowsInStateStore * retrievalRate).toInt
      val numTotalKeys = inputData.map(_._1).distinct.size
      val numValuesPerKey = numRowsInStateStore / numTotalKeys
      val numKeysToGet = Math.ceil(
        targetNumValuesToRetrieve.toDouble / numValuesPerKey.toDouble).toInt

      val shuffledKeys = Random.shuffle(inputData).map(_._1).distinct
      assert(numKeysToGet <= shuffledKeys.size,
        s"numKeysToGet ($numKeysToGet) must be less than or equal to " +
          s"the number of distinct keys in state store (${shuffledKeys.size})")
      val shuffledKeysForJoinedRows = Random.shuffle(inputData).map(_._1).distinct
        .take(numKeysToGet)

      val actualMatchingValues = shuffledKeysForJoinedRows.map { keyRow =>
        inputData.filter { case (k, _) => k == keyRow }.count(_ => true)
      }.sum

      val benchmarkForGetJoinedRows = new Benchmark(
        s"[Time-interval Join] GetJoinedRows $numKeysToGet keys " +
          s"(matching values: $actualMatchingValues) " +
          s"from $numRowsInStateStore rows (numTimestamp: $numTimestamps, " +
          s"numValuesPerTimestamp: $numValuesPerTimestamp)",
        // FIXME: Should we measure this by keys or values?
        actualMatchingValues,
        minNumIters = 3,
        output = output)

      val dummyValueRow = inputData.head._2

      // FIXME: state format version 3 raises an exception
      //  Exception in thread "main" java.lang.NullPointerException:
      //  Cannot invoke "org.apache.spark.sql.catalyst.expressions.SpecializedGetters.getInt(int)"
      //  because "<parameter1>" is null
      //  Need to fix this after testing.
      stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForGetJoinedRows.addTimerCase(
          s"state format version: $stateFormatVersion", numIters = 3) { timer =>

          ProfilingHelper.profile(
            s"time-interval-getjoinedrows-$actualMatchingValues-" +
              s"state-version-$stateFormatVersion") {

            val cloneCheckpointLocation = newDir()
            Utils.copyDirectory(
              new java.io.File(stateOpInfo.checkpointLocation),
              new java.io.File(cloneCheckpointLocation))

            val joinStateManagerVer1 = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = cloneCheckpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 1,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            val joinedRow = new JoinedRow()

            timer.startTiming()

            var joinedRowsCount = 0

            shuffledKeysForJoinedRows.foreach { keyRow =>
              val joinedRows = joinStateManagerVer1.getJoinedRows(
                keyRow,
                thatRow => joinedRow.withLeft(dummyValueRow).withRight(thatRow),
                _ => true,
                excludeRowsAlreadyMatched = false)
              joinedRows.foreach { _ =>
                joinedRowsCount += 1
              }
            }

            assert(joinedRowsCount == actualMatchingValues,
              s"Expected $actualMatchingValues joined rows, but got $joinedRowsCount")

            joinStateManagerVer1.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForGetJoinedRows.run()
    }
  }

  private def testEvictionRowsWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      numTimestamps: Int,
      numValuesPerTimestamp: Int,
      evictionRate: Double): Unit = {

    assert(evictionRate >= 0.0 && evictionRate < 1.0,
      s"Eviction rate must be between 0.0 and 1.0, but got $evictionRate")

    val totalStateRows = inputData.size
    val numTargetEvictRows = (totalStateRows * evictionRate).toInt

    val orderedInputDataByTime = inputData.sortBy { case (_, valueRow) =>
      // impressionTimestamp is at index 1 in value row
      valueRow.getLong(1)
    }

    val tsForEviction = orderedInputDataByTime.take(numTargetEvictRows).last
      ._2.getLong(1)

    val actualNumRowsToExpectEviction = inputData.count { case (_, valueRow) =>
      valueRow.getLong(1) <= tsForEviction
    }

    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForEviction = new Benchmark(
        s"[Time-interval Join] Eviction with $actualNumRowsToExpectEviction " +
          s"from $totalStateRows rows (numTimestamp: $numTimestamps, " +
          s"numValuesPerTimestamp: $numValuesPerTimestamp) " +
          s"(changelog checkpoint: $useChangelogCheckpoint)",
        actualNumRowsToExpectEviction,
        minNumIters = 3,
        output = output)

      // scalastyle:off line.size.limit
      // FIXME: state format version 3 has some issue with this -
      //  java.lang.NullPointerException: Cannot invoke
      //  "org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager$ValueAndMatchPair.value()"
      //  because "valuePair" is null
      // scalastyle:on line.size.limit
      stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForEviction.addTimerCase(
          s"state format version: $stateFormatVersion", numIters = 3) { timer =>

          ProfilingHelper.profile(
            s"time-interval-eviction-$actualNumRowsToExpectEviction-" +
              s"state-version-$stateFormatVersion") {
            val cloneCheckpointLocation = newDir()
            Utils.copyDirectory(
              new java.io.File(stateOpInfo.checkpointLocation),
              new java.io.File(cloneCheckpointLocation))

            val joinStateManagerVer1 = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = cloneCheckpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 1,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            timer.startTiming()

            var evictedRowsCount = 0

            joinStateManagerVer1 match {
              case m: SupportsEvictByTimestamp =>
                evictedRowsCount = m.evictByTimestamp(tsForEviction) * numValuesPerTimestamp

              case _ =>
                val evictedRows = joinStateManagerVer1.removeByValueCondition { valueRow =>
                  valueRow.getLong(1) <= tsForEviction
                }
                evictedRows.foreach { _ =>
                  evictedRowsCount += 1
                }
            }

            assert(evictedRowsCount == actualNumRowsToExpectEviction,
              s"Expected $actualNumRowsToExpectEviction joined rows, but got $evictedRowsCount")

            joinStateManagerVer1.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForEviction.run()
    }
  }

  private def testEvictionAndReturnRowsWithTimeIntervalJoin(
      inputData: List[(UnsafeRow, UnsafeRow)],
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      stateFormatVersions: Seq[Int],
      changelogCheckpointOptions: Seq[Boolean],
      stateFormatVersionToStateOpInfo: mutable.HashMap[(Int, Boolean), StateOpInfo],
      numTimestamps: Int,
      numValuesPerTimestamp: Int,
      evictionRate: Double): Unit = {

    assert(evictionRate >= 0.0 && evictionRate < 1.0,
      s"Eviction rate must be between 0.0 and 1.0, but got $evictionRate")

    val totalStateRows = inputData.size
    val numTargetEvictRows = (totalStateRows * evictionRate).toInt

    val orderedInputDataByTime = inputData.sortBy { case (_, valueRow) =>
      // impressionTimestamp is at index 1 in value row
      valueRow.getLong(1)
    }

    val tsForEviction = orderedInputDataByTime.take(numTargetEvictRows).last
      ._2.getLong(1)

    val actualNumRowsToExpectEviction = inputData.count { case (_, valueRow) =>
      valueRow.getLong(1) <= tsForEviction
    }

    changelogCheckpointOptions.foreach { useChangelogCheckpoint =>
      val benchmarkForEviction = new Benchmark(
        s"[Time-interval Join] Eviction And Return Rows with $actualNumRowsToExpectEviction " +
          s"from $totalStateRows rows (numTimestamp: $numTimestamps, " +
          s"numValuesPerTimestamp: $numValuesPerTimestamp) " +
          s"(changelog checkpoint: $useChangelogCheckpoint)",
        actualNumRowsToExpectEviction,
        minNumIters = 3,
        output = output)

      // scalastyle:off line.size.limit
      // FIXME: state format version 3 has some issue with this -
      //  java.lang.NullPointerException: Cannot invoke
      //  "org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager$ValueAndMatchPair.value()"
      //  because "valuePair" is null
      // scalastyle:on line.size.limit
      stateFormatVersions.diff(Seq(3)).foreach { stateFormatVersion =>
        val stateOpInfo = stateFormatVersionToStateOpInfo(
          (stateFormatVersion, useChangelogCheckpoint))

        benchmarkForEviction.addTimerCase(
          s"state format version: $stateFormatVersion", numIters = 3) { timer =>

          ProfilingHelper.profile(
            s"time-interval-eviction-and-return-$actualNumRowsToExpectEviction-" +
              s"state-version-$stateFormatVersion") {
            val cloneCheckpointLocation = newDir()
            Utils.copyDirectory(
              new java.io.File(stateOpInfo.checkpointLocation),
              new java.io.File(cloneCheckpointLocation))

            val joinStateManagerVer1 = createJoinStateManager(
              queryRunId = stateOpInfo.queryRunId,
              checkpointLocation = cloneCheckpointLocation,
              operatorId = stateOpInfo.operatorId,
              storeVersion = 1,
              inputAttributes = inputAttributes,
              joinKeys = joinKeys,
              stateFormatVersion = stateFormatVersion,
              useChangelogCheckpoint = useChangelogCheckpoint)

            timer.startTiming()

            var evictedRowsCount = 0

            joinStateManagerVer1 match {
              case m: SupportsEvictByTimestamp =>
                m.evictAndReturnByTimestamp(tsForEviction).foreach { _ =>
                  evictedRowsCount += 1
                }

              case _ =>
                val evictedRows = joinStateManagerVer1.removeByValueCondition { valueRow =>
                  valueRow.getLong(1) <= tsForEviction
                }
                evictedRows.foreach { _ =>
                  evictedRowsCount += 1
                }
            }

            assert(evictedRowsCount == actualNumRowsToExpectEviction,
              s"Expected $actualNumRowsToExpectEviction joined rows, but got $evictedRowsCount")

            joinStateManagerVer1.commit()

            timer.stopTiming()
          }
        }
      }

      benchmarkForEviction.run()
    }
  }

  private def prepareInputDataForTimeIntervalJoin(
      joinKeys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      numTotalRows: Int,
      numTimestamps: Int,
      numValuesPerTimestamp: Int): List[(UnsafeRow, UnsafeRow)] = {
    val rowsToAppend = mutable.ArrayBuffer[(UnsafeRow, UnsafeRow)]()
    val keyProj = UnsafeProjection.create(joinKeys, inputAttributes)
    val valueProj = UnsafeProjection.create(inputAttributes, inputAttributes)

    assert(
      numTotalRows >= (numTimestamps * numValuesPerTimestamp) &&
      numTotalRows % (numTimestamps * numValuesPerTimestamp) == 0,
      "totalRows must be divisible by (numTimestamps * numValuesPerTimestamp), " +
        s"but got totalRows: $numTotalRows, numTimestamps: $numTimestamps, " +
        s"numValuesPerTimestamp: $numValuesPerTimestamp"
    )

    val numKeys = numTotalRows / (numTimestamps * numValuesPerTimestamp)

    (0 until numKeys).foreach { keyIdx =>
      val adId = keyIdx

      (0 until numTimestamps).foreach { timestampIdx =>
        val impressionTimestamp = timestampIdx * 1000L

        (0 until numValuesPerTimestamp).foreach { _ =>
          // Below twos are effectively not used in join criteria and does not impact the benchmark
          val userId = Random.nextInt(1000)
          val campaignId = Random.nextInt(10000)

          val row = new GenericInternalRow(
            Array[Any](
              adId,
              impressionTimestamp,
              userId,
              campaignId)
          )

          val keyUnsafeRow = keyProj(row).copy()
          val valueUnsafeRow = valueProj(row).copy()
          rowsToAppend.append((keyUnsafeRow, valueUnsafeRow))
        }
      }
    }

    Random.shuffle(rowsToAppend.toList)
  }

  private def getAttributesForTimeWindowJoin(): (Seq[Expression], Seq[Attribute]) = {
    val inputAttributes = Seq(
      AttributeReference("deptId", IntegerType, nullable = false)(),
      AttributeReference(
        "window",
        StructType(
          Seq(
            StructField("start", TimestampType, nullable = false),
            StructField("end", TimestampType, nullable = false))),
        nullable = false,
        metadata = new MetadataBuilder()
          .putLong(EventTimeWatermark.delayKey, 0L)
          .build())(),
      AttributeReference("sum", LongType, nullable = false)(),
      AttributeReference("count", LongType, nullable = false)()
    )

    val joinKeys = Seq(
      inputAttributes(0), // deptId
      inputAttributes(1)  // window
    )

    (joinKeys, inputAttributes)
  }

  private def getAttributesForTimeIntervalJoin(): (Seq[Expression], Seq[Attribute]) = {
    val inputAttributes = Seq(
      AttributeReference("adId", IntegerType, nullable = false)(),
      AttributeReference("impressionTimestamp", TimestampType, nullable = false,
        metadata = new MetadataBuilder()
          .putLong(EventTimeWatermark.delayKey, 0L)
          .build())(),
      AttributeReference("userId", IntegerType, nullable = false)(),
      AttributeReference("campaignId", IntegerType, nullable = false)()
    )
    val joinKeys = Seq(
      inputAttributes(0) // adId
    )
    (joinKeys, inputAttributes)
  }

  final def skip(benchmarkName: String)(func: => Any): Unit = {
    output.foreach(_.write(s"$benchmarkName is skipped".getBytes))
  }

  private def createJoinStateManager(
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      queryRunId: UUID,
      checkpointLocation: String,
      operatorId: Int,
      storeVersion: Int,
      stateFormatVersion: Int,
      useChangelogCheckpoint: Boolean): SymmetricHashJoinStateManager = {
    val joinSide = LeftSide
    val partitionId = 0

    val stateInfo = Some(
      StatefulOperatorStateInfo(
        checkpointLocation = checkpointLocation,
        queryRunId = queryRunId,
        operatorId = operatorId,
        storeVersion = storeVersion,
        numPartitions = 1,
        stateSchemaMetadata = None,
        stateStoreCkptIds = None
      )
    )

    val sqlConf = new SQLConf()
    // We are only interested with RocksDB state store provider here
    sqlConf.setConfString("spark.sql.streaming.stateStore.providerClass",
      classOf[RocksDBStateStoreProvider].getName)
    sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
      useChangelogCheckpoint.toString)
    // FIXME: temporarily enabling this to debug
    sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows",
      false.toString)
    sqlConf.setConfString("spark.sql.streaming.stateStore.coordinatorReportSnapshotUploadLag",
      false.toString)
    val storeConf = new StateStoreConf(sqlConf)

    val hadoopConf = new Configuration
    hadoopConf.set(StreamExecution.RUN_ID_KEY, queryRunId.toString)

    val joinStoreGenerator = new JoinStateManagerStoreGenerator()

    SymmetricHashJoinStateManager(
      joinSide = joinSide,
      inputValueAttributes = inputAttributes,
      joinKeys = joinKeys,
      stateInfo = stateInfo,
      storeConf = storeConf,
      hadoopConf = hadoopConf,
      partitionId = partitionId,
      keyToNumValuesStateStoreCkptId = None,
      keyWithIndexToValueStateStoreCkptId = None,
      snapshotOptions = None,
      stateFormatVersion = stateFormatVersion,
      skippedNullValueCount = None,
      joinStoreGenerator = joinStoreGenerator,
      useStateStoreCoordinator = false
    )
  }

  private def newDir(): String = Utils.createTempDir().toString
}

object ProfilingHelper extends Logging {
  // FIXME: NO-OP for now
  // Automatically loads the native library from the JAR
  private val profiler = AsyncProfiler.getInstance()

  def profile[T](name: String)(block: => T): T = {
    // FIXME: NO-OP for now
    /*
    // FIXME: for devbox
    val outputFile = s"/home/jungtaek.lim/flamegraph/flamegraph-$name.html"

    // Start profiling: CPU mode, interval 10ms
    // 'event' can be "cpu", "alloc", "wall", "lock"
    profiler.start(Events.CPU, 10000000)

    try {
      logWarning(s"Profiling started for $name...")
      block
    } finally {
      // Stop and dump to HTML
      profiler.stop()
      profiler.execute(s"stop,file=$outputFile")
      logWarning(s"Profiling stopped. Flamegraph saved to: $outputFile")
    }
    */
    block
  }
}

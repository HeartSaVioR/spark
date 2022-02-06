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

import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf.TIME_WINDOW_NEW_LOGIC

/**
 * Synthetic benchmark for date and timestamp functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/TimeWindowBenchmark-results.txt".
 * }}}
 */
object TimeWindowBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numOfRow = 10000000
    runBenchmark("create windows") {
      val benchmark = new Benchmark("tumbling windows",
        numOfRow, minTime = 10.seconds, output = output)

      benchmark.addCase("old logic") { _ =>
        withSQLConf(TIME_WINDOW_NEW_LOGIC.key -> "false") {
          runTumbleWindow(numOfRow)
        }
      }

      benchmark.addCase("new logic") { _ =>
        withSQLConf(TIME_WINDOW_NEW_LOGIC.key -> "true") {
          runTumbleWindow(numOfRow)
        }
      }

      benchmark.run()

      val benchmark2 = new Benchmark("sliding windows",
        numOfRow, minTime = 10.seconds, output = output)

      benchmark2.addCase("old logic") { _ =>
        withSQLConf(TIME_WINDOW_NEW_LOGIC.key -> "false") {
          runSlidingWindow(numOfRow)
        }
      }

      benchmark2.addCase("new logic") { _ =>
        withSQLConf(TIME_WINDOW_NEW_LOGIC.key -> "true") {
          runSlidingWindow(numOfRow)
        }
      }

      benchmark2.run()
    }
  }

  private def runTumbleWindow(numOfRow: Int): Unit = {
    spark.range(numOfRow)
      .selectExpr("CAST(id AS timestamp) AS time")
      .select(window(col("time"), "12 seconds", "12 seconds", "2 seconds"))
      .count()
  }

  private def runSlidingWindow(numOfRow: Int): Unit = {
    spark.range(numOfRow)
      .selectExpr("CAST(id AS timestamp) AS time")
      .select(window(col("time"), "17 seconds", "5 seconds", "2 seconds"))
      .count()
  }
}

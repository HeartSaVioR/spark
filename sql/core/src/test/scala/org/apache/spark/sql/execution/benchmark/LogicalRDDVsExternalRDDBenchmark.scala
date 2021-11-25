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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row

object LogicalRDDVsExternalRDDBenchmark extends SqlBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 10000000
    runBenchmark("Typed Dataset") {
      val baseDF = spark.range(N) // Dataset[java.lang.Long]
      val encoder = baseDF.exprEnc

      val benchmark = new Benchmark("LogicalRDD vs ExternalRDD", N, minNumIters = 100,
        output = output)

      benchmark.addCase("LogicalRDD") { _ =>
        val rdd = baseDF.queryExecution.toRdd
        implicit val enc = encoder
        val ds = baseDF.sparkSession.internalCreateDataFrame(rdd, baseDF.schema).as[java.lang.Long]
        ds.write.mode("overwrite").format("noop").save()
      }

      benchmark.addCase("ExternalRDD") { _ =>
        val resolvedEncoder = encoder.resolveAndBind(
          baseDF.logicalPlan.output,
          baseDF.sparkSession.sessionState.analyzer)
        val fromRow = resolvedEncoder.createDeserializer()
        val rdd = baseDF.queryExecution.toRdd.map[java.lang.Long](fromRow)(encoder.clsTag)
        val ds = baseDF.sparkSession.createDataset(rdd)(encoder)
        ds.write.mode("overwrite").format("noop").save()
      }

      benchmark.run()
    }

    runBenchmark("Untyped Dataset") {
      val baseDF = spark.range(N).toDF() // Dataset[Row]
      val encoder = baseDF.exprEnc

      val benchmark = new Benchmark("LogicalRDD vs ExternalRDD", N, minNumIters = 100,
        output = output)

      benchmark.addCase("LogicalRDD") { _ =>
        val rdd = baseDF.queryExecution.toRdd
        implicit val enc = encoder
        val ds = baseDF.sparkSession.internalCreateDataFrame(rdd, baseDF.schema).as[Row]
        ds.write.mode("overwrite").format("noop").save()
      }

      benchmark.addCase("ExternalRDD") { _ =>
        val resolvedEncoder = encoder.resolveAndBind(
          baseDF.logicalPlan.output,
          baseDF.sparkSession.sessionState.analyzer)
        val fromRow = resolvedEncoder.createDeserializer()
        val rdd = baseDF.queryExecution.toRdd.map[Row](fromRow)(encoder.clsTag)
        val ds = baseDF.sparkSession.createDataset(rdd)(encoder)
        ds.write.mode("overwrite").format("noop").save()
      }

      benchmark.run()
    }
  }
}

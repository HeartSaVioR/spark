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

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Synthetic benchmark for State Store operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/StateStoreBenchmark-results.txt".
 * }}}
 */
object StateStoreBenchmark extends SqlBasedBenchmark {

  // private val maxNumOfRows = 1000000
  private val maxNumOfRows = 1000000
  private val minNumOfRows = 1000000
  private val keySchema = StructType(
    Seq(StructField("key1", StringType, true), StructField("key2", IntegerType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("full scan") {
      val testData = constructTestData(maxNumOfRows)
      var curNumOfRows = minNumOfRows

      while (curNumOfRows <= maxNumOfRows) {
        val curTestData = testData.take(curNumOfRows)

        val benchmark = new Benchmark(s"full scan on $curNumOfRows rows",
          curNumOfRows, output = output)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val inMemoryStore = inMemoryProvider.getStore(0)

        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBStore = rocksDBProvider.getStore(0)

        curTestData.foreach { case (key, value) =>
          inMemoryStore.put(key, value)
          rocksDBStore.put(key, value)
        }

        val newVersionForInMemory = inMemoryStore.commit()
        val newVersionForRocksDB = rocksDBStore.commit()

        val evictRate = 10 // 1/10

        benchmark.addTimerCase("HDFSBackedStateStoreProvider", 1000) { timer =>
          val inMemoryStore2 = inMemoryProvider.getStore(newVersionForInMemory)

          timer.startTiming()
          inMemoryStore2.iterator().foreach { r =>
            if (r.key.getInt(1) % evictRate == 0) {
              inMemoryStore2.remove(r.key)
            }
          }
          timer.stopTiming()

          inMemoryStore2.abort()
        }

        benchmark.addTimerCase("RocksDBStateStoreProvider", 1000) { timer =>
          val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

          timer.startTiming()
          rocksDBStore2.iterator().foreach { r =>
            if (r.key.getInt(1) % evictRate == 0) {
              rocksDBStore2.remove(r.key)
            }
          }
          timer.stopTiming()

          rocksDBStore2.abort()
        }

        benchmark.run()

        inMemoryProvider.close()
        rocksDBProvider.close()

        curNumOfRows *= 10
      }
    }
  }

  private def constructTestData(numRows: Int): Seq[(UnsafeRow, UnsafeRow)] = {
    (1 to numRows).map { idx =>
      val keyRow = new GenericInternalRow(2)
      keyRow.update(0, UTF8String.fromString("a"))
      keyRow.setInt(1, idx)
      val valueRow = new GenericInternalRow(1)
      valueRow.setInt(0, idx)

      val keyUnsafeRow = keyProjection(keyRow).copy()
      val valueUnsafeRow = valueProjection(valueRow).copy()

      (keyUnsafeRow, valueUnsafeRow)
    }
  }

  private def newHDFSBackedStateStoreProvider(): StateStoreProvider = {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new HDFSBackedStateStoreProvider()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.sql.streaming.stateStore.compression.codec", "zstd")
    val storeConf = new StateStoreConf(sqlConf)
    provider.init(
      storeId, keySchema, valueSchema, numColsPrefixKey = 0,
      storeConf, new Configuration)
    provider
  }

  private def newRocksDBStateProvider(): StateStoreProvider = {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new RocksDBStateStoreProvider()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.sql.streaming.stateStore.compression.codec", "zstd")
    val storeConf = new StateStoreConf(sqlConf)
    provider.init(
      storeId, keySchema, valueSchema, numColsPrefixKey = 0,
      storeConf, new Configuration)
    provider
  }

  private def newDir(): String = Utils.createTempDir().toString
}

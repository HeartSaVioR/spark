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
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
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

  private val numOfRows: Seq[Int] = Seq(250000, 500000, 750000, 1000000)

  // 50%, 25%, 10%, 5%, 1%, no update
  private val updateRates: Seq[Int] = Seq(50, 25, 10, 5, 1, 0)

  // 50%, 25%, 10%, 5%, 1%, no evict
  private val evictRates: Seq[(Int, Int)] = Seq((2, 50), (4, 25), (10, 10), (20, 5), (100, 1),
    (Int.MaxValue, 0))

  private val keySchema = StructType(
    Seq(StructField("key1", StringType, true), StructField("key2", IntegerType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("simulate eviction") {
      val testData = constructTestData(numOfRows.last)

      numOfRows.foreach { numOfRow =>
        val curData = testData.take(numOfRow)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val inMemoryStore = inMemoryProvider.getStore(0)

        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBStore = rocksDBProvider.getStore(0)

        curData.foreach { case (key, value) =>
          inMemoryStore.put(key, value)
          rocksDBStore.put(key, value)
        }

        val newVersionForInMemory = inMemoryStore.commit()
        val newVersionForRocksDB = rocksDBStore.commit()

        updateRates.foreach { updateRate =>
          val numRowsUpdate = numOfRow / 100 * updateRate

          evictRates.foreach { case (evictModVal, evictRate) =>
            val benchmark = new Benchmark(s"simulating evict on $numOfRow rows, update " +
              s"$numRowsUpdate rows ($updateRate %), evict rate $evictRate %",
              numOfRow, output = output)

            benchmark.addTimerCase("HDFSBackedStateStoreProvider", 1000) { timer =>
              val inMemoryStore2 = inMemoryProvider.getStore(newVersionForInMemory)

              timer.startTiming()
              insertRows(inMemoryStore2, numRows = numRowsUpdate, minIdx = numOfRows.last)
              evictAsFullScanAndRemove(inMemoryStore2, evictModVal)
              timer.stopTiming()

              inMemoryStore2.abort()
            }

            benchmark.addTimerCase("RocksDBStateStoreProvider", 1000) { timer =>
              val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

              timer.startTiming()
              insertRows(rocksDBStore2, numRows = numRowsUpdate, minIdx = numOfRows.last)
              evictAsFullScanAndRemove(rocksDBStore2, evictModVal)
              timer.stopTiming()

              rocksDBStore2.abort()
            }

            benchmark.run()
          }

          inMemoryProvider.close()
          rocksDBProvider.close()
        }
      }
    }
  }

  private def insertRows(store: StateStore, numRows: Int, minIdx: Int): Unit = {

  }

  private def evictAsScanningIndexAndRemove(store: StateStore, evictMod: Int): Unit = {

  }

  private def evictAsFullScanAndRemove(store: StateStore, evictMod: Int): Unit = {
    store.iterator().foreach { r =>
      if (r.key.getInt(1) % evictMod == 0) {
        store.remove(r.key)
      }
    }
  }

  // FIXME: should the size of key / value be variables?
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

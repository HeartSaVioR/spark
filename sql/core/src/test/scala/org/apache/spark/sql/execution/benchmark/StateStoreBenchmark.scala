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

import java.{util => jutil}

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

  private val numOfRows: Seq[Int] = Seq(10000, 50000, 100000, 500000, 1000000)

  // 100%, 75%, 50%, 25%, 10%, 5%, 1%, no update
  // rate is relative to the number of rows in prev. batch
  private val updateRates: Seq[Int] = Seq(100, 75, 50, 25, 10, 5, 1, 0)

  // 100%, 75%, 50%, 25%, 10%, 5%, 1%, no evict
  // rate is relative to the number of rows in prev. batch
  private val evictRates: Seq[Int] = Seq(100, 75, 50, 25, 10, 5, 1, 0)

  private val keySchema = StructType(
    Seq(StructField("key1", StringType, true), StructField("key2", IntegerType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val testData = constructTestData(numOfRows.last)

    runBenchmark("simulate eviction") {
      numOfRows.foreach { numOfRow =>
        val curData = testData.take(numOfRow)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val inMemoryStore = inMemoryProvider.getStore(0)

        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBStore = rocksDBProvider.getStore(0)

        val indexForInMemoryStore = new jutil.concurrent.ConcurrentSkipListMap[
          Int, jutil.List[UnsafeRow]]()
        val indexForRocksDBStore = new jutil.concurrent.ConcurrentSkipListMap[
          Int, jutil.List[UnsafeRow]]()

        updateRowsWithSortedMapIndex(inMemoryStore, indexForInMemoryStore, curData)
        updateRowsWithSortedMapIndex(rocksDBStore, indexForRocksDBStore, curData)

        assert(indexForInMemoryStore.size() == numOfRow)
        assert(indexForRocksDBStore.size() == numOfRow)

        val newVersionForInMemory = inMemoryStore.commit()
        val newVersionForRocksDB = rocksDBStore.commit()

        val rowsToUpdate = constructTestData(numOfRow / 100 * updateRates.last,
          minIdx = numOfRow + 1)
        updateRates.foreach { updateRate =>
          val numRowsUpdate = numOfRow / 100 * updateRate
          val curRowsToUpdate = rowsToUpdate.take(numRowsUpdate)

          evictRates.foreach { evictRate =>
            val maxIdxToEvict = numOfRow / 100 * evictRate

            val benchmark = new Benchmark(s"simulating evict on $numOfRow rows, update " +
              s"$numRowsUpdate rows ($updateRate %), evict $maxIdxToEvict rows ($evictRate %)",
              numOfRow, output = output)

            benchmark.addTimerCase("HDFSBackedStateStoreProvider", 1000) { timer =>
              val inMemoryStore2 = inMemoryProvider.getStore(newVersionForInMemory)

              timer.startTiming()
              updateRows(inMemoryStore2, curRowsToUpdate)
              evictAsFullScanAndRemove(inMemoryStore2, maxIdxToEvict)
              timer.stopTiming()

              inMemoryStore2.abort()
            }

            benchmark.addTimerCase("HDFSBackedStateStoreProvider - sorted map index",
              1000) { timer =>

              val inMemoryStore2 = inMemoryProvider.getStore(newVersionForInMemory)

              val curIndex = new jutil.concurrent.ConcurrentSkipListMap[Int,
                jutil.List[UnsafeRow]]()
              curIndex.putAll(indexForInMemoryStore)

              assert(curIndex.size() == numOfRow)

              timer.startTiming()
              updateRowsWithSortedMapIndex(inMemoryStore2, curIndex, curRowsToUpdate)

              assert(curIndex.size() == numOfRow + curRowsToUpdate.size)

              evictAsScanSortedMapIndexAndRemove(inMemoryStore2, curIndex, maxIdxToEvict)
              timer.stopTiming()

              assert(curIndex.size() == numOfRow + curRowsToUpdate.size - maxIdxToEvict)

              curIndex.clear()

              inMemoryStore2.abort()
            }

            benchmark.addTimerCase("RocksDBStateStoreProvider", 1000) { timer =>
              val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

              timer.startTiming()
              updateRows(rocksDBStore2, curRowsToUpdate)
              evictAsFullScanAndRemove(rocksDBStore2, maxIdxToEvict)
              timer.stopTiming()

              rocksDBStore2.abort()
            }

            benchmark.addTimerCase("RocksDBStateStoreProvider - sorted map index",
              1000) { timer =>

              val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

              val curIndex = new jutil.concurrent.ConcurrentSkipListMap[Int,
                jutil.List[UnsafeRow]]()
              curIndex.putAll(indexForRocksDBStore)

              assert(curIndex.size() == numOfRow)

              timer.startTiming()
              updateRowsWithSortedMapIndex(rocksDBStore2, curIndex, curRowsToUpdate)

              assert(curIndex.size() == numOfRow + curRowsToUpdate.size)

              evictAsScanSortedMapIndexAndRemove(rocksDBStore2, curIndex, maxIdxToEvict)
              timer.stopTiming()

              assert(curIndex.size() == numOfRow + curRowsToUpdate.size - maxIdxToEvict)

              curIndex.clear()

              rocksDBStore2.abort()
            }

            benchmark.run()
          }
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
      }
    }
  }

  private def updateRows(
      store: StateStore,
      rows: Seq[(UnsafeRow, UnsafeRow)]): Unit = {
    rows.foreach { case (key, value) =>
      store.put(key, value)
    }
  }

  private def evictAsFullScanAndRemove(
      store: StateStore,
      maxIdxToEvict: Int): Unit = {
    store.iterator().foreach { r =>
      if (r.key.getInt(1) < maxIdxToEvict) {
        store.remove(r.key)
      }
    }
  }

  private def updateRowsWithSortedMapIndex(
      store: StateStore,
      index: jutil.SortedMap[Int, jutil.List[UnsafeRow]],
      rows: Seq[(UnsafeRow, UnsafeRow)]): Unit = {
    rows.foreach { case (key, value) =>
      val idx = key.getInt(1)

      // TODO: rewrite this in atomic way?
      if (index.containsKey(idx)) {
        val list = index.get(idx)
        list.add(key)
      } else {
        val list = new jutil.ArrayList[UnsafeRow]()
        list.add(key)
        index.put(idx, list)
      }

      store.put(key, value)
    }
  }

  private def evictAsScanSortedMapIndexAndRemove(
      store: StateStore,
      index: jutil.SortedMap[Int, jutil.List[UnsafeRow]],
      maxIdxToEvict: Int): Unit = {
    val keysToRemove = index.headMap(maxIdxToEvict + 1)
    val keysToRemoveIter = keysToRemove.entrySet().iterator()
    while (keysToRemoveIter.hasNext) {
      val entry = keysToRemoveIter.next()
      val keys = entry.getValue
      val keysIter = keys.iterator()
      while (keysIter.hasNext) {
        val key = keysIter.next()
        store.remove(key)
      }
      keys.clear()
      keysToRemoveIter.remove()
    }
  }

  // FIXME: should the size of key / value be variables?
  private def constructTestData(numRows: Int, minIdx: Int = 0): Seq[(UnsafeRow, UnsafeRow)] = {
    (1 to numRows).map { idx =>
      val keyRow = new GenericInternalRow(2)
      keyRow.update(0, UTF8String.fromString("a"))
      keyRow.setInt(1, minIdx + idx)
      val valueRow = new GenericInternalRow(1)
      valueRow.setInt(0, minIdx + idx)

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

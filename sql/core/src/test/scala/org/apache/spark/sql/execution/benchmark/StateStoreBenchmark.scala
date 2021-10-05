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
import org.rocksdb.RocksDBException

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, StatefulOperatorContext, StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
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

  private val numOfRows: Seq[Int] = Seq(10000, 50000, 100000) // Seq(10000, 100000, 1000000)

  // 200%, 100%, 50%, 25%, 10%, 5%, 1%, no update
  // rate is relative to the number of rows in prev. batch
  private val updateRates: Seq[Int] = Seq(25, 10, 5) // Seq(200, 100, 50, 25, 10, 5, 1, 0)

  // 100%, 50%, 25%, 10%, 5%, 1%, no evict
  // rate is relative to the number of rows in prev. batch
  private val evictRates: Seq[Int] = Seq(100, 50, 25, 10, 5, 1, 0)

  private val keySchema = StructType(
    Seq(StructField("key1", IntegerType, true), StructField("key2", TimestampType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  private def runEvictBenchmark(): Unit = {
    runBenchmark("evict rows") {
      val numOfRows = Seq(100000) // Seq(1000, 10000, 100000)
      val numOfTimestamps = Seq(100, 1000, 10000, 100000)
      val numOfEvictionRates = Seq(100, 50, 10, 0) // Seq(100, 75, 50, 25, 1, 0)

      numOfRows.foreach { numOfRow =>
        numOfTimestamps.foreach { numOfTimestamp =>
          val timestampsInMicros = (0L until numOfTimestamp).map(ts => ts * 1000L).toList

          val testData = constructRandomizedTestData(numOfRow, timestampsInMicros, 0)

          val rocksDBProvider = newRocksDBStateProviderWithEventTimeIdx()
          val rocksDBStore = rocksDBProvider.getStore(0)
          updateRows(rocksDBStore, testData)

          val committedVersion = try {
            rocksDBStore.commit()
          } catch {
            case exc: RocksDBException =>
              // scalastyle:off println
              System.out.println(s"Exception in RocksDB happen! ${exc.getMessage} / " +
                s"status: ${exc.getStatus.getState} / ${exc.getStatus.getCodeString}" )
              exc.printStackTrace()
              throw exc
              // scalastyle:on println
          }

          numOfEvictionRates.foreach { numOfEvictionRate =>
            val numOfRowsToEvict = numOfRow * numOfEvictionRate / 100

            // scalastyle:off println
            /*
            System.out.println(s"numOfRowsToEvict: $numOfRowsToEvict / " +
              s"timestampsInMicros: $timestampsInMicros / " +
              s"numOfEvictionRate: $numOfEvictionRate / " +
              s"numOfTimestamp: $numOfTimestamp / " +
              s"take: ${numOfTimestamp * numOfEvictionRate / 100}")
            */
            // scalastyle:on println

            val maxTimestampToEvictInMillis = timestampsInMicros
              .take(numOfTimestamp * numOfEvictionRate / 100)
              .lastOption.map(_ / 1000).getOrElse(-1L)

            val benchmark = new Benchmark(s"evicting $numOfRowsToEvict rows " +
              s"(max timestamp to evict in millis: $maxTimestampToEvictInMillis) " +
              s"from $numOfRow rows with $numOfTimestamp timestamps " +
              s"(${numOfRow / numOfTimestamp} rows" +
              s" for the same timestamp)",
              numOfRow, minNumIters = 1000, output = output)

            benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
              val rocksDBStore = rocksDBProvider.getStore(committedVersion)

              timer.startTiming()
              evictAsFullScanAndRemove(rocksDBStore, maxTimestampToEvictInMillis)
              timer.stopTiming()

              rocksDBStore.abort()
            }

            benchmark.addTimerCase("RocksDBStateStoreProvider with event time idx") { timer =>
              val rocksDBStore = rocksDBProvider.getStore(committedVersion)

              timer.startTiming()
              evictAsNewEvictApi(rocksDBStore, maxTimestampToEvictInMillis)
              timer.stopTiming()

              rocksDBStore.abort()
            }

            benchmark.run()
          }

          rocksDBProvider.close()
        }
      }
    }
  }

  private def runPutBenchmark(): Unit = {
    runBenchmark("put rows") {
      val numOfRows = Seq(10000) // Seq(1000, 10000, 100000)
      val numOfTimestamps = Seq(100, 1000, 10000) // Seq(1, 10, 100, 1000, 10000)
      numOfRows.foreach { numOfRow =>
        numOfTimestamps.foreach { numOfTimestamp =>
          val timestamps = (0L until numOfTimestamp).map(ts => ts * 1000L).toList

          val testData = constructRandomizedTestData(numOfRow, timestamps, 0)

          val rocksDBProvider = newRocksDBStateProvider()
          val rocksDBWithIdxProvider = newRocksDBStateProviderWithEventTimeIdx()

          val benchmark = new Benchmark(s"putting $numOfRow rows, with $numOfTimestamp " +
            s"timestamps (${numOfRow / numOfTimestamp} rows for the same timestamp)",
            numOfRow, minNumIters = 1000, output = output)

          benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
            val rocksDBStore = rocksDBProvider.getStore(0)

            timer.startTiming()
            updateRows(rocksDBStore, testData)
            timer.stopTiming()

            rocksDBStore.abort()
          }

          benchmark.addTimerCase("RocksDBStateStoreProvider with event time idx") { timer =>
            val rocksDBWithIdxStore = rocksDBWithIdxProvider.getStore(0)

            timer.startTiming()
            updateRows(rocksDBWithIdxStore, testData)
            timer.stopTiming()

            rocksDBWithIdxStore.abort()
          }

          benchmark.run()

          rocksDBProvider.close()
          rocksDBWithIdxProvider.close()
        }
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // runPutBenchmark()
    runEvictBenchmark()

    /*
    val testData = constructRandomizedTestData(numOfRows.max)

    skip("scanning and comparing") {
      numOfRows.foreach { numOfRow =>
        val curData = testData.take(numOfRow)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val inMemoryStore = inMemoryProvider.getStore(0)

        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBStore = rocksDBProvider.getStore(0)

        updateRows(inMemoryStore, curData)
        updateRows(rocksDBStore, curData)

        val newVersionForInMemory = inMemoryStore.commit()
        val newVersionForRocksDB = rocksDBStore.commit()

        val benchmark = new Benchmark(s"scanning and comparing $numOfRow rows",
          numOfRow, minNumIters = 1000, output = output)

        benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
          val inMemoryStore2 = inMemoryProvider.getStore(newVersionForInMemory)

          timer.startTiming()
          // NOTE: the latency would be quite similar regardless of the rate of eviction
          // as we don't remove the actual row, so I simply picked 10 %
          fullScanAndCompareTimestamp(inMemoryStore2, (numOfRow * 0.1).toInt)
          timer.stopTiming()

          inMemoryStore2.abort()
        }

        benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
          val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

          timer.startTiming()
          // NOTE: the latency would be quite similar regardless of the rate of eviction
          // as we don't remove the actual row, so I simply picked 10 %
          fullScanAndCompareTimestamp(rocksDBStore2, (numOfRow * 0.1).toInt)
          timer.stopTiming()

          rocksDBStore2.abort()
        }

        benchmark.run()

        inMemoryProvider.close()
        rocksDBProvider.close()
      }
    }

    // runBenchmark("simulate full operations on eviction") {
    skip("simulate full operations on eviction") {
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

        val rowsToUpdate = constructRandomizedTestData(numOfRow / 100 * updateRates.max,
          minIdx = numOfRow + 1)

        updateRates.foreach { updateRate =>
          val numRowsUpdate = numOfRow / 100 * updateRate
          val curRowsToUpdate = rowsToUpdate.take(numRowsUpdate)

          evictRates.foreach { evictRate =>
            val maxIdxToEvict = numOfRow / 100 * evictRate

            val benchmark = new Benchmark(s"simulating evict on $numOfRow rows, update " +
              s"$numRowsUpdate rows ($updateRate %), evict $maxIdxToEvict rows ($evictRate %)",
              numOfRow, minNumIters = 100, output = output)

            benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
              val inMemoryStore2 = inMemoryProvider.getStore(newVersionForInMemory)

              timer.startTiming()
              updateRows(inMemoryStore2, curRowsToUpdate)
              evictAsFullScanAndRemove(inMemoryStore2, maxIdxToEvict)
              timer.stopTiming()

              inMemoryStore2.abort()
            }

            benchmark.addTimerCase("HDFSBackedStateStoreProvider - sorted map index") { timer =>

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

            benchmark.run()

            val benchmark2 = new Benchmark(s"simulating evict on $numOfRow rows, update " +
              s"$numRowsUpdate rows ($updateRate %), evict $maxIdxToEvict rows ($evictRate %)",
              numOfRow, minNumIters = 100, output = output)

            benchmark2.addTimerCase("RocksDBStateStoreProvider") { timer =>
              val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

              timer.startTiming()
              updateRows(rocksDBStore2, curRowsToUpdate)
              evictAsFullScanAndRemove(rocksDBStore2, maxIdxToEvict)
              timer.stopTiming()

              rocksDBStore2.abort()
            }

            benchmark2.addTimerCase("RocksDBStateStoreProvider - sorted map index") { timer =>

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

            benchmark2.run()
          }
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
      }
    }

    // runBenchmark("simulate full operations on eviction") {
    skip("simulate full operations on eviction - scannable index") {
      numOfRows.foreach { numOfRow =>
        val curData = testData.take(numOfRow)

        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBStore = rocksDBProvider.getStore(0)

        val rocksDBWithIdxProvider = newRocksDBStateProviderWithEventTimeIdx()
        val rocksDBWithIdxStore = rocksDBWithIdxProvider.getStore(0)

        updateRows(rocksDBStore, curData)
        updateRows(rocksDBWithIdxStore, curData)

        val newVersionForRocksDB = rocksDBStore.commit()
        val newVersionForRocksDBWithIdx = rocksDBWithIdxStore.commit()

        val rowsToUpdate = constructRandomizedTestData(numOfRow / 100 * updateRates.max,
          minIdx = numOfRow + 1)

        updateRates.foreach { updateRate =>
          val numRowsUpdate = numOfRow / 100 * updateRate
          val curRowsToUpdate = rowsToUpdate.take(numRowsUpdate)

          evictRates.foreach { evictRate =>
            val maxIdxToEvict = numOfRow / 100 * evictRate

            val benchmark = new Benchmark(s"simulating evict on $numOfRow rows, update " +
              s"$numRowsUpdate rows ($updateRate %), evict $maxIdxToEvict rows ($evictRate %)",
              numOfRow, minNumIters = 100, output = output)

            benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
              val rocksDBStore2 = rocksDBProvider.getStore(newVersionForRocksDB)

              timer.startTiming()
              updateRows(rocksDBStore2, curRowsToUpdate)
              evictAsFullScanAndRemove(rocksDBStore2, maxIdxToEvict)
              // evictAsNewEvictApi(rocksDBStore2, maxIdxToEvict)
              timer.stopTiming()

              rocksDBStore2.abort()
            }

            benchmark.addTimerCase("RocksDBStateStoreProvider with event time idx") { timer =>
              val rocksDBWithIdxStore2 = rocksDBWithIdxProvider.getStore(
                newVersionForRocksDBWithIdx)

              timer.startTiming()
              updateRows(rocksDBWithIdxStore2, curRowsToUpdate)
              evictAsNewEvictApi(rocksDBWithIdxStore2, maxIdxToEvict)
              timer.stopTiming()

              rocksDBWithIdxStore2.abort()
            }

            benchmark.run()
          }
        }

        rocksDBProvider.close()
        rocksDBWithIdxProvider.close()
      }
    }

    runBenchmark("put rows") {
      numOfRows.foreach { numOfRow =>
        val curData = testData.take(numOfRow)

        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBWithIdxProvider = newRocksDBStateProviderWithEventTimeIdx()

        val benchmark = new Benchmark(s"putting $numOfRow rows",
          numOfRow, minNumIters = 1000, output = output)

        benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
          val rocksDBStore = rocksDBProvider.getStore(0)

          timer.startTiming()
          updateRows(rocksDBStore, curData)
          timer.stopTiming()

          rocksDBStore.abort()
        }

        benchmark.addTimerCase("RocksDBStateStoreProvider with event time idx") { timer =>
          val rocksDBWithIdxStore = rocksDBWithIdxProvider.getStore(0)

          timer.startTiming()
          updateRows(rocksDBWithIdxStore, curData)
          timer.stopTiming()

          rocksDBWithIdxStore.abort()
        }

        benchmark.run()

        rocksDBProvider.close()
        rocksDBWithIdxProvider.close()
      }
    }
     */
  }

  final def skip(benchmarkName: String)(func: => Any): Unit = {
    output.foreach(_.write(s"$benchmarkName is skipped".getBytes))
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
      maxTimestampToEvict: Long): Unit = {
    store.iterator().foreach { r =>
      if (r.key.getLong(1) < maxTimestampToEvict) {
        store.remove(r.key)
      }
    }
  }

  private def evictAsNewEvictApi(
      store: StateStore,
      maxTimestampToEvict: Long): Unit = {
    store.evictOnWatermark(maxTimestampToEvict, pair => {
      pair.key.getLong(1) < maxTimestampToEvict
    }).foreach { _ => }
  }

  private def fullScanAndCompareTimestamp(
      store: StateStore,
      maxIdxToEvict: Int): Unit = {
    var i: Long = 0
    store.iterator().foreach { r =>
      if (r.key.getInt(1) < maxIdxToEvict) {
        // simply to avoid the "if statement" to be no-op
        i += 1
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
      keyRow.setInt(0, 1)
      keyRow.setLong(1, (minIdx + idx) * 1000L) // microseconds
      val valueRow = new GenericInternalRow(1)
      valueRow.setInt(0, minIdx + idx)

      val keyUnsafeRow = keyProjection(keyRow).copy()
      val valueUnsafeRow = valueProjection(valueRow).copy()

      (keyUnsafeRow, valueUnsafeRow)
    }
  }

  // This prevents created keys to be in order, which may affect the performance on RocksDB.
  private def constructRandomizedTestData(
      numRows: Int,
      timestamps: List[Long],
      minIdx: Int = 0): Seq[(UnsafeRow, UnsafeRow)] = {
    assert(numRows >= timestamps.length)
    assert(numRows % timestamps.length == 0)

    (1 to numRows).map { idx =>
      val keyRow = new GenericInternalRow(2)
      keyRow.setInt(0, Random.nextInt(Int.MaxValue))
      keyRow.setLong(1, timestamps((minIdx + idx) % timestamps.length)) // microseconds
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
      storeId, keySchema, valueSchema, StatefulOperatorContext(),
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
      storeId, keySchema, valueSchema, StatefulOperatorContext(),
      storeConf, new Configuration)
    provider
  }

  private def newRocksDBStateProviderWithEventTimeIdx(): StateStoreProvider = {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new RocksDBStateStoreProvider()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.sql.streaming.stateStore.compression.codec", "zstd")
    val storeConf = new StateStoreConf(sqlConf)
    provider.init(
      storeId, keySchema, valueSchema, StatefulOperatorContext(eventTimeColIdx = Array(1)),
      storeConf, new Configuration)
    provider
  }

  private def newDir(): String = Utils.createTempDir().toString
}

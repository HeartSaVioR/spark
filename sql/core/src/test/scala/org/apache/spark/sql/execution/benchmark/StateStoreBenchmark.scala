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
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider, RocksDBStateStoreProviderNew, StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
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

  private val keySchema = StructType(
    Seq(StructField("key1", IntegerType, true), StructField("key2", TimestampType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // runPutBenchmark()
    // runDeleteBenchmark()
    // runEvictBenchmark()
    // runPutWithCommitBenchmark()
    // runGetAndOverwriteBenchmark()
    runAccurateLatencyOverheadBenchmark()
  }

  final def skip(benchmarkName: String)(func: => Any): Unit = {
    output.foreach(_.write(s"$benchmarkName is skipped".getBytes))
  }

  private def runAccurateLatencyOverheadBenchmark(): Unit = {
    runBenchmark("additional operations") {
      val numRows = 1000000
      val benchmark = new Benchmark(s"additional operations",
        numRows, minNumIters = 1000, output = output)

      def latencyOverhead(sampleInterval: Int): Unit = {
        var cursor = 0L
        var totalNs = 0L
        (1 to numRows).foreach { _ =>
          if (sampleInterval == 0) {
            val startTimeNs = System.nanoTime()
            val elapsedNs = System.nanoTime() - startTimeNs
            totalNs += elapsedNs
          } else {
            if (cursor % sampleInterval == 0) {
              val startTimeNs = System.nanoTime()
              val elapsedNs = System.nanoTime() - startTimeNs
              totalNs += elapsedNs * sampleInterval
            }
            cursor += 1
          }
        }
      }

      benchmark.addCase("no sample") { _ =>
        latencyOverhead(0)
      }

      benchmark.addCase("1/5 sample") { _ =>
        latencyOverhead(5)
      }

      benchmark.addCase("1/10 sample") { _ =>
        latencyOverhead(10)
      }

      benchmark.addCase("1/50 sample") { _ =>
        latencyOverhead(50)
      }

      benchmark.addCase("1/100 sample") { _ =>
        latencyOverhead(100)
      }

      benchmark.run()
    }
  }

  private def runPutWithCommitBenchmark(): Unit = {
    runBenchmark("put rows & commit") {
      val numOfRowToPutPerVersion = 10000
      val numVersions = 100
      val benchmark = new Benchmark(s"putting $numOfRowToPutPerVersion and committing" +
        s" per version, $numVersions versions",
        numOfRowToPutPerVersion * 100, minNumIters = 100, output = output)

      benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        var curVersion = 0L
        (1 to numVersions).foreach { _ =>
          val testData = constructRandomizedTestData(numOfRowToPutPerVersion,
            (1 to numOfRowToPutPerVersion).map(_ * 1000L).toList, 0)
          val inMemoryStore = inMemoryProvider.getStore(curVersion)

          // include the time to update rows, because there is an overhead on writing
          // changelog while updating. the actual latency on updating row is trivial.
          timer.startTiming()
          updateRows(inMemoryStore, testData)
          curVersion = inMemoryStore.commit()
          timer.stopTiming()
        }
        inMemoryProvider.close()
      }

      benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
        val rocksDBProvider = newRocksDBStateProvider()
        var curVersion = 0L
        (1 to numVersions).foreach { _ =>
          val testData = constructRandomizedTestData(numOfRowToPutPerVersion,
            (1 to numOfRowToPutPerVersion).map(_ * 1000L).toList, 0)
          val rocksDBStore = rocksDBProvider.getStore(curVersion)

          updateRows(rocksDBStore, testData)

          timer.startTiming()
          curVersion = rocksDBStore.commit()
          timer.stopTiming()
        }
        rocksDBProvider.close()
      }

      benchmark.run()
    }
  }

  private def runPutBenchmark(): Unit = {
    runBenchmark("put rows") {
      val numOfRows = Seq(10000)
      val overwriteRates = Seq(100, 75, 50, 25, 10, 5, 0)
      numOfRows.foreach { numOfRow =>
        val testData = constructRandomizedTestData(numOfRow,
          (1 to numOfRow).map(_ * 1000L).toList, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()

        val inMemoryStore = inMemoryProvider.getStore(0)
        updateRows(inMemoryStore, testData)

        val rocksDBStore = rocksDBProvider.getStore(0)
        updateRows(rocksDBStore, testData)

        val committedInMemoryVersion = inMemoryStore.commit()
        val committedVersion = rocksDBStore.commit()

        overwriteRates.foreach { overwriteRate =>
          val numOfRowsToOverwrite = numOfRow * overwriteRate / 100

          val benchmark = new Benchmark(s"putting $numOfRow rows " +
            s"($numOfRowsToOverwrite rows to overwrite - rate $overwriteRate)",
            numOfRow, minNumIters = 10000, output = output)

          val numOfNewRows = numOfRow - numOfRowsToOverwrite
          val newRows = if (numOfNewRows > 0) {
            constructRandomizedTestData(numOfNewRows,
              (1 to numOfNewRows).map(_ * 1000L).toList, 0)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val existingRows = if (numOfRowsToOverwrite > 0) {
            Random.shuffle(testData).take(numOfRowsToOverwrite)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val rowsToPut = Random.shuffle(newRows ++ existingRows)

          benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
            val inMemoryStore = inMemoryProvider.getStore(committedInMemoryVersion)

            timer.startTiming()
            updateRows(inMemoryStore, rowsToPut)
            timer.stopTiming()

            inMemoryStore.abort()
          }

          benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
            val rocksDBStore = rocksDBProvider.getStore(committedVersion)

            timer.startTiming()
            updateRows(rocksDBStore, rowsToPut)
            timer.stopTiming()

            rocksDBStore.abort()
          }

          benchmark.run()
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
      }
    }
  }

  private def runGetAndOverwriteBenchmark(): Unit = {
    runBenchmark("get and overwrite rows (simulate workload on streaming aggregation)") {
      val numOfRows = Seq(10000)
      val overwriteRates = Seq(100, 75, 50, 25, 10, 5, 0)
      numOfRows.foreach { numOfRow =>
        val testData = constructRandomizedTestData(numOfRow,
          (1 to numOfRow).map(_ * 1000L).toList, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()
        val rocksDBProviderNew = newRocksDBStateProviderNew()

        val inMemoryStore = inMemoryProvider.getStore(0)
        updateRows(inMemoryStore, testData)

        val rocksDBStore = rocksDBProvider.getStore(0)
        updateRows(rocksDBStore, testData)

        val rocksDBStoreNew = rocksDBProviderNew.getStore(0)
        updateRows(rocksDBStoreNew, testData)

        val committedInMemoryVersion = inMemoryStore.commit()
        val committedVersion = rocksDBStore.commit()
        val committedVersionNew = rocksDBStoreNew.commit()

        val rowsToOverwrite = testData.map { case (key, value) =>
          // modify the value in the value row to simulate overwrite
          value.setLong(0, value.getLong(0) + 1)
          (key, value)
        }

        val benchmark = new Benchmark(s"get and overwrite $numOfRow rows",
          numOfRow, minNumIters = 1000, output = output)

        benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
          val inMemoryStore = inMemoryProvider.getStore(committedInMemoryVersion)

          // issues "get operation" first to the all keys
          timer.startTiming()
          getRows(inMemoryStore, testData.map(_._1))

          // overwrite all keys
          updateRows(inMemoryStore, rowsToOverwrite)
          timer.stopTiming()

          inMemoryStore.abort()
        }

        benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
          val rocksDBStore = rocksDBProvider.getStore(committedVersion)

          // issues "get operation" first to the all keys
          timer.startTiming()
          getRows(rocksDBStore, testData.map(_._1))

          // overwrite all keys
          updateRows(rocksDBStore, rowsToOverwrite)
          timer.stopTiming()

          rocksDBStore.abort()
        }

        benchmark.addTimerCase("New RocksDBStateStoreProvider") { timer =>
          val rocksDBStoreNew = rocksDBProviderNew.getStore(committedVersion)

          // issues "get operation" first to the all keys
          timer.startTiming()
          getRows(rocksDBStoreNew, testData.map(_._1))

          // overwrite all keys
          updateRows(rocksDBStoreNew, rowsToOverwrite)
          timer.stopTiming()

          rocksDBStoreNew.abort()
        }

        benchmark.run()

        inMemoryProvider.close()
        rocksDBProvider.close()
        rocksDBProviderNew.close()
      }
    }
  }

  private def runDeleteBenchmark(): Unit = {
    runBenchmark("delete rows") {
      val numOfRows = Seq(10000)
      val nonExistRates = Seq(100, 75, 50, 25, 10, 5, 0)
      numOfRows.foreach { numOfRow =>
        val testData = constructRandomizedTestData(numOfRow,
          (1 to numOfRow).map(_ * 1000L).toList, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()

        val inMemoryStore = inMemoryProvider.getStore(0)
        updateRows(inMemoryStore, testData)

        val rocksDBStore = rocksDBProvider.getStore(0)
        updateRows(rocksDBStore, testData)

        val committedInMemoryVersion = inMemoryStore.commit()
        val committedVersion = rocksDBStore.commit()

        nonExistRates.foreach { nonExistRate =>
          val numOfRowsNonExist = numOfRow * nonExistRate / 100

          val benchmark = new Benchmark(s"trying to delete $numOfRow rows " +
            s"from $numOfRow rows" +
            s"($numOfRowsNonExist rows are non-existing - rate $nonExistRate)",
            numOfRow, minNumIters = 10000, output = output)

          val numOfExistingRows = numOfRow - numOfRowsNonExist
          val nonExistingRows = if (numOfRowsNonExist > 0) {
            constructRandomizedTestData(numOfRowsNonExist,
              (numOfRow + 1 to numOfRow + numOfRowsNonExist).map(_ * 1000L).toList, 0)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val existingRows = if (numOfExistingRows > 0) {
            Random.shuffle(testData).take(numOfExistingRows)
          } else {
            Seq.empty[(UnsafeRow, UnsafeRow)]
          }
          val rowsToDelete = Random.shuffle(nonExistingRows ++ existingRows)

          benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
            val inMemoryStore = inMemoryProvider.getStore(committedInMemoryVersion)

            timer.startTiming()
            deleteRows(inMemoryStore, rowsToDelete.map(_._1))
            timer.stopTiming()

            inMemoryStore.abort()
          }

          benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
            val rocksDBStore = rocksDBProvider.getStore(committedVersion)

            timer.startTiming()
            deleteRows(rocksDBStore, rowsToDelete.map(_._1))
            timer.stopTiming()

            rocksDBStore.abort()
          }

          benchmark.run()
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
      }
    }
  }

  private def runEvictBenchmark(): Unit = {
    runBenchmark("evict rows") {
      val numOfRows = Seq(10000)
      val numOfEvictionRates = Seq(100, 75, 50, 25, 10, 5, 0)

      numOfRows.foreach { numOfRow =>
        val timestampsInMicros = (0L until numOfRow).map(ts => ts * 1000L).toList

        val testData = constructRandomizedTestData(numOfRow, timestampsInMicros, 0)

        val inMemoryProvider = newHDFSBackedStateStoreProvider()
        val rocksDBProvider = newRocksDBStateProvider()

        val inMemoryStore = inMemoryProvider.getStore(0)
        updateRows(inMemoryStore, testData)

        val rocksDBStore = rocksDBProvider.getStore(0)
        updateRows(rocksDBStore, testData)

        val committedInMemoryVersion = inMemoryStore.commit()
        val committedVersion = rocksDBStore.commit()

        numOfEvictionRates.foreach { numOfEvictionRate =>
          val numOfRowsToEvict = numOfRow * numOfEvictionRate / 100
          val maxTimestampToEvictInMillis = timestampsInMicros
            .take(numOfRow * numOfEvictionRate / 100)
            .lastOption.map(_ / 1000).getOrElse(-1L)

          val benchmark = new Benchmark(s"evicting $numOfRowsToEvict rows " +
            s"(maxTimestampToEvictInMillis: $maxTimestampToEvictInMillis) " +
            s"from $numOfRow rows",
            numOfRow, minNumIters = 10000, output = output)

          benchmark.addTimerCase("HDFSBackedStateStoreProvider") { timer =>
            val inMemoryStore = inMemoryProvider.getStore(committedInMemoryVersion)

            timer.startTiming()
            evictAsFullScanAndRemove(inMemoryStore, maxTimestampToEvictInMillis,
              numOfRowsToEvict)
            timer.stopTiming()

            inMemoryStore.abort()
          }

          benchmark.addTimerCase("RocksDBStateStoreProvider") { timer =>
            val rocksDBStore = rocksDBProvider.getStore(committedVersion)

            timer.startTiming()
            evictAsFullScanAndRemove(rocksDBStore, maxTimestampToEvictInMillis,
              numOfRowsToEvict)
            timer.stopTiming()

            rocksDBStore.abort()
          }

          benchmark.run()
        }

        inMemoryProvider.close()
        rocksDBProvider.close()
      }
    }
  }

  private def getRows(store: StateStore, keys: Seq[UnsafeRow]): Seq[UnsafeRow] = {
    keys.map(store.get)
  }

  private def updateRows(
      store: StateStore,
      rows: Seq[(UnsafeRow, UnsafeRow)]): Unit = {
    rows.foreach { case (key, value) =>
      store.put(key, value)
    }
  }

  private def deleteRows(
      store: StateStore,
      rows: Seq[UnsafeRow]): Unit = {
    rows.foreach { key =>
      store.remove(key)
    }
  }

  private def evictAsFullScanAndRemove(
      store: StateStore,
      maxTimestampToEvictMillis: Long,
      expectedNumOfRows: Long): Unit = {
    var removedRows: Long = 0
    store.iterator().foreach { r =>
      if (r.key.getLong(1) <= maxTimestampToEvictMillis * 1000L) {
        store.remove(r.key)
        removedRows += 1
      }
    }
    assert(removedRows == expectedNumOfRows,
      s"expected: $expectedNumOfRows actual: $removedRows")
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
      storeId, keySchema, valueSchema, 0,
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
      storeId, keySchema, valueSchema, 0,
      storeConf, new Configuration)
    provider
  }

  private def newRocksDBStateProviderNew(): StateStoreProvider = {
    val storeId = StateStoreId(newDir(), Random.nextInt(), 0)
    val provider = new RocksDBStateStoreProviderNew()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.sql.streaming.stateStore.compression.codec", "zstd")
    val storeConf = new StateStoreConf(sqlConf)
    provider.init(
      storeId, keySchema, valueSchema, 0,
      storeConf, new Configuration)
    provider
  }

  private def newDir(): String = Utils.createTempDir().toString
}

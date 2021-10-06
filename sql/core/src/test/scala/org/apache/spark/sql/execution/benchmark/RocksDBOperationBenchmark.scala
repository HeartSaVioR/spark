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

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Random

import org.rocksdb.{RocksDB => NativeRocksDB, _}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.state.{RocksDBConf, RocksDBLoader, RocksDBStateEncoder, StateStoreConf}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.util.Utils

object RocksDBOperationBenchmark extends SqlBasedBenchmark {

  RocksDBLoader.loadLibrary()

  private val keySchema = StructType(
    Seq(StructField("key1", IntegerType, true), StructField("key2", TimestampType, true)))
  private val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private val keyProjection = UnsafeProjection.create(keySchema)
  private val valueProjection = UnsafeProjection.create(valueSchema)

  // Java wrapper objects linking to native RocksDB objects
  private val readOptions = new ReadOptions()  // used for gets
  private val writeOptions = new WriteOptions().setSync(true)  // wait for batched write to complete
  private val flushOptions = new FlushOptions().setWaitForFlush(true)  // wait for flush to complete

  private val conf = RocksDBConf(new StateStoreConf())
  private val bloomFilter = new BloomFilter()
  private val tableFormatConfig = new BlockBasedTableConfig()
  tableFormatConfig.setBlockSize(conf.blockSizeKB * 1024)
  tableFormatConfig.setBlockCache(new LRUCache(conf.blockCacheSizeMB * 1024 * 1024))
  tableFormatConfig.setFilterPolicy(bloomFilter)
  tableFormatConfig.setFormatVersion(conf.formatVersion)

  private val dbOptions = new Options() // options to open the RocksDB
  dbOptions.setCreateIfMissing(true)
  dbOptions.setTableFormatConfig(tableFormatConfig)
  dbOptions.setStatistics(new Statistics())

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runMultiGetBenchmark()
  }

  final def skip(benchmarkName: String)(func: => Any): Unit = {
    output.foreach(_.write(s"$benchmarkName is skipped".getBytes))
  }

  private def runMultiGetBenchmark(): Unit = {
    runBenchmark("multi get") {
      val remoteDir = Utils.createTempDir().toString
      new File(remoteDir).delete()  // to make sure that the directory gets created

      val db = NativeRocksDB.open(dbOptions, remoteDir)

      val numOfRows = Seq(100000)
      numOfRows.foreach { numOfRow =>
        val testData = constructRandomizedTestData(numOfRow,
          (1 to numOfRow).map(_ * 1000L).toList, 0)

        val testData1 = testData.take(numOfRow / 2)

        testData1.foreach { case (key, value) =>
          db.put(key, value)
        }

        val testData2 = testData.drop(numOfRow / 2)

        testData2.foreach { case (key, value) =>
          db.put(key, value)
        }

        db.flush(flushOptions)

        val benchmark = new Benchmark(s"get operation against $numOfRow rows",
          numOfRow, minNumIters = 1000, output = output)

        benchmark.addCase("normal get") { _ =>
          testData.foreach { case (key, _) =>
            val v = db.get(key)
            assert(v != null)
          }
        }

        benchmark.addCase("multi get with 1000 keys per each") { _ =>
          testData.grouped(1000).foreach { kvs =>
            val vs = db.multiGetAsList(kvs.map(_._1).toList.asJava)
            assert(vs.size() == kvs.size)
            assert(!vs.asScala.contains(null))
          }
        }

        benchmark.addCase("multi get with 10000 keys per each") { _ =>
          testData.grouped(10000).foreach { kvs =>
            val vs = db.multiGetAsList(kvs.map(_._1).toList.asJava)
            assert(vs.size() == kvs.size)
            assert(!vs.asScala.contains(null))
          }
        }

        benchmark.run()
      }
    }
  }

  // This prevents created keys to be in order, which may affect the performance on RocksDB.
  private def constructRandomizedTestData(
      numRows: Int,
      timestamps: List[Long],
      minIdx: Int = 0): Seq[(Array[Byte], Array[Byte])] = {
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

      (RocksDBStateEncoder.encodeUnsafeRow(keyUnsafeRow),
        RocksDBStateEncoder.encodeUnsafeRow(valueUnsafeRow))
    }
  }

}

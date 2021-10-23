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

package org.apache.spark.sql.execution.streaming.sources

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class IncrementalRowStreamProvider extends SimpleTableProvider with DataSourceRegister {
  import IncrementalRowStreamProvider._

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val rowsPerBatch = options.getLong(ROWS_PER_BATCH, 1)
    if (rowsPerBatch <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$rowsPerBatch'. The option 'rowsPerBatch' must be positive")
    }

    val numPartitions = options.getInt(
      NUM_PARTITIONS, SparkSession.active.sparkContext.defaultParallelism)
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$numPartitions'. The option 'numPartitions' must be positive")
    }

    val startTimestamp = options.getLong(START_TIMESTAMP, 0)
    if (startTimestamp < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$startTimestamp'. The option 'startTimestamp' must be non-negative")
    }

    val advanceMillisPerBatch = options.getInt(ADVANCE_MILLIS_PER_BATCH, 1000)
    if (advanceMillisPerBatch < 0) {
      throw new IllegalArgumentException(
        s"Invalid value '$advanceMillisPerBatch'. The option 'advanceMillisPerBatch' " +
          "must be non-negative")
    }

    new IncrementalRowStreamTable(rowsPerBatch, numPartitions, startTimestamp,
      advanceMillisPerBatch)
  }

  override def shortName(): String = "incremental"
}

class IncrementalRowStreamTable(
    rowsPerBatch: Long,
    numPartitions: Int,
    startTimestamp: Long,
    advanceMillisPerBatch: Int) extends Table with SupportsRead {
  override def name(): String = {
    s"IncrementalRowStream(rowsPerBatch=$rowsPerBatch, numPartitions=$numPartitions)"
  }

  override def schema(): StructType = IncrementalRowStreamProvider.SCHEMA

  // FIXME: continuous mode!
  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = IncrementalRowStreamProvider.SCHEMA

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
      new IncrementalRowStreamMicroBatchStream(rowsPerBatch, numPartitions, startTimestamp,
        advanceMillisPerBatch, options)

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new UnsupportedOperationException("continuous mode is not supported yet!")
    }
  }
}

object IncrementalRowStreamProvider {
  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) :: StructField("value", LongType) :: Nil)

  val VERSION = 1

  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_BATCH = "rowsPerBatch"
  val START_TIMESTAMP = "startTimestamp"
  val ADVANCE_MILLIS_PER_BATCH = "advanceMillisPerBatch"
}

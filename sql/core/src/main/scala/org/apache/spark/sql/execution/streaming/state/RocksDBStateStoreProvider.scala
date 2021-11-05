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

package org.apache.spark.sql.execution.streaming.state

import java.io._

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.util.{NextIterator, Utils}

private[sql] class RocksDBStateStoreProvider
  extends StateStoreProvider with Logging with Closeable {
  import RocksDBStateStoreProvider._

  class RocksDBStateStore(lastVersion: Long, eventTimeColIdx: Array[Int]) extends StateStore {
    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    @volatile private var state: STATE = UPDATING
    @volatile private var isValidated = false

    private val supportEventTimeIndex: Boolean = eventTimeColIdx.nonEmpty

    if (supportEventTimeIndex) {
      validateColumnTypeOnEventTimeColumn()
    }

    private def validateColumnTypeOnEventTimeColumn(): Unit = {
      require(eventTimeColIdx.nonEmpty)

      var curSchema: StructType = keySchema
      eventTimeColIdx.dropRight(1).foreach { idx =>
        curSchema(idx).dataType match {
          case stType: StructType =>
            curSchema = stType
          case _ =>
            // FIXME: better error message
            throw new IllegalStateException("event time column is not properly specified! " +
              s"index: ${eventTimeColIdx.mkString("(", ", ", ")")} / key schema: $keySchema")
        }
      }

      curSchema(eventTimeColIdx.last).dataType match {
        case _: TimestampType =>
        case _ =>
          // FIXME: better error message
          throw new IllegalStateException("event time column is not properly specified! " +
            s"index: ${eventTimeColIdx.mkString("(", ", ", ")")} / key schema: $keySchema")
      }
    }

    private var lowestEventTime: Long = Option(rocksDB.getCustomMetadata())
      .flatMap(_.get(METADATA_KEY_LOWEST_EVENT_TIME).map(_.toLong))
      .getOrElse(INVALID_LOWEST_EVENT_TIME_VALUE)

    override def id: StateStoreId = RocksDBStateStoreProvider.this.stateStoreId

    override def version: Long = lastVersion

    override def get(key: UnsafeRow): UnsafeRow = {
      verify(key != null, "Key cannot be null")
      val value = encoder.decodeValue(rocksDB.get(encoder.encodeKey(key)))
      if (!isValidated && value != null) {
        StateStoreProvider.validateStateRowFormat(
          key, keySchema, value, valueSchema, storeConf)
        isValidated = true
      }
      value
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot put a null value")

      val encodedKey = encoder.encodeKey(key)
      val encodedValue = encoder.encodeValue(value)
      rocksDB.put(encodedKey, encodedValue)

      if (supportEventTimeIndex) {
        val eventTimeValue = extractEventTime(key)
        if (lowestEventTime != INVALID_LOWEST_EVENT_TIME_VALUE
          && lowestEventTime > eventTimeValue) {
          lowestEventTime = eventTimeValue
        }
      }
    }

    override def remove(key: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      verify(key != null, "Key cannot be null")

      rocksDB.remove(encoder.encodeKey(key))
      if (supportEventTimeIndex) {
        val eventTimeValue = extractEventTime(key)
        if (lowestEventTime == eventTimeValue) {
          // We can't track the next lowest event time without scanning entire keys.
          // Mark the lowest event time value as invalid, so that scan happens in evict phase and
          // the value is correctly updated later.
          lowestEventTime = INVALID_LOWEST_EVENT_TIME_VALUE
        }
      }
    }

    override def iterator(): Iterator[UnsafeRowPair] = {
      rocksDB.iterator().map { kv =>
        val rowPair = encoder.decode(kv)
        if (!isValidated && rowPair.value != null) {
          StateStoreProvider.validateStateRowFormat(
            rowPair.key, keySchema, rowPair.value, valueSchema, storeConf)
          isValidated = true
        }
        rowPair
      }
    }

    override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = {
      require(encoder.supportPrefixKeyScan, "Prefix scan requires setting prefix key!")

      val prefix = encoder.encodePrefixKey(prefixKey)
      rocksDB.prefixScan(prefix).map(kv => encoder.decode(kv))
    }

    /** FIXME: method doc */
    override def evictOnWatermark(
      watermarkMs: Long,
      altPred: UnsafeRowPair => Boolean): Iterator[UnsafeRowPair] = {
      if (supportEventTimeIndex) {
        // convert lowestEventTime to milliseconds, and compare to watermarkMs
        // retract 1 ms to avoid edge-case on conversion from microseconds to milliseconds
        if (lowestEventTime != INVALID_LOWEST_EVENT_TIME_VALUE
          && ((lowestEventTime / 1000) - 1 > watermarkMs)) {
          Iterator.empty
        } else {
          // start with invalidating the lowest event time
          lowestEventTime = INVALID_LOWEST_EVENT_TIME_VALUE

          new NextIterator[UnsafeRowPair] {
            private val iter = rocksDB.iterator()

            // here we use Long.MaxValue as invalid value
            private var lowestEventTimeInIter = Long.MaxValue

            override protected def getNext(): UnsafeRowPair = {
              var result: UnsafeRowPair = null
              while (result == null && iter.hasNext) {
                val kv = iter.next()
                val rowPair = encoder.decode(kv)
                if (altPred(rowPair)) {
                  rocksDB.remove(kv.key)
                  result = rowPair
                } else {
                  val eventTime = extractEventTime(rowPair.key)
                  if (lowestEventTimeInIter > eventTime) {
                    lowestEventTimeInIter = eventTime
                  }
                }
              }

              if (result == null) {
                finished = true
                null
              } else {
                result
              }
            }

            override protected def close(): Unit = {
              if (lowestEventTimeInIter != Long.MaxValue) {
                lowestEventTime = lowestEventTimeInIter
              }
            }
          }
        }
      } else {
        rocksDB.iterator().flatMap { kv =>
          val rowPair = encoder.decode(kv)
          if (altPred(rowPair)) {
            rocksDB.remove(kv.key)
            Some(rowPair)
          } else {
            None
          }
        }
      }
    }

    private def extractEventTime(key: UnsafeRow): Long = {
      var curRow: UnsafeRow = key
      var curSchema: StructType = keySchema

      eventTimeColIdx.dropRight(1).foreach { idx =>
        // validation is done in initialization phase
        curSchema = curSchema(idx).dataType.asInstanceOf[StructType]
        curRow = curRow.getStruct(idx, curSchema.length)
      }

      curRow.getLong(eventTimeColIdx.last)
    }

    override def commit(): Long = synchronized {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")

      // set the metadata to RocksDB instance so that it can be committed as well
      rocksDB.setCustomMetadata(Map(METADATA_KEY_LOWEST_EVENT_TIME -> lowestEventTime.toString))

      val newVersion = rocksDB.commit()
      state = COMMITTED
      logInfo(s"Committed $newVersion for $id")
      newVersion
    }

    override def abort(): Unit = {
      verify(state == UPDATING || state == ABORTED, "Cannot abort after already committed")
      logInfo(s"Aborting ${version + 1} for $id")
      rocksDB.rollback()
      state = ABORTED
    }

    override def metrics: StateStoreMetrics = {
      val rocksDBMetrics = rocksDB.metrics
      def commitLatencyMs(typ: String): Long = rocksDBMetrics.lastCommitLatencyMs.getOrElse(typ, 0L)
      def nativeOpsLatencyMillis(typ: String): Long = {
        rocksDBMetrics.nativeOpsMetrics.get(typ).map(_ * 1000).getOrElse(0)
      }
      def sumNativeOpsLatencyMillis(typ: String): Long = {
        rocksDBMetrics.nativeOpsHistograms.get(typ).map(_.sum / 1000).getOrElse(0)
      }
      def nativeOpsCount(typ: String): Long = {
        rocksDBMetrics.nativeOpsHistograms.get(typ).map(_.count).getOrElse(0)
      }
      def nativeOpsMetrics(typ: String): Long = {
        rocksDBMetrics.nativeOpsMetrics.get(typ).getOrElse(0)
      }

      val stateStoreCustomMetrics = Map[StateStoreCustomMetric, Long](
        CUSTOM_METRIC_SST_FILE_SIZE -> rocksDBMetrics.totalSSTFilesBytes,
        CUSTOM_METRIC_GET_TIME -> sumNativeOpsLatencyMillis("get"),
        CUSTOM_METRIC_PUT_TIME -> sumNativeOpsLatencyMillis("put"),
        CUSTOM_METRIC_GET_COUNT -> nativeOpsCount("get"),
        CUSTOM_METRIC_PUT_COUNT -> nativeOpsCount("put"),
        CUSTOM_METRIC_WRITEBATCH_TIME -> commitLatencyMs("writeBatch"),
        CUSTOM_METRIC_FLUSH_TIME -> commitLatencyMs("flush"),
        CUSTOM_METRIC_COMMIT_COMPACT_TIME -> commitLatencyMs("compact"),
        CUSTOM_METRIC_PAUSE_TIME -> commitLatencyMs("pauseBg"),
        CUSTOM_METRIC_CHECKPOINT_TIME -> commitLatencyMs("checkpoint"),
        CUSTOM_METRIC_FILESYNC_TIME -> commitLatencyMs("fileSync"),
        CUSTOM_METRIC_BYTES_COPIED -> rocksDBMetrics.bytesCopied,
        CUSTOM_METRIC_FILES_COPIED -> rocksDBMetrics.filesCopied,
        CUSTOM_METRIC_FILES_REUSED -> rocksDBMetrics.filesReused,
        CUSTOM_METRIC_BLOCK_CACHE_MISS -> nativeOpsMetrics("readBlockCacheMissCount"),
        CUSTOM_METRIC_BLOCK_CACHE_HITS -> nativeOpsMetrics("readBlockCacheHitCount"),
        CUSTOM_METRIC_BYTES_READ -> nativeOpsMetrics("totalBytesRead"),
        CUSTOM_METRIC_BYTES_WRITTEN -> nativeOpsMetrics("totalBytesWritten"),
        CUSTOM_METRIC_ITERATOR_BYTES_READ -> nativeOpsMetrics("totalBytesReadThroughIterator"),
        CUSTOM_METRIC_STALL_TIME -> nativeOpsLatencyMillis("writerStallDuration"),
        CUSTOM_METRIC_TOTAL_COMPACT_TIME -> sumNativeOpsLatencyMillis("compaction"),
        CUSTOM_METRIC_COMPACT_READ_BYTES -> nativeOpsMetrics("totalBytesReadByCompaction"),
        CUSTOM_METRIC_COMPACT_WRITTEN_BYTES -> nativeOpsMetrics("totalBytesWrittenByCompaction")
      ) ++ rocksDBMetrics.zipFileBytesUncompressed.map(bytes =>
        Map(CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED -> bytes)).getOrElse(Map())

      StateStoreMetrics(
        rocksDBMetrics.numUncommittedKeys,
        rocksDBMetrics.memUsageBytes,
        stateStoreCustomMetrics)
    }

    override def hasCommitted: Boolean = state == COMMITTED

    override def toString: String = {
      s"RocksDBStateStore[id=(op=${id.operatorId},part=${id.partitionId})," +
        s"dir=${id.storeCheckpointLocation()}]"
    }

    /** Return the [[RocksDB]] instance in this store. This is exposed mainly for testing. */
    def dbInstance(): RocksDB = rocksDB
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      operatorContext: StatefulOperatorContext,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf

    require((keySchema.length == 0 && operatorContext.numColsPrefixKey == 0) ||
      (keySchema.length > operatorContext.numColsPrefixKey), "The number of columns in the key " +
      "must be greater than the number of columns for prefix key!")

    this.operatorContext = operatorContext

    this.encoder = RocksDBStateEncoder.getEncoder(keySchema, valueSchema,
      operatorContext.numColsPrefixKey)

    rocksDB // lazy initialization
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def getStore(version: Long): StateStore = {
    require(version >= 0, "Version cannot be less than 0")
    rocksDB.load(version)
    new RocksDBStateStore(version, operatorContext.eventTimeColIdx)
  }

  override def doMaintenance(): Unit = {
    rocksDB.cleanup()
  }

  override def close(): Unit = {
    rocksDB.close()
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = ALL_CUSTOM_METRICS

  private[state] def latestVersion: Long = rocksDB.getLatestVersion()

  /** Internal fields and methods */

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var operatorContext: StatefulOperatorContext = _

  private[sql] lazy val rocksDB = {
    val dfsRootDir = stateStoreId.storeCheckpointLocation().toString
    val storeIdStr = s"StateStoreId(opId=${stateStoreId.operatorId}," +
      s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val localRootDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), storeIdStr)
    new RocksDB(dfsRootDir, RocksDBConf(storeConf),
      localRootDir = localRootDir,
      hadoopConf = hadoopConf, loggingId = storeIdStr)
  }

  @volatile private var encoder: RocksDBStateEncoder = _

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) { throw new IllegalStateException(msg) }
  }
}

object RocksDBStateStoreProvider {
  // Version as a single byte that specifies the encoding of the row data in RocksDB
  val STATE_ENCODING_NUM_VERSION_BYTES = 1
  val STATE_ENCODING_VERSION: Byte = 0

  val INVALID_LOWEST_EVENT_TIME_VALUE: Long = Long.MinValue
  val METADATA_KEY_LOWEST_EVENT_TIME: String = "lowestEventTimeInState"

  // Native operation latencies report as latency in microseconds
  // as SQLMetrics support millis. Convert the value to millis
  val CUSTOM_METRIC_GET_TIME = StateStoreCustomTimingMetric(
    "rocksdbGetLatency", "RocksDB: total get call latency")
  val CUSTOM_METRIC_PUT_TIME = StateStoreCustomTimingMetric(
    "rocksdbPutLatency", "RocksDB: total put call latency")

  val CUSTOM_METRIC_GET_COUNT = StateStoreCustomSumMetric(
    "rocksdbGetCount", "RocksDB: number of get calls")
  val CUSTOM_METRIC_PUT_COUNT = StateStoreCustomSumMetric(
    "rocksdbPutCount", "RocksDB: number of put calls")

  // Commit latency detailed breakdown
  val CUSTOM_METRIC_WRITEBATCH_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitWriteBatchLatency", "RocksDB: commit - write batch time")
  val CUSTOM_METRIC_FLUSH_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitFlushLatency", "RocksDB: commit - flush time")
  val CUSTOM_METRIC_COMMIT_COMPACT_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitCompactLatency", "RocksDB: commit - compact time")
  val CUSTOM_METRIC_PAUSE_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitPauseLatency", "RocksDB: commit - pause bg time")
  val CUSTOM_METRIC_CHECKPOINT_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitCheckpointLatency", "RocksDB: commit - checkpoint time")
  val CUSTOM_METRIC_FILESYNC_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitFileSyncLatencyMs", "RocksDB: commit - file sync to external storage time")
  val CUSTOM_METRIC_FILES_COPIED = StateStoreCustomSumMetric(
    "rocksdbFilesCopied", "RocksDB: file manager - files copied")
  val CUSTOM_METRIC_BYTES_COPIED = StateStoreCustomSizeMetric(
    "rocksdbBytesCopied", "RocksDB: file manager - bytes copied")
  val CUSTOM_METRIC_FILES_REUSED = StateStoreCustomSumMetric(
    "rocksdbFilesReused", "RocksDB: file manager - files reused")
  val CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED = StateStoreCustomSizeMetric(
    "rocksdbZipFileBytesUncompressed", "RocksDB: file manager - uncompressed zip file bytes")

  val CUSTOM_METRIC_BLOCK_CACHE_MISS = StateStoreCustomSumMetric(
    "rocksdbReadBlockCacheMissCount",
    "RocksDB: read - count of cache misses that required reading from local disk")
  val CUSTOM_METRIC_BLOCK_CACHE_HITS = StateStoreCustomSumMetric(
    "rocksdbReadBlockCacheHitCount",
    "RocksDB: read - count of cache hits in RocksDB block cache avoiding disk read")
  val CUSTOM_METRIC_BYTES_READ = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesRead",
    "RocksDB: read - total of uncompressed bytes read (from memtables/cache/sst) from DB::Get()")
  val CUSTOM_METRIC_BYTES_WRITTEN = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesWritten",
    "RocksDB: write - total of uncompressed bytes written by " +
      "DB::{Put(), Delete(), Merge(), Write()}")
  val CUSTOM_METRIC_ITERATOR_BYTES_READ = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesReadThroughIterator",
    "RocksDB: read - total of uncompressed bytes read using an iterator")
  val CUSTOM_METRIC_STALL_TIME = StateStoreCustomTimingMetric(
    "rocksdbWriterStallLatencyMs",
    "RocksDB: write - writer wait time for compaction or flush to finish")
  val CUSTOM_METRIC_TOTAL_COMPACT_TIME = StateStoreCustomTimingMetric(
    "rocksdbTotalCompactionLatencyMs",
    "RocksDB: compaction - total compaction time including background")
  val CUSTOM_METRIC_COMPACT_READ_BYTES = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesReadByCompaction",
    "RocksDB: compaction - total bytes read by the compaction process")
  val CUSTOM_METRIC_COMPACT_WRITTEN_BYTES = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesWrittenByCompaction",
    "RocksDB: compaction - total bytes written by the compaction process")

  // Total SST file size
  val CUSTOM_METRIC_SST_FILE_SIZE = StateStoreCustomSizeMetric(
    "rocksdbSstFileSize", "RocksDB: size of all SST files")

  val ALL_CUSTOM_METRICS = Seq(
    CUSTOM_METRIC_SST_FILE_SIZE, CUSTOM_METRIC_GET_TIME, CUSTOM_METRIC_PUT_TIME,
    CUSTOM_METRIC_WRITEBATCH_TIME, CUSTOM_METRIC_FLUSH_TIME, CUSTOM_METRIC_COMMIT_COMPACT_TIME,
    CUSTOM_METRIC_PAUSE_TIME, CUSTOM_METRIC_CHECKPOINT_TIME, CUSTOM_METRIC_FILESYNC_TIME,
    CUSTOM_METRIC_BYTES_COPIED, CUSTOM_METRIC_FILES_COPIED, CUSTOM_METRIC_FILES_REUSED,
    CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED, CUSTOM_METRIC_GET_COUNT, CUSTOM_METRIC_PUT_COUNT,
    CUSTOM_METRIC_BLOCK_CACHE_MISS, CUSTOM_METRIC_BLOCK_CACHE_HITS, CUSTOM_METRIC_BYTES_READ,
    CUSTOM_METRIC_BYTES_WRITTEN, CUSTOM_METRIC_ITERATOR_BYTES_READ, CUSTOM_METRIC_STALL_TIME,
    CUSTOM_METRIC_TOTAL_COMPACT_TIME, CUSTOM_METRIC_COMPACT_READ_BYTES,
    CUSTOM_METRIC_COMPACT_WRITTEN_BYTES
  )
}

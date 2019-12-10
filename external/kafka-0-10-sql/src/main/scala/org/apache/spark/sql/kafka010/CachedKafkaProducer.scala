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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaRedactionUtil}

private[kafka010] class CachedKafkaProducer(
    val cacheKey: Seq[(String, Object)],
    val producer: KafkaProducer[Array[Byte], Array[Byte]]) extends Logging {
  val id: String = ju.UUID.randomUUID().toString

  private def close(): Unit = {
    try {
      logInfo(s"Closing the KafkaProducer with id: $id.")
      producer.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing kafka producer.", e)
    }
  }
}

private[kafka010] object CachedKafkaProducer extends Logging {
  private type CacheKey = Seq[(String, Object)]
  private type Producer = KafkaProducer[Array[Byte], Array[Byte]]

  /**
   * This class is used as metadata of cache, and shouldn't be exposed to the public (it would be
   * fine for testing). This class assumes thread-safety is guaranteed by the caller.
   */
  class CachedProducerEntry(val producer: CachedKafkaProducer) {
    private var refCount: Long = 0L
    private var expireAt: Long = Long.MaxValue

    def handleBorrowed(): Unit = {
      refCount += 1
      expireAt = Long.MaxValue
    }

    def handleReturned(): Unit = {
      refCount -= 1
      if (refCount <= 0) {
        expireAt = System.currentTimeMillis() + cacheExpireTimeout
      }
    }

    def expired: Boolean = refCount <= 0 && expireAt < System.currentTimeMillis()

    /** expose for testing, don't call otherwise */
    private[kafka010] def injectDebugValues(refCnt: Long, expire: Long): Unit = {
      refCount = refCnt
      expireAt = expire
    }
  }

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long = Option(SparkEnv.get)
    .map(_.conf.get(PRODUCER_CACHE_TIMEOUT))
    .getOrElse(defaultCacheExpireTimeout)

  private var acquireCount: Long = 0

  private val cache = new mutable.HashMap[CacheKey, CachedProducerEntry]

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  private[kafka010] def acquire(kafkaParams: ju.Map[String, Object]): CachedKafkaProducer = {
    acquireCount += 1
    if (acquireCount % 100 == 0) {
      expire()
    }

    val updatedKafkaProducerConfiguration =
      KafkaConfigUpdater("executor", kafkaParams.asScala.toMap)
        .setAuthenticationConfigIfNeeded()
        .build()
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(updatedKafkaProducerConfiguration)
    synchronized {
      val entry = cache.getOrElseUpdate(paramsSeq, {
        val producer = createKafkaProducer(paramsSeq)
        val cachedProducer = new CachedKafkaProducer(paramsSeq, producer)
        new CachedProducerEntry(cachedProducer)
      })
      entry.handleBorrowed()
      entry.producer
    }
  }

  private[kafka010] def release(producer: CachedKafkaProducer): Unit = {
    def closeProducerNotInCache(producer: CachedKafkaProducer): Unit = {
      logWarning(s"Released producer ${producer.id} is not a member of the cache. Closing.")
      producer.close()
    }

    synchronized {
      cache.get(producer.cacheKey) match {
        case Some(entry) if entry.producer.id == producer.id => entry.handleReturned()
        case _ => closeProducerNotInCache(producer)
      }
    }
  }

  private def removeFromCache(key: CacheKey): Unit = {
    cache.remove(key).foreach { instance => instance.producer.close() }
  }

  /** expose for testing */
  private[kafka010] def expire(): Unit = synchronized {
    cache.filter { case (_, v) => v.expired }.keys.foreach(removeFromCache)
  }

  private def createKafkaProducer(paramsSeq: Seq[(String, Object)]): Producer = {
    val kafkaProducer: Producer = new Producer(paramsSeq.toMap.asJava)
    if (log.isDebugEnabled()) {
      val redactedParamsSeq = KafkaRedactionUtil.redactParams(paramsSeq)
      logDebug(s"Created a new instance of KafkaProducer for $redactedParamsSeq.")
    }
    kafkaProducer
  }

  private def paramsToSeq(kafkaParams: ju.Map[String, Object]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = kafkaParams.asScala.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  private[kafka010] def clear(): Unit = {
    logInfo("Cleaning up cache.")
    synchronized {
      cache.keys.foreach(removeFromCache)
    }
  }

  // Intended for testing purpose only.
  private def getAsMap: Map[CacheKey, CachedProducerEntry] = cache.toMap
}

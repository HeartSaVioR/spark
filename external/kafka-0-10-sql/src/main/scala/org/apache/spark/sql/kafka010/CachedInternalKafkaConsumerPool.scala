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
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.pool2.{BaseKeyedPooledObjectFactory, PooledObject, SwallowedExceptionListener}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericKeyedObjectPool, GenericKeyedObjectPoolConfig}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.CachedInternalKafkaConsumerPool._
import org.apache.spark.sql.kafka010.KafkaDataConsumer._

private[kafka010] class CachedInternalKafkaConsumerPool(objectFactory: ObjectFactory,
                                                        poolConfig: PoolConfig) {

  private lazy val pool = {
    val internalPool = new GenericKeyedObjectPool[CacheKey, InternalKafkaConsumer](
      objectFactory, poolConfig)
    internalPool.setSwallowedExceptionListener(CustomSwallowedExceptionListener)
    internalPool
  }

  def borrowObject(key: CacheKey, kafkaParams: ju.Map[String, Object]): InternalKafkaConsumer = {
    updateKafkaParamForKey(key, kafkaParams)
    pool.borrowObject(key)
  }

  def returnObject(intConsumer: InternalKafkaConsumer): Unit = {
    val key = new CacheKey(intConsumer.topicPartition, intConsumer.kafkaParams)
    pool.returnObject(key, intConsumer)
  }

  def invalidateKey(key: CacheKey): Unit = {
    // invalidate all idle consumers for the key
    pool.clear(key)

    pool.getNumActive()
    // set invalidate timestamp to let active objects being destroyed when returning to pool
    objectFactory.keyToLastInvalidatedTimestamp.put(key, System.currentTimeMillis())
  }

  def close(): Unit = {
    pool.close()
  }

  def getNumIdle: Int = pool.getNumIdle

  def getNumIdle(key: CacheKey): Int = pool.getNumIdle(key)

  def getNumActive: Int = pool.getNumActive

  def getNumActive(key: CacheKey): Int = pool.getNumActive(key)

  def getTotal: Int = getNumIdle + getNumActive

  def getTotal(key: CacheKey): Int = getNumIdle(key) + getNumActive(key)

  private def updateKafkaParamForKey(key: CacheKey, kafkaParams: ju.Map[String, Object]): Unit = {
    // We can assume that kafkaParam should not be different for same cache key,
    // otherwise we can't reuse the cached object and cache key should contain kafkaParam.
    // So it should be safe to put the key/value pair only when the key doesn't exist.
    objectFactory.keyToKafkaParams.putIfAbsent(key, kafkaParams)
  }
}

private[kafka010] object CachedInternalKafkaConsumerPool {

  def build: CachedInternalKafkaConsumerPool = {
    val objFactory = new ObjectFactory
    val poolConfig = new PoolConfig
    new CachedInternalKafkaConsumerPool(objFactory, poolConfig)
  }

  case class PooledObjectInvalidated(key: CacheKey, lastInvalidatedTimestamp: Long,
                                     lastBorrowedTime: Long) extends RuntimeException

  object CustomSwallowedExceptionListener extends SwallowedExceptionListener with Logging {
    override def onSwallowException(e: Exception): Unit = e match {
      case e1: PooledObjectInvalidated =>
        logDebug("Pool for key was invalidated after cached object was borrowed. " +
          s"Invalidating cached object - key: ${e1.key} / borrowed timestamp: " +
          s"${e1.lastBorrowedTime} / invalidated timestamp for key: ${e1.lastInvalidatedTimestamp}")

      case _ => logError(s"Error closing Kafka consumer", e)
    }
  }

  class PoolConfig extends GenericKeyedObjectPoolConfig[InternalKafkaConsumer] {

    final val minEvictableIdleTimeMillis = 5 * 60 * 1000 // 5 minutes

    init()

    def init(): Unit = {
      val conf = SparkEnv.get.conf
      val capacity = conf.getInt("spark.sql.kafkaConsumerCache.capacity", 64)
      val jmxEnabled = conf.getBoolean("spark.sql.kafkaConsumerCache.jmx.enable",
        defaultValue = false)

      // 1. Set min idle objects per key to 0 to avoid creating unnecessary object.
      // 2. Set max idle objects per key to 1 but set total objects per key to infinite
      // which ensures borrowing per key is not restricted (though it can be restricted in
      // total objects).
      // 3. Set max total objects to the capacity users set to the configuration.
      // This will prevent having active objects more than capacity. Fail-back approach should be
      // provided when the pool exceeds the capacity.
      setMinIdlePerKey(0)
      setMaxIdlePerKey(1)
      setMaxTotalPerKey(-1)
      setMaxTotal(capacity)

      // Set minimum evictable idle time which will be referred from evictor thread
      setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
      setSoftMinEvictableIdleTimeMillis(-1)

      // Immediately fail on exhausted pool while borrowing
      setBlockWhenExhausted(false)

      setJmxEnabled(jmxEnabled)
      setJmxNamePrefix("kafka010-cached-kafka-consumer-pool")
    }
  }

  class ObjectFactory extends BaseKeyedPooledObjectFactory[CacheKey, InternalKafkaConsumer]
    with Logging {

    lazy val keyToKafkaParams: ConcurrentHashMap[CacheKey, ju.Map[String, Object]] =
      new ConcurrentHashMap[CacheKey, ju.Map[String, Object]]()

    lazy val keyToLastInvalidatedTimestamp: ConcurrentHashMap[CacheKey, Long] =
      new ConcurrentHashMap[CacheKey, Long]()

    override def create(key: CacheKey): InternalKafkaConsumer = {
      val kafkaParams = keyToKafkaParams.get(key)
      if (kafkaParams == null) {
        throw new IllegalStateException("Kafka params should be set before borrowing object.")
      }
      new InternalKafkaConsumer(key.topicPartition, kafkaParams)
    }

    override def wrap(value: InternalKafkaConsumer): PooledObject[InternalKafkaConsumer] = {
      new DefaultPooledObject[InternalKafkaConsumer](value)
    }

    override def passivateObject(key: CacheKey, p: PooledObject[InternalKafkaConsumer]): Unit = {
      Option(keyToLastInvalidatedTimestamp.get(key)) match {
        // If the object is borrowed before than key pool being invalidated,
        // throw exception to invalidate pooled object while returning to pool.
        case Some(lastInvalidatedTimestamp) if lastInvalidatedTimestamp > p.getLastBorrowTime =>
          throw PooledObjectInvalidated(key, lastInvalidatedTimestamp,
            p.getLastBorrowTime)
        case _ =>
      }
    }

    override def destroyObject(key: CacheKey, p: PooledObject[InternalKafkaConsumer]): Unit = {
      p.getObject.close()
    }
  }
}

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
import org.apache.commons.pool2.impl.{DefaultEvictionPolicy, DefaultPooledObject, GenericKeyedObjectPool, GenericKeyedObjectPoolConfig}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.InternalKafkaConsumerPool._
import org.apache.spark.sql.kafka010.KafkaDataConsumer.CacheKey
import org.apache.spark.util.Utils

/**
 * Provides object pool for [[InternalKafkaConsumer]] which is grouped by [[CacheKey]].
 *
 * This class leverages [[GenericKeyedObjectPool]] internally, hence providing methods based on
 * the class, and same contract applies: after using the borrowed object, you must either call
 * returnObject() if the object is healthy to return to pool, or invalidateObject() if the object
 * should be destroyed.
 *
 * The soft capacity of pool is determined by "spark.sql.kafkaConsumerCache.capacity" config value,
 * and the pool will have reasonable default value if the value is not provided.
 * (The instance will do its best effort to respect soft capacity but it can exceed when there's
 * a borrowing request and there's neither free space nor idle object to clear.)
 *
 * This class guarantees that no caller will get pooled object once the object is borrowed and
 * not yet returned, hence provide thread-safety usage of non-thread-safe [[InternalKafkaConsumer]]
 * unless caller shares the object to multiple threads.
 */
private[kafka010] class InternalKafkaConsumerPool(
    objectFactory: ObjectFactory,
    poolConfig: PoolConfig) extends Logging {

  // the class is intended to have only soft capacity
  assert(poolConfig.getMaxTotal < 0)

  private lazy val pool = {
    val internalPool = new GenericKeyedObjectPool[CacheKey, InternalKafkaConsumer](
      objectFactory, poolConfig)
    internalPool.setSwallowedExceptionListener(CustomSwallowedExceptionListener)
    internalPool
  }

  /**
   * Borrows [[InternalKafkaConsumer]] object from the pool. If there's no idle object for the key,
   * the pool will create the [[InternalKafkaConsumer]] object.
   *
   * If the pool doesn't have idle object for the key and also exceeds the soft capacity,
   * pool will try to clear some of idle objects.
   *
   * Borrowed object must be returned by either calling returnObject or invalidateObject, otherwise
   * the object will be kept in pool as active object.
   */
  def borrowObject(key: CacheKey, kafkaParams: ju.Map[String, Object]): InternalKafkaConsumer = {
    val (ret, elapsed) = Utils.timeTakenMs {
      updateKafkaParamForKey(key, kafkaParams)

      val softMaxCapacity = poolConfig.getSoftMaxTotal()
      if (getTotal == softMaxCapacity) {
        logWarning(s"KafkaConsumer cache hitting max capacity of $softMaxCapacity, " +
          s"removing oldest consumer.")
        pool.clearOldest()
      }

      logWarning(s"DEBUG: borrowing object for key $key ...")
      pool.borrowObject(key)
    }

    logWarning(s"DEBUG: borrowObject elapsed: $elapsed ms")

    ret
  }

  /** Returns borrowed object to the pool. */
  def returnObject(consumer: InternalKafkaConsumer): Unit = {
    val (_, elapsed) = Utils.timeTakenMs {
      pool.returnObject(extractCacheKey(consumer), consumer)
    }

    logWarning(s"DEBUG: returnObject elapsed: $elapsed ms")
  }

  /** Invalidates (destroy) borrowed object to the pool. */
  def invalidateObject(consumer: InternalKafkaConsumer): Unit = {
    val (_, elapsed) = Utils.timeTakenMs {
      pool.invalidateObject(extractCacheKey(consumer), consumer)
    }

    logWarning(s"DEBUG: invalidateObject elapsed: $elapsed ms")
  }

  /** Invalidates all idle consumers for the key */
  def invalidateKey(key: CacheKey): Unit = {
    val (_, elapsed) = Utils.timeTakenMs {
      pool.clear(key)
    }

    logWarning(s"DEBUG: invalidateKey elapsed: $elapsed ms")
  }

  /**
   * Closes the keyed object pool. Once the pool is closed,
   * borrowObject will fail with [[IllegalStateException]], but returnObject and invalidateObject
   * will continue to work, with returned objects destroyed on return.
   *
   * Also destroys idle instances in the pool.
   */
  def close(): Unit = {
    pool.close()
  }

  def reset(): Unit = {
    // this is the best-effort of clearing up. otherwise we should close the pool and create again
    // but we don't want to make it "var" only because of tests.
    pool.clear()
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
    val oldKafkaParams = objectFactory.keyToKafkaParams.putIfAbsent(key, kafkaParams)
    require(oldKafkaParams == null || kafkaParams == oldKafkaParams, "Kafka parameters for same " +
      s"cache key should be equal. old parameters: $oldKafkaParams new parameters: $kafkaParams")
  }

  private def extractCacheKey(consumer: InternalKafkaConsumer): CacheKey = {
    new CacheKey(consumer.topicPartition, consumer.kafkaParams)
  }
}

private[kafka010] object InternalKafkaConsumerPool {

  /**
   * Builds the pool for [[InternalKafkaConsumer]]. The pool instance is created per each call.
   */
  def build: InternalKafkaConsumerPool = {
    val objFactory = new ObjectFactory
    val poolConfig = new PoolConfig
    new InternalKafkaConsumerPool(objFactory, poolConfig)
  }

  object CustomSwallowedExceptionListener extends SwallowedExceptionListener with Logging {
    override def onSwallowException(e: Exception): Unit = {
      logError(s"Error closing Kafka consumer", e)
    }
  }

  class PoolConfig extends GenericKeyedObjectPoolConfig[InternalKafkaConsumer] {
    private var softMaxTotal = Int.MaxValue

    def getSoftMaxTotal(): Int = softMaxTotal

    init()

    def init(): Unit = {
      import PoolConfig._

      val conf = SparkEnv.get.conf

      softMaxTotal = conf.getInt(CONFIG_NAME_CAPACITY, DEFAULT_VALUE_CAPACITY)

      val jmxEnabled = conf.getBoolean(CONFIG_NAME_JMX_ENABLED,
        defaultValue = DEFAULT_VALUE_JMX_ENABLED)
      val minEvictableIdleTimeMillis = conf.getLong(CONFIG_NAME_MIN_EVICTABLE_IDLE_TIME_MILLIS,
        DEFAULT_VALUE_MIN_EVICTABLE_IDLE_TIME_MILLIS)
      val evictorThreadRunIntervalMillis = conf.getLong(
        CONFIG_NAME_EVICTOR_THREAD_RUN_INTERVAL_MILLIS,
        DEFAULT_VALUE_EVICTOR_THREAD_RUN_INTERVAL_MILLIS)

      // NOTE: Below lines define the behavior, so do not modify unless you know what you are
      // doing, and update the class doc accordingly if necessary when you modify.

      // 1. Set min idle objects per key to 0 to avoid creating unnecessary object.
      // 2. Set max idle objects per key to 3 but set total objects per key to infinite
      // which ensures borrowing per key is not restricted.
      // 3. Set max total objects to infinite which ensures all objects are managed in this pool.
      setMinIdlePerKey(0)
      setMaxIdlePerKey(3)
      setMaxTotalPerKey(-1)
      setMaxTotal(-1)

      // Set minimum evictable idle time which will be referred from evictor thread
      setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
      setSoftMinEvictableIdleTimeMillis(-1)

      // evictor thread will run test with ten idle objects
      setTimeBetweenEvictionRunsMillis(evictorThreadRunIntervalMillis)
      setNumTestsPerEvictionRun(10)
      setEvictionPolicy(new DefaultEvictionPolicy[InternalKafkaConsumer]())

      // Immediately fail on exhausted pool while borrowing
      setBlockWhenExhausted(false)

      setJmxEnabled(jmxEnabled)
      setJmxNamePrefix("kafka010-cached-simple-kafka-consumer-pool")
    }
  }

  object PoolConfig {
    val CONFIG_NAME_PREFIX = "spark.sql.kafkaConsumerCache."
    val CONFIG_NAME_CAPACITY = CONFIG_NAME_PREFIX + "capacity"
    val CONFIG_NAME_JMX_ENABLED = CONFIG_NAME_PREFIX + "jmx.enable"
    val CONFIG_NAME_MIN_EVICTABLE_IDLE_TIME_MILLIS = CONFIG_NAME_PREFIX +
      "minEvictableIdleTimeMillis"
    val CONFIG_NAME_EVICTOR_THREAD_RUN_INTERVAL_MILLIS = CONFIG_NAME_PREFIX +
      "evictorThreadRunIntervalMillis"

    val DEFAULT_VALUE_CAPACITY = 64
    val DEFAULT_VALUE_JMX_ENABLED = false
    val DEFAULT_VALUE_MIN_EVICTABLE_IDLE_TIME_MILLIS = 5 * 60 * 1000 // 5 minutes
    val DEFAULT_VALUE_EVICTOR_THREAD_RUN_INTERVAL_MILLIS = 3 * 60 * 1000 // 3 minutes
  }

  class ObjectFactory extends BaseKeyedPooledObjectFactory[CacheKey, InternalKafkaConsumer]
    with Logging {

    val keyToKafkaParams: ConcurrentHashMap[CacheKey, ju.Map[String, Object]] =
      new ConcurrentHashMap[CacheKey, ju.Map[String, Object]]()

    override def create(key: CacheKey): InternalKafkaConsumer = {
      Option(keyToKafkaParams.get(key)) match {
        case Some(kafkaParams) => new InternalKafkaConsumer(key.topicPartition, kafkaParams)
        case None => throw new IllegalStateException("Kafka params should be set before " +
          "borrowing object.")
      }
    }

    override def wrap(value: InternalKafkaConsumer): PooledObject[InternalKafkaConsumer] = {
      new DefaultPooledObject[InternalKafkaConsumer](value)
    }

    override def destroyObject(key: CacheKey, p: PooledObject[InternalKafkaConsumer]): Unit = {
      p.getObject.close()
    }
  }
}


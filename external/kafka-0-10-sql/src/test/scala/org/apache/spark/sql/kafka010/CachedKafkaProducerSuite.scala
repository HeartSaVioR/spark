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
import java.util.concurrent.{Executors, TimeUnit}

import scala.util.Random

import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.sql.kafka010.CachedKafkaProducer.CachedProducerEntry
import org.apache.spark.sql.test.SharedSparkSession

class CachedKafkaProducerSuite extends SharedSparkSession with PrivateMethodTester with KafkaTest {
  private val cacheMap = PrivateMethod[Map[Seq[(String, Object)], CachedProducerEntry]](
    Symbol("getAsMap"))
  private val refCount = PrivateMethod[Long](Symbol("refCount"))
  private val expireAt = PrivateMethod[Long](Symbol("expireAt"))
  private val cacheExpireTimeout = PrivateMethod[Long](Symbol("cacheExpireTimeout"))

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedKafkaProducer.clear()
  }

  test("Should return same cached instance on calling acquire with same params.") {
    val kafkaParams = getTestKafkaParams()
    val producer = CachedKafkaProducer.acquire(kafkaParams)
    val producer2 = CachedKafkaProducer.acquire(kafkaParams)
    assert(producer === producer2)

    val map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.size === 1)
    val cacheEntry = map.head._2
    assertCacheEntry(cacheEntry, 2L)

    CachedKafkaProducer.release(producer)
    assertCacheEntry(cacheEntry, 1L)

    CachedKafkaProducer.release(producer2)
    assertCacheEntry(cacheEntry, 0L)

    val producer3 = CachedKafkaProducer.acquire(kafkaParams)
    assertCacheEntry(cacheEntry, 1L)
    assert(producer === producer3)
  }

  test("Should return different cached instances on calling acquire with different params.") {
    val kafkaParams = getTestKafkaParams()
    val producer = CachedKafkaProducer.acquire(kafkaParams)
    kafkaParams.put("acks", "1")
    val producer2 = CachedKafkaProducer.acquire(kafkaParams)
    // With updated conf, a new producer instance should be created.
    assert(producer !== producer2)

    val map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.size === 2)
    val cacheEntry = map.find(_._2.producer.id == producer.id).get._2
    assertCacheEntry(cacheEntry, 1L)
    val cacheEntry2 = map.find(_._2.producer.id == producer2.id).get._2
    assertCacheEntry(cacheEntry2, 1L)
  }

  test("expire instances") {
    val kafkaParams = getTestKafkaParams()

    var map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.isEmpty)

    CachedKafkaProducer.acquire(kafkaParams)
    map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.size === 1)

    CachedKafkaProducer.expire()
    map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.size === 1)

    val cacheEntry = map.head._2

    // Here we do some hack, modifying the cache entry directly, as we cannot change the value of
    // `cacheExpireTimeout` in CachedKafkaProducer.
    // We test the functionality of 'refCount' and 'expireAt' in other suites, so modifying these
    // values arbitrary doesn't hurt.
    val currentTimestamp = System.currentTimeMillis()
    cacheEntry.injectDebugValues(0, currentTimestamp - 1000)

    // Calling expire will clean up instance from cache.
    CachedKafkaProducer.expire()

    map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.size === 0)
  }

  test("reference counting with concurrent access") {
    val kafkaParams = getTestKafkaParams()

    val numThreads = 100
    val numProducerUsages = 500

    def produce(i: Int): Unit = {
      val producer = CachedKafkaProducer.acquire(kafkaParams)
      try {
        val map = CachedKafkaProducer.invokePrivate(cacheMap())
        assert(map.size === 1)
        val cacheEntry = map.head._2
        assert(cacheEntry.invokePrivate(refCount()) > 0L)
        assert(cacheEntry.invokePrivate(expireAt()) === Long.MaxValue)

        Thread.sleep(Random.nextInt(100))
      } finally {
        CachedKafkaProducer.release(producer)
      }
    }

    val threadpool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numProducerUsages).map { i =>
        threadpool.submit(new Runnable {
          override def run(): Unit = { produce(i) }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
    } finally {
      threadpool.shutdown()
    }

    val map = CachedKafkaProducer.invokePrivate(cacheMap())
    assert(map.size === 1)

    val cacheEntry = map.head._2
    assertCacheEntry(cacheEntry, 0L)
  }

  private def getTestKafkaParams(): ju.HashMap[String, Object] = {
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("acks", "0")
    // Here only host should be resolvable, it does not need a running instance of kafka server.
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams
  }

  private def assertCacheEntry(cacheEntry: CachedProducerEntry, expectedRefCount: Long): Unit = {
    val timeoutVal = CachedKafkaProducer.invokePrivate(cacheExpireTimeout())
    assert(cacheEntry.invokePrivate(refCount()) === expectedRefCount)
    if (expectedRefCount > 0) {
      assert(cacheEntry.invokePrivate(expireAt()) === Long.MaxValue)
    } else {
      assert(cacheEntry.invokePrivate(expireAt()) <= System.currentTimeMillis() + timeoutVal)
    }
  }
}

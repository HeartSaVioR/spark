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

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.SparkEnv
import org.apache.spark.sql.kafka010.KafkaDataConsumer.CacheKey
import org.apache.spark.sql.test.SharedSQLContext

class InternalKafkaConsumerPoolSuite extends SharedSQLContext {

  test("basic multiple borrows and returns for single key") {
    val pool = InternalKafkaConsumerPool.build

    val topic = "topic"
    val partitionId = 0
    val topicPartition = new TopicPartition(topic, partitionId)

    val kafkaParams: ju.Map[String, Object] = getTestKafkaParams

    val key = new CacheKey(topicPartition, kafkaParams)
    val pooledObject = pool.borrowObject(key, kafkaParams)

    assertPooledObject(pooledObject, topicPartition, kafkaParams)
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 1, numTotal = 1)
    assertPoolState(pool, numIdle = 0, numActive = 1, numTotal = 1)

    // it doesn't still exceed total pool size
    val pooledObject2 = pool.borrowObject(key, kafkaParams)

    assertPooledObject(pooledObject2, topicPartition, kafkaParams)
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 2, numTotal = 2)
    assertPoolState(pool, numIdle = 0, numActive = 2, numTotal = 2)

    // we only allow one idle object per key
    pool.returnObject(pooledObject)

    assertPoolStateForKey(pool, key, numIdle = 1, numActive = 1, numTotal = 2)
    assertPoolState(pool, numIdle = 1, numActive = 1, numTotal = 2)

    pool.returnObject(pooledObject2)

    assertPoolStateForKey(pool, key, numIdle = 1, numActive = 0, numTotal = 1)
    assertPoolState(pool, numIdle = 1, numActive = 0, numTotal = 1)
  }

  test("basic borrow and return for multiple keys") {
    val pool = InternalKafkaConsumerPool.build

    val kafkaParams = getTestKafkaParams
    val topicPartitions: List[TopicPartition] = for (
      topic <- List("topic", "topic2");
      partitionId <- 0 to 5
    ) yield new TopicPartition(topic, partitionId)

    val keys: List[CacheKey] = topicPartitions.map { part =>
      new CacheKey(part, kafkaParams)
    }

    // while in loop pool doesn't still exceed total pool size
    val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

    assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length,
      numTotal = keyToPooledObjectPairs.length)

    returnObjects(pool, keyToPooledObjectPairs)

    assertPoolState(pool, numIdle = keyToPooledObjectPairs.length, numActive = 0,
      numTotal = keyToPooledObjectPairs.length)
  }

  test("borrow more than capacity from pool, and free up idle objects automatically") {
    val conf = SparkEnv.get.conf
    val capacity = 16
    val prevKafkaConsumerCacheCapacity = conf.get("spark.sql.kafkaConsumerCache.capacity", "64")
    try {
      conf.set("spark.sql.kafkaConsumerCache.capacity", capacity.toString)

      val pool = InternalKafkaConsumerPool.build

      val kafkaParams = getTestKafkaParams
      val topicPartitions: List[TopicPartition] = for (
        partitionId <- (0 until capacity).toList
      ) yield new TopicPartition("topic", partitionId)

      val keys: List[CacheKey] = topicPartitions.map { part =>
        new CacheKey(part, kafkaParams)
      }

      // while in loop pool doesn't still exceed total pool size
      val keyToPooledObjectPairs = borrowObjectsPerKey(pool, kafkaParams, keys)

      val moreTopicPartition = new TopicPartition("topic2", 0)
      val newCacheKey = new CacheKey(moreTopicPartition, kafkaParams)

      assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length,
        numTotal = keyToPooledObjectPairs.length)

      // already exceed limit on pool, and no idle object
      intercept[NoSuchElementException] {
        pool.borrowObject(newCacheKey, kafkaParams)
      }

      // no change on pool
      assertPoolState(pool, numIdle = 0, numActive = keyToPooledObjectPairs.length,
        numTotal = keyToPooledObjectPairs.length)

      // return 20% of objects to ensure there're some idle objects to free up later
      val numToReturn = (keyToPooledObjectPairs.length * 0.2).toInt
      returnObjects(pool, keyToPooledObjectPairs.take(numToReturn))

      assertPoolState(pool, numIdle = numToReturn,
        numActive = keyToPooledObjectPairs.length - numToReturn,
        numTotal = keyToPooledObjectPairs.length)

      // borrow it again: pool will clean up some of idle objects to add new object
      val newObject = pool.borrowObject(newCacheKey, kafkaParams)
      assertPooledObject(newObject, moreTopicPartition, kafkaParams)
      assertPoolStateForKey(pool, newCacheKey, numIdle = 0, numActive = 1, numTotal = 1)

      // at least one of idle object should be freed up
      assert(pool.getNumIdle < numToReturn)
      // we can determine number of active objects correctly
      assert(pool.getNumActive === keyToPooledObjectPairs.length - numToReturn + 1)
      // total objects should be more than number of active + 1 but can't expect exact number
      assert(pool.getTotal > keyToPooledObjectPairs.length - numToReturn + 1)
    } finally {
      conf.set("spark.sql.kafkaConsumerCache.capacity", prevKafkaConsumerCacheCapacity)
    }
  }

  test("invalidate key after borrowing consumer for the key") {
    val pool = InternalKafkaConsumerPool.build

    val topic = "topic"
    val partitionId = 0
    val topicPartition = new TopicPartition(topic, partitionId)

    val kafkaParams: ju.Map[String, Object] = getTestKafkaParams

    val key = new CacheKey(topicPartition, kafkaParams)
    val pooledObject = pool.borrowObject(key, kafkaParams)

    assertPooledObject(pooledObject, topicPartition, kafkaParams)
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 1, numTotal = 1)
    assertPoolState(pool, numIdle = 0, numActive = 1, numTotal = 1)

    // put 10 ms sleep to ensure invalidated timestamp is not same as borrowed timestamp
    Thread.sleep(10)

    pool.invalidateKey(key)

    pool.returnObject(pooledObject)

    // borrowed object before invalidating key should be invalidated instead of being idle
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 0, numTotal = 0)
    assertPoolState(pool, numIdle = 0, numActive = 0, numTotal = 0)

    // borrowing new object after invalidating key
    val pooledObject2 = pool.borrowObject(key, kafkaParams)

    assertPooledObject(pooledObject2, topicPartition, kafkaParams)
    assertPoolStateForKey(pool, key, numIdle = 0, numActive = 1, numTotal = 1)
    assertPoolState(pool, numIdle = 0, numActive = 1, numTotal = 1)

    pool.returnObject(pooledObject2)

    // this object should be kept idle since it is not a target of invalidation
    assertPoolStateForKey(pool, key, numIdle = 1, numActive = 0, numTotal = 1)
    assertPoolState(pool, numIdle = 1, numActive = 0, numTotal = 1)
  }

  private def assertPooledObject(
      pooledObject: InternalKafkaConsumer,
      expectedTopicPartition: TopicPartition,
      expectedKafkaParams: ju.Map[String, Object]): Unit = {
    assert(pooledObject != null)
    assert(pooledObject.kafkaParams === expectedKafkaParams)
    assert(pooledObject.topicPartition === expectedTopicPartition)
  }

  private def assertPoolState(pool: InternalKafkaConsumerPool, numIdle: Int,
                              numActive: Int, numTotal: Int): Unit = {
    assert(pool.getNumIdle === numIdle)
    assert(pool.getNumActive === numActive)
    assert(pool.getTotal === numTotal)
  }

  private def assertPoolStateForKey(pool: InternalKafkaConsumerPool, key: CacheKey,
                                    numIdle: Int, numActive: Int, numTotal: Int): Unit = {
    assert(pool.getNumIdle(key) === numIdle)
    assert(pool.getNumActive(key) === numActive)
    assert(pool.getTotal(key) === numTotal)
  }

  private def getTestKafkaParams: ju.Map[String, Object] = Map[String, Object](
    GROUP_ID_CONFIG -> "groupId",
    BOOTSTRAP_SERVERS_CONFIG -> "PLAINTEXT://localhost:9092",
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ).asJava

  private def borrowObjectsPerKey(
      pool: InternalKafkaConsumerPool,
      kafkaParams: ju.Map[String, Object],
      keys: List[CacheKey]): Seq[(CacheKey, InternalKafkaConsumer)] = {
    keys.map { key =>
      val numActiveBeforeBorrowing = pool.getNumActive
      val numIdleBeforeBorrowing = pool.getNumIdle
      val numTotalBeforeBorrowing = pool.getTotal

      val pooledObj = pool.borrowObject(key, kafkaParams)

      assertPoolStateForKey(pool, key, numIdle = 0, numActive = 1, numTotal = 1)
      assertPoolState(pool, numIdle = numIdleBeforeBorrowing,
        numActive = numActiveBeforeBorrowing + 1, numTotal = numTotalBeforeBorrowing + 1)

      (key, pooledObj)
    }
  }

  private def returnObjects(pool: InternalKafkaConsumerPool,
                            objects: Seq[(CacheKey, InternalKafkaConsumer)]): Unit = {
    objects.foreach { case (key, pooledObj) =>
      val numActiveBeforeReturning = pool.getNumActive
      val numIdleBeforeReturning = pool.getNumIdle
      val numTotalBeforeReturning = pool.getTotal

      pool.returnObject(pooledObj)

      // we only allow one idle object per key
      assertPoolStateForKey(pool, key, numIdle = 1, numActive = 0, numTotal = 1)
      assertPoolState(pool, numIdle = numIdleBeforeReturning + 1,
        numActive = numActiveBeforeReturning - 1, numTotal = numTotalBeforeReturning)
    }
  }
}

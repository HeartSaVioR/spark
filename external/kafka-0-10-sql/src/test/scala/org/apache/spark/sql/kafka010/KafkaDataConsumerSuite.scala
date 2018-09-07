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

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.sql.test.SharedSQLContext

class KafkaDataConsumerSuite extends SharedSQLContext with PrivateMethodTester {

  protected var testUtils: KafkaTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(Map[String, Object]())
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  test("SPARK-19886: Report error cause correctly in reportDataLoss") {
    val cause = new Exception("D'oh!")
    val reportDataLoss = PrivateMethod[Unit]('reportDataLoss0)
    val e = intercept[IllegalStateException] {
      KafkaDataConsumer.invokePrivate(reportDataLoss(true, "message", cause))
    }
    assert(e.getCause === cause)
  }

  test("SPARK-23623: concurrent use of KafkaDataConsumer") {
    val topic = "topic" + Random.nextInt()
    val data: immutable.IndexedSeq[String] = prepareTestTopicHavingTestMessages(topic)

    val topicPartition = new TopicPartition(topic, 0)

    import ConsumerConfig._
    val kafkaParams = Map[String, Object](
      GROUP_ID_CONFIG -> "groupId",
      BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val numThreads = 100
    val numConsumerUsages = 500

    @volatile var error: Throwable = null

    def consume(i: Int): Unit = {
      val taskContext = if (Random.nextBoolean) {
        new TaskContextImpl(0, 0, 0, 0, attemptNumber = Random.nextInt(2), null, null, null)
      } else {
        null
      }
      TaskContext.setTaskContext(taskContext)
      val consumer = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
      try {
        val range = consumer.getAvailableOffsetRange()
        val rcvd = range.earliest until range.latest map { offset =>
          val bytes = consumer.get(offset, Long.MaxValue, 10000, failOnDataLoss = false).value()
          new String(bytes)
        }
        assert(rcvd == data)
      } catch {
        case e: Throwable =>
          error = e
          throw e
      } finally {
        consumer.release()
      }
    }

    val threadpool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numConsumerUsages).map { i =>
        threadpool.submit(new Runnable {
          override def run(): Unit = { consume(i) }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
      assert(error == null)
    } finally {
      threadpool.shutdown()
    }
  }

  test("SPARK-25251 Handles multiple tasks in executor fetching same (topic, partition) pair") {
    val topic = "topic" + Random.nextInt()
    prepareTestTopicHavingTestMessages(topic)
    val topicPartition = new TopicPartition(topic, 0)

    import ConsumerConfig._
    val kafkaParams = Map[String, Object](
      GROUP_ID_CONFIG -> "groupId",
      BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    // reset statistic in fetched data pool so that we can verify expected behavior
    val fetchedDataPoolHack = PrivateMethod[FetchedDataPool]('fetchedDataPool)
    val fetchedDataPool = KafkaDataConsumer.invokePrivate(fetchedDataPoolHack())
    fetchedDataPool.resetStatistic()

    val taskContext = new TaskContextImpl(0, 0, 0, 0, 0, null, null, null)
    TaskContext.setTaskContext(taskContext)

    // task A trying to fetch offset 0 to 100, and read 5 records
    val consumer1 = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
    val lastOffsetForConsumer1 = readAndGetLastOffset(consumer1, 0, 100, 5)
    consumer1.release()

    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

    // task B trying to fetch offset 300 to 500, and read 5 records
    val consumer2 = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
    val lastOffsetForConsumer2 = readAndGetLastOffset(consumer2, 300, 500, 5)
    consumer2.release()

    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)

    // task A continue reading from the last offset + 1, with upper bound 100 again
    val consumer1a = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)

    consumer1a.get(lastOffsetForConsumer1 + 1, 100, 10000, failOnDataLoss = false)
    consumer1a.release()

    // pool should succeed to provide cached data instead of creating one
    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)

    // task B also continue reading from the last offset + 1, with upper bound 500 again
    val consumer2a = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)

    consumer2a.get(lastOffsetForConsumer2 + 1, 500, 10000, failOnDataLoss = false)
    consumer2a.release()

    // same expectation: pool should succeed to provide cached data instead of creating one
    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)
  }

  test("SPARK-25251 Handles multiple tasks in executor fetching same (topic, partition) pair " +
    "and same offset (edge-case) - data in use") {
    val topic = "topic" + Random.nextInt()
    prepareTestTopicHavingTestMessages(topic)
    val topicPartition = new TopicPartition(topic, 0)

    import ConsumerConfig._
    val kafkaParams = Map[String, Object](
      GROUP_ID_CONFIG -> "groupId",
      BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    // reset statistic in fetched data pool so that we can verify expected behavior
    val fetchedDataPoolHack = PrivateMethod[FetchedDataPool]('fetchedDataPool)
    val fetchedDataPool = KafkaDataConsumer.invokePrivate(fetchedDataPoolHack())
    fetchedDataPool.resetStatistic()

    val taskContext = new TaskContextImpl(0, 0, 0, 0, 0, null, null, null)
    TaskContext.setTaskContext(taskContext)

    // task A trying to fetch offset 0 to 100, and read 5 records (still reading)
    val consumer1 = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
    val lastOffsetForConsumer1 = readAndGetLastOffset(consumer1, 0, 100, 5)

    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

    // task B trying to fetch offset the last offset task A is reading so far + 1 to 500
    // this is a condition for edge case
    val consumer2 = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
    consumer2.get(lastOffsetForConsumer1 + 1, 100, 10000, failOnDataLoss = false)

    // Pool must create a new fetched data instead of returning existing on now in use even
    // there's fetched data matching start offset.
    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 2, expectedNumTotal = 2)

    consumer1.release()
    consumer2.release()
  }

  test("SPARK-25251 Handles multiple tasks in executor fetching same (topic, partition) pair " +
    "and same offset (edge-case) - data not in use") {
    val topic = "topic" + Random.nextInt()
    prepareTestTopicHavingTestMessages(topic)
    val topicPartition = new TopicPartition(topic, 0)

    import ConsumerConfig._
    val kafkaParams = Map[String, Object](
      GROUP_ID_CONFIG -> "groupId",
      BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    // reset statistic in fetched data pool so that we can verify expected behavior
    val fetchedDataPoolHack = PrivateMethod[FetchedDataPool]('fetchedDataPool)
    val fetchedDataPool = KafkaDataConsumer.invokePrivate(fetchedDataPoolHack())
    fetchedDataPool.resetStatistic()

    val taskContext = new TaskContextImpl(0, 0, 0, 0, 0, null, null, null)
    TaskContext.setTaskContext(taskContext)

    // task A trying to fetch offset 0 to 100, and read 5 records (still reading)
    val consumer1 = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
    val lastOffsetForConsumer1 = readAndGetLastOffset(consumer1, 0, 100, 5)
    consumer1.release()

    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

    // task B trying to fetch offset the last offset task A is reading so far + 1 to 500
    // this is a condition for edge case
    val consumer2 = KafkaDataConsumer.acquire(topicPartition, kafkaParams.asJava)
    consumer2.get(lastOffsetForConsumer1 + 1, 100, 10000, failOnDataLoss = false)

    // Pool cannot determine the origin task, so it has to just provide matching one.
    // task A may come back and try to fetch, and cannot find previous data (or the data is in use).
    // If then task A may have to fetch from Kafka, but we already avoided fetching from Kafka in
    // task B, so it is not a big deal in overall.
    assertFetchedDataPoolStatistic(fetchedDataPool, expectedNumCreated = 1, expectedNumTotal = 1)

    consumer2.release()
  }

  private def assertFetchedDataPoolStatistic(
      fetchedDataPool: FetchedDataPool,
      expectedNumCreated: Long,
      expectedNumTotal: Long): Unit = {
    assert(fetchedDataPool.getNumCreated === expectedNumCreated)
    assert(fetchedDataPool.getNumTotal === expectedNumTotal)
  }

  private def readAndGetLastOffset(consumer: KafkaDataConsumer, startOffset: Long,
                                   untilOffset: Long, numToRead: Int): Long = {
    var lastOffset: Long = startOffset - 1
    (0 until 5).foreach { _ =>
      val record = consumer.get(lastOffset + 1, untilOffset, 10000, failOnDataLoss = false)
      // validation for fetched record is covered by other tests, so skip on validating
      lastOffset = record.offset()
    }
    lastOffset
  }

  private def prepareTestTopicHavingTestMessages(topic: String) = {
    val data = (1 to 1000).map(_.toString)
    testUtils.createTopic(topic, 1)
    testUtils.sendMessages(topic, data.toArray)
    data
  }
}

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

import scala.language.implicitConversions
import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.{ByteArrayPair, RocksDB, RocksDBConf}
import org.apache.spark.util.{NextIterator, Utils}

object RocksDBEstimatedNumKeys extends Logging {
  def main(args: Array[String]): Unit = {
    def testOps(compactOnCommit: Boolean): Unit = {
      logWarning(s"Running tests... compactOnCommit: $compactOnCommit")

      val remoteDir = Utils.createTempDir().toString
      new File(remoteDir).delete()  // to make sure that the directory gets created

      val conf = RocksDBConf().copy(compactOnCommit = compactOnCommit)

      withDB(remoteDir, conf = conf, version = 0) { db =>
        var latestVersion = 0L

        var seqChar = 'a'
        var seqNum = 0L

        def reportApproxNumKeys(): Unit = {
          val iter = db.iterator().asInstanceOf[NextIterator[ByteArrayPair]]
          val numKeys = iter.count(_ => true)
          iter.closeIfNeeded()

          val approxNumKeys = db.metrics.approxNumKeys
          val numKeyDiffAbs = Math.abs(approxNumKeys - numKeys)
          val incorrectRate = numKeyDiffAbs * 1.0d / numKeys

          logWarning(s"Version\t$latestVersion\tApproxNumKeys\t$approxNumKeys\tActualNumKeys" +
            s"\t$numKeys\tIncorrectRate\t${incorrectRate * 100}")
        }

        def createNonExistingKey(): String = {
          if (seqNum >= Long.MaxValue) {
            seqNum = 0L
            seqChar = (seqChar + 1).asInstanceOf[Char]

            if (seqChar >= 'z') {
              throw new Exception("exceeds key range!!")
            }
          } else {
            seqNum += 1
          }

          s"$seqChar-$seqNum"
        }

        var numOps = 0L
        while (numOps < 10000) {
          val op = Random.nextInt(100)
          op match {
            case s if (s >= 0 && s < 50) =>
              (0 to Random.nextInt(100)).foreach { _ =>
                val key = createNonExistingKey()
                db.put(key, toStr("1"))
              }

            case s if s >= 50 && s < 60 =>
              // NOTE: need to consume full iterator!
              val iter = db.iterator().asInstanceOf[NextIterator[ByteArrayPair]]
              if (iter.nonEmpty) {
                iter.take(1).foreach { pair =>
                  db.put(pair.key, toStr("2"))
                }
              }
              iter.closeIfNeeded()

            case s if s >= 60 && s < 70 =>
              val key = createNonExistingKey()
              db.remove(key)

            case s if (s >= 70 && s < 80 || (s >= 90 && s < 98)) =>
              // NOTE: need to consume full iterator!
              val iter = db.iterator().asInstanceOf[NextIterator[ByteArrayPair]]
              if (iter.nonEmpty) {
                iter.take(1).foreach { pair =>
                  db.remove(pair.key)
                }
              }
              iter.closeIfNeeded()

            case s if s >= 80 && s < 85 =>
              val key = createNonExistingKey()
              db.get(key)

            case s if s >= 85 && s < 90 =>
              // NOTE: need to consume full iterator!
              val iter = db.iterator().asInstanceOf[NextIterator[ByteArrayPair]]
              if (iter.nonEmpty) {
                iter.take(1).foreach { pair =>
                  db.get(pair.key)
                }
              }
              iter.closeIfNeeded()

            case s if s == 98 || s == 99 =>
              latestVersion = db.commit()

              reportApproxNumKeys()

            case _ => // no-op
          }

          numOps += 1
        }
      }
    }

    for (compactOnCommit <- Seq(false, true)) {
      testOps(compactOnCommit)
    }
  }

  implicit def toFile(path: String): File = new File(path)

  implicit def toArray(str: String): Array[Byte] = if (str != null) str.getBytes else null

  implicit def toStr(bytes: Array[Byte]): String = if (bytes != null) new String(bytes) else null

  private def withDB[T](
      remoteDir: String,
      version: Int = 0,
      conf: RocksDBConf = RocksDBConf().copy(compactOnCommit = false, minVersionsToRetain = 100),
      hadoopConf: Configuration = new Configuration())(
      func: RocksDB => T): T = {
    var db: RocksDB = null
    try {
      db = new RocksDB(
        remoteDir, conf = conf, hadoopConf = hadoopConf,
        loggingId = s"[Thread-${Thread.currentThread.getId}]")
      db.load(version)
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }
}

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

package org.apache.spark.deploy.history

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale
import java.util.zip.{ZipEntry, ZipFile, ZipOutputStream}

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{Options, ReadOptions}
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.status._
import org.apache.spark.status.KVUtils.KVStoreScalaSerializer
import org.apache.spark.status.api.v1.{JobData, StageData}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}

class LevelDBSnapshotPocSuite extends SparkFunSuite with PrivateMethodTester with Logging {

  test("LevelDB only: Restore from LevelDB directory") {
    // scalastyle:off println

    withTempDir { tmpDir =>
      val originDir = new File(tmpDir, "origin")
      originDir.mkdirs()

      val snapshotDir = new File(tmpDir, "snapshot")
      snapshotDir.mkdirs()

      val restoreDir = new File(tmpDir, "restore")
      restoreDir.mkdirs()

      println(s"DEBUG: Created directories in $tmpDir")

      val metadata = new AppStatusStoreMetadata(AppStatusStore.CURRENT_VERSION)

      val appCompleted = false
      val originLevelDb = KVUtils.open(originDir, metadata) // new LevelDB(originDir)
      val conf = new SparkConf()
      val replayConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
      val trackingStore = new ElementTrackingStore(originLevelDb, replayConf)
      val replayBus = new ReplayListenerBus()
      val listener = new AppStatusListener(trackingStore, replayConf, false)
      replayBus.addListener(listener)

      val testingFileUri = this.getClass
        .getResource("/spark-events/application_1553914137147_0018")
        .toURI

      val testingFilePath = new Path(testingFileUri)
      val fs = testingFilePath.getFileSystem(new Configuration())

      println(s"DEBUG: testing event log file size: ${fs.getFileStatus(testingFilePath).getLen}")

      val ((jobs, stages), loadElapsedMs) = Utils.timeTakenMs {
        Utils.tryWithResource(EventLoggingListener.openEventLog(testingFilePath, fs)) { in =>
          replayBus.replay(in, testingFilePath.getName, !appCompleted)
        }

        trackingStore.close(closeParent = false)

        val store = new AppStatusStore(originLevelDb)
        (store.jobsList(null), store.stageList(null))
      }

      println(s"DEBUG: job data count: ${jobs.length}")
      println(s"DEBUG: first 3 jobs: ${jobs.take(3).map(jobToString)}")
      println(s"DEBUG: stage data count: ${stages.length}")
      println(s"DEBUG: first 3 stages: ${stages.take(3).map(stageToString)}")

      println(s"DEBUG: load took $loadElapsedMs ms")

      originLevelDb.close()

      val archivedPath = new File(snapshotDir, "event-snapshot.zip").toPath
      val (_, archiveMs) = Utils.timeTakenMs {
        zip(archivedPath, originDir.listFiles().map(_.toPath))
      }

      println(s"DEBUG: archiving LevelDB directory took $archiveMs ms - " +
        s"size: ${Files.size(archivedPath)}")

      val (_, extractMs) = Utils.timeTakenMs {
        unzip(archivedPath, restoreDir.toPath)
      }

      println(s"DEBUG: extracting snapshot to LevelDB directory took $extractMs ms")

      val restoredLevelDb = KVUtils.open(restoreDir, metadata) // new LevelDB(restoreDir)
      val restoredTrackingStore = new ElementTrackingStore(restoredLevelDb, replayConf)
      val restoredReplayBus = new ReplayListenerBus()
      val restoredListener = new AppStatusListener(restoredTrackingStore, replayConf, false)
      restoredReplayBus.addListener(restoredListener)

      val store2 = new AppStatusStore(restoredTrackingStore)
      val jobs2 = store2.jobsList(null)
      val stages2 = store2.stageList(null)

      println(s"DEBUG: job data count: ${jobs2.length}")
      println(s"DEBUG: first 3 jobs: ${jobs2.take(3).map(jobToString)}")
      println(s"DEBUG: stage data count: ${stages2.length}")
      println(s"DEBUG: first 3 stages: ${stages2.take(3).map(stageToString)}")

      assert(jobs.map(j => (j.jobId, j.name, j.status)) ===
        jobs2.map(j => (j.jobId, j.name, j.status)))
      assert(stages.map(s => (s.stageId, s.name, s.status)) ===
        stages2.map(s => (s.stageId, s.name, s.status)))
    }

    // scalastyle:on println
  }

  test("LevelDB only: Restore from snapshot file leveraging snapshot feature") {
    // scalastyle:off println

    withTempDir { tmpDir =>
      val originDir = new File(tmpDir, "origin")
      originDir.mkdirs()

      val snapshotDir = new File(tmpDir, "snapshot")
      snapshotDir.mkdirs()

      val restoreDir = new File(tmpDir, "restore")
      restoreDir.mkdirs()

      println(s"DEBUG: Created directories in $tmpDir")

      val metadata = new AppStatusStoreMetadata(AppStatusStore.CURRENT_VERSION)

      val appCompleted = false
      val originLevelDb = KVUtils.open(originDir, metadata) // new LevelDB(originDir)
      val conf = new SparkConf()
      val replayConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
      val trackingStore = new ElementTrackingStore(originLevelDb, replayConf)
      val replayBus = new ReplayListenerBus()
      val listener = new AppStatusListener(trackingStore, replayConf, false)
      replayBus.addListener(listener)

      val testingFileUri = this.getClass
        .getResource("/spark-events/application_1553914137147_0018")
        .toURI

      val testingFilePath = new Path(testingFileUri)
      val fs = testingFilePath.getFileSystem(new Configuration())

      println(s"DEBUG: testing event log file size: ${fs.getFileStatus(testingFilePath).getLen}")

      val ((jobs, stages), loadElapsedMs) = Utils.timeTakenMs {
        Utils.tryWithResource(EventLoggingListener.openEventLog(testingFilePath, fs)) { in =>
          replayBus.replay(in, testingFilePath.getName, !appCompleted)
        }

        trackingStore.close(closeParent = false)

        val store = new AppStatusStore(originLevelDb)
        (store.jobsList(null), store.stageList(null))
      }

      println(s"DEBUG: job data count: ${jobs.length}")
      println(s"DEBUG: first 3 jobs: ${jobs.take(3).map(jobToString)}")
      println(s"DEBUG: stage data count: ${stages.length}")
      println(s"DEBUG: first 3 stages: ${stages.take(3).map(stageToString)}")

      println(s"DEBUG: load took $loadElapsedMs ms")

      originLevelDb.close()

      val snapshotFile = new File(snapshotDir, "event.snapshot")
      val (_, snapshotMs) = Utils.timeTakenMs {
        dumpSnapshot(originDir, snapshotFile)
      }

      println(s"DEBUG: writing snapshot took $snapshotMs ms - " +
        s"size: ${Files.size(snapshotFile.toPath)}")

      val (_, restoreMs) = Utils.timeTakenMs {
        restoreLevelDBFromSnapshot(snapshotFile, restoreDir)
      }
      println(s"DEBUG: Reading from snapshot took $restoreMs ms")

      val restoredLevelDb = KVUtils.open(restoreDir, metadata) // new LevelDB(restoreDir)
      val restoredTrackingStore = new ElementTrackingStore(restoredLevelDb, replayConf)
      val restoredReplayBus = new ReplayListenerBus()
      val restoredListener = new AppStatusListener(restoredTrackingStore, replayConf, false)
      restoredReplayBus.addListener(restoredListener)

      val store2 = new AppStatusStore(restoredTrackingStore)
      val jobs2 = store2.jobsList(null)
      val stages2 = store2.stageList(null)

      println(s"DEBUG: job data count: ${jobs2.length}")
      println(s"DEBUG: first 3 jobs: ${jobs2.take(3).map(jobToString)}")
      println(s"DEBUG: stage data count: ${stages2.length}")
      println(s"DEBUG: first 3 stages: ${stages2.take(3).map(stageToString)}")

      assert(jobs.map(j => (j.jobId, j.name, j.status)) ===
        jobs2.map(j => (j.jobId, j.name, j.status)))
      assert(stages.map(s => (s.stageId, s.name, s.status)) ===
        stages2.map(s => (s.stageId, s.name, s.status)))
    }

    // scalastyle:on println
  }

  (for {
    x <- Seq("inmemory", "leveldb")
    y <- Seq("inmemory", "leveldb")
  } yield (x, y)).foreach { matrix =>
    val (input, output) = matrix
    test(s"Restore from snapshot file leveraging KVStore interface - $input to $output") {
      runSnapshotAndRestoreViaKVStoreInterface(input, output)
    }
  }

  private def openStore(storeType: String, dir: File): KVStore = {
    storeType.toLowerCase(Locale.ROOT) match {
      case "inmemory" => new InMemoryStore
      case "leveldb" =>
        val metadata = new AppStatusStoreMetadata(AppStatusStore.CURRENT_VERSION)
        KVUtils.open(dir, metadata)
    }
  }

  private def runSnapshotAndRestoreViaKVStoreInterface(input: String, output: String): Unit = {
    // scalastyle:off println
    withTempDir { tmpDir =>
      val originDir = new File(tmpDir, "origin")
      originDir.mkdirs()

      val snapshotDir = new File(tmpDir, "snapshot")
      snapshotDir.mkdirs()

      val restoreDir = new File(tmpDir, "restore")
      restoreDir.mkdirs()

      println(s"DEBUG: Created directories in $tmpDir")

      // originDir must be clean directory
      val originStore = openStore(input, originDir)

      val appCompleted = false
      val conf = new SparkConf()
      val replayConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
      val trackingStore = new ElementTrackingStore(originStore, replayConf)
      val replayBus = new ReplayListenerBus()
      val listener = new AppStatusListener(trackingStore, replayConf, false)
      replayBus.addListener(listener)

      val testingFileUri = this.getClass
        .getResource("/spark-events/application_1553914137147_0018")
        .toURI

      val testingFilePath = new Path(testingFileUri)
      val fs = testingFilePath.getFileSystem(new Configuration())

      println(s"DEBUG: testing event log file size: ${fs.getFileStatus(testingFilePath).getLen}")

      val ((jobs, stages), loadElapsedMs) = Utils.timeTakenMs {
        Utils.tryWithResource(EventLoggingListener.openEventLog(testingFilePath, fs)) { in =>
          replayBus.replay(in, testingFilePath.getName, !appCompleted)
        }

        trackingStore.close(closeParent = false)

        val store = new AppStatusStore(originStore)
        (store.jobsList(null), store.stageList(null))
      }

      println(s"DEBUG: job data count: ${jobs.length}")
      println(s"DEBUG: first 3 jobs: ${jobs.take(3).map(jobToString)}")
      println(s"DEBUG: stage data count: ${stages.length}")
      println(s"DEBUG: first 3 stages: ${stages.take(3).map(stageToString)}")

      println(s"DEBUG: load took $loadElapsedMs ms")

      val snapshotFile = new File(snapshotDir, "event.snapshot")

      val (_, snapshotMs) = Utils.timeTakenMs {
        // KVStore should not be modified, or should provide consistent view while dumping
        dumpSnapshotViaKVStore(originStore, snapshotFile)
      }

      println(s"DEBUG: writing snapshot took $snapshotMs ms - " +
        s"size: ${Files.size(snapshotFile.toPath)}")

      originStore.close()

      // restoreDir must be clean directory
      val restoreStore = openStore(output, restoreDir)
      val restoredTrackingStore = new ElementTrackingStore(restoreStore, replayConf)

      val (_, restoreMs) = Utils.timeTakenMs {
        restoreKVStoreFromSnapshot(restoredTrackingStore, snapshotFile)
      }
      println(s"DEBUG: Reading from snapshot took $restoreMs ms")

      val restoredReplayBus = new ReplayListenerBus()
      val restoredListener = new AppStatusListener(restoredTrackingStore, replayConf, false)
      restoredReplayBus.addListener(restoredListener)

      val store2 = new AppStatusStore(restoredTrackingStore)
      val jobs2 = store2.jobsList(null)
      val stages2 = store2.stageList(null)

      println(s"DEBUG: job data count: ${jobs2.length}")
      println(s"DEBUG: first 3 jobs: ${jobs2.take(3).map(jobToString)}")
      println(s"DEBUG: stage data count: ${stages2.length}")
      println(s"DEBUG: first 3 stages: ${stages2.take(3).map(stageToString)}")

      assert(jobs.map(j => (j.jobId, j.name, j.status)) ===
        jobs2.map(j => (j.jobId, j.name, j.status)))
      assert(stages.map(s => (s.stageId, s.name, s.status)) ===
        stages2.map(s => (s.stageId, s.name, s.status)))

      restoredTrackingStore.close(closeParent = true)
    }

    // scalastyle:on println
  }

  private def jobToString(data: JobData): String = {
    // only takes some fields...
    s"<jobId: ${data.jobId}, name: ${data.name}, status: ${data.status}, ...>"
  }

  private def stageToString(data: StageData): String = {
    // only takes some fields...
    s"<stageId: ${data.stageId}, name: ${data.name}, status: ${data.status}, ...>"
  }

  private def zip(out: java.nio.file.Path, files: Iterable[java.nio.file.Path]): Unit = {
    val zip = new ZipOutputStream(Files.newOutputStream(out))
    files.foreach { file =>
      // FIXME: DEBUG - this is just testing purpose so let's leverage the fact that
      //  LevelDB creates all files in its directory.
      zip.putNextEntry(new ZipEntry(file.getFileName.toString))
      Files.copy(file, zip)
      zip.closeEntry()
    }
    zip.close()
  }

  private def unzip(zipPath: java.nio.file.Path, outputPath: java.nio.file.Path): Unit = {
    import scala.collection.JavaConverters._

    val zipFile = new ZipFile(zipPath.toFile)
    for (entry <- zipFile.entries.asScala) {
      val path = outputPath.resolve(entry.getName)
      if (entry.isDirectory) {
        Files.createDirectories(path)
      } else {
        Files.createDirectories(path.getParent)
        Files.copy(zipFile.getInputStream(entry), path)
      }
    }
  }

  private def dumpSnapshot(dirLevelDB: File, snapshotFile: File): Unit = {
    val options = new Options
    options.createIfMissing(true)
    val db = JniDBFactory.factory.open(dirLevelDB, options)

    try {
      val snapshot = db.getSnapshot
      val readOption = new ReadOptions().snapshot(snapshot)
      val dbIter = db.iterator(readOption)

      /*
      https://github.com/fusesource/leveldbjni

      DBIterator iterator = db.iterator();
      try {
        for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
          String key = asString(iterator.peekNext().getKey());
          String value = asString(iterator.peekNext().getValue());
          System.out.println(key+" = "+value);
        }
      } finally {
        // Make sure you close the iterator to avoid resource leaks.
        iterator.close();
      }
      */
      try {
        dbIter.seekToFirst()

        val output = new DataOutputStream(new FileOutputStream(snapshotFile))
        while (dbIter.hasNext) {
          val entry = dbIter.peekNext()

          val keyBytes = entry.getKey()
          val valueBytes = entry.getValue()
          output.writeInt(keyBytes.size)
          output.write(keyBytes)
          output.writeInt(valueBytes.size)
          output.write(valueBytes)

          dbIter.next()
        }
        output.writeInt(-1)
        output.close()
      } finally {
        dbIter.close()
        readOption.snapshot().close()
      }
    } finally {
      db.close()
    }
  }

  private def restoreLevelDBFromSnapshot(snapshotFile: File, restoreDir: File): Unit = {
    val options = new Options
    options.createIfMissing(true)
    val db = JniDBFactory.factory.open(restoreDir, options)
    try {
      var writeBatch = db.createWriteBatch()
      try {
        val input = new DataInputStream(new FileInputStream(snapshotFile))
        var eof = false
        var count = 0
        while (!eof) {
          val keySize = input.readInt()
          if (keySize == -1) {
            eof = true
          } else if (keySize < 0) {
            throw new IOException("DEBUG - something is wrong!")
          } else {
            val keyRowBuffer = new Array[Byte](keySize)
            ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

            val valueSize = input.readInt()
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)

            writeBatch.put(keyRowBuffer, valueRowBuffer)
            count += 1

            if (count % 100 == 0) {
              db.write(writeBatch)
              writeBatch.close()
              writeBatch = db.createWriteBatch()
            }
          }
        }
        input.close()
        db.write(writeBatch)
      } finally {
        writeBatch.close()
      }
    } finally {
      db.close()
    }
  }

  private def dumpSnapshotViaKVStore(store: KVStore, snapshotFile: File): Unit = {
    val output = new DataOutputStream(new FileOutputStream(snapshotFile))

    val serializer = new KVStoreScalaSerializer()

    // store metadata if it exists
    val metadataType = store.metadataType()
    if (metadataType != null) {
      val clazzName = metadataType.getCanonicalName.getBytes(StandardCharsets.UTF_8)
      output.writeInt(clazzName.length)
      output.write(clazzName)
      val metadata = store.getMetadata(metadataType)
      val ser = serializer.serialize(metadata)
      output.writeInt(ser.length)
      output.write(ser)
      output.writeInt(-2)
    } else {
      output.writeInt(-2)
    }

    val types = store.types()
    import scala.collection.JavaConverters._
    types.asScala.foreach { clazz =>
      val clazzName = clazz.getCanonicalName.getBytes(StandardCharsets.UTF_8)
      output.writeInt(clazzName.length)
      output.write(clazzName)

      val view = store.view(clazz)
      view.iterator().asScala.foreach { obj =>
        val ser = serializer.serialize(obj)
        output.writeInt(ser.length)
        output.write(ser)
      }
      output.writeInt(-2)
    }
    output.writeInt(-1)

    output.close()
  }

  private def restoreKVStoreFromSnapshot(store: KVStore, snapshotFile: File): Unit = {
    val input = new DataInputStream(new FileInputStream(snapshotFile))

    val serializer = new KVStoreScalaSerializer()
    // first one would be metadata
    val metadataClazzNameLen = input.readInt()
    if (metadataClazzNameLen > 0) {
      // metadata presented
      val metadataClazzNameBuffer = new Array[Byte](metadataClazzNameLen)
      ByteStreams.readFully(input, metadataClazzNameBuffer, 0, metadataClazzNameLen)
      val metadataClazzName = new String(metadataClazzNameBuffer, StandardCharsets.UTF_8)

      // Without explicitly setting AnyRef it works a bit weird,
      // Class[Nothing$] seems to be inferred as type of metadataClazz
      val metadataClazz = Utils.classForName[AnyRef](metadataClazzName)

      val objLen = input.readInt()
      val objBuffer = new Array[Byte](objLen)
      ByteStreams.readFully(input, objBuffer, 0, objLen)
      val obj = serializer.deserialize(objBuffer, metadataClazz)
      store.setMetadata(obj)
      // additionally read -2 as end of type
      val eotCode = input.readInt()
      require(eotCode == -2,
        s"The notion of 'end of type' is expected here, but got $eotCode instead")
    }

    var eof = false
    while (!eof) {
      val typeClazzNameLen = input.readInt()
      if (typeClazzNameLen == -1) {
        eof = true
      } else {
        val typeClazzNameBuffer = new Array[Byte](typeClazzNameLen)
        ByteStreams.readFully(input, typeClazzNameBuffer, 0, typeClazzNameLen)
        val typeClazzName = new String(typeClazzNameBuffer, StandardCharsets.UTF_8)

        // Without explicitly setting AnyRef it works a bit weird,
        // Class[Nothing$] seems to be inferred as type of typeClazz
        val typeClazz = Utils.classForName[AnyRef](typeClazzName)

        var eot = false
        while (!eot) {
          val objLen = input.readInt()
          if (objLen == -2) {
            eot = true
          } else {
            val objBuffer = new Array[Byte](objLen)
            ByteStreams.readFully(input, objBuffer, 0, objLen)
            val obj = serializer.deserialize(objBuffer, typeClazz)
            store.write(obj)
          }
        }
      }
    }

    input.close()
  }
}

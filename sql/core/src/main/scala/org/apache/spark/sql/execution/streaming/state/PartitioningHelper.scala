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

import java.io.StringReader

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.streaming.MetadataVersionUtil
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Helper classes for reading/writing state partitioning.
 */
object PartitioningHelper {
  sealed trait PartitioningReader {
    def read(inputStream: FSDataInputStream): Seq[Seq[(String, DataType)]]
  }

  object PartitioningReader {
    def createPartitioningReader(versionStr: String): PartitioningReader = {
      val version = MetadataVersionUtil.validateVersion(versionStr,
        StatePartitioningManager.VERSION)
      version match {
        case 1 => new PartitioningV1Reader
      }
    }
  }

  class PartitioningV1Reader extends PartitioningReader {
    def read(inputStream: FSDataInputStream): Seq[Seq[(String, DataType)]] = {
      val buf = new StringBuilder
      val numKeyChunks = inputStream.readInt()
      (0 until numKeyChunks).foreach(_ => buf.append(inputStream.readUTF()))
      val partitioningStr = buf.toString()

      val structType = StructType.fromString(partitioningStr)
      structType.fields.toSeq.map {
        case StructField(_, dataType: StructType, _, _) =>
          // each field represents a set of attribute
          dataType.fields.toSeq.map { field =>
            (field.name, field.dataType)
          }

        // FIXME: better error message!
        case _ =>
          throw new IllegalStateException("Shouldn't reach here!")
      }
    }
  }

  trait PartitioningWriter {
    val version: Int

    final def write(
        groupingAttributes: Seq[Seq[Attribute]],
        outputStream: FSDataOutputStream): Unit = {
      writeVersion(outputStream)
      writePartitioning(groupingAttributes, outputStream)
    }

    private def writeVersion(outputStream: FSDataOutputStream): Unit = {
      outputStream.writeUTF(s"v${version}")
    }

    protected def writePartitioning(
        groupingAttributes: Seq[Seq[Attribute]],
        outputStream: FSDataOutputStream): Unit
  }

  object PartitioningWriter {
    def createPartitioningWriter(version: Int): PartitioningWriter = {
      version match {
        case 1 => new PartitioningV1Writer
      }
    }
  }

  class PartitioningV1Writer extends PartitioningWriter {
    val version: Int = 1

    // 2^16 - 1 bytes
    final val MAX_UTF_CHUNK_SIZE = 65535

    override protected def writePartitioning(
        groupingAttributes: Seq[Seq[Attribute]],
        outputStream: FSDataOutputStream): Unit = {
      val buf = new Array[Char](MAX_UTF_CHUNK_SIZE)

      val convertedFields = groupingAttributes.zipWithIndex.map { case (grouping, idx) =>
        StructField(s"criteria$idx", grouping.toStructType)
      }
      val converted = StructType(convertedFields)

      // DataOutputStream.writeUTF can't write a string at once
      // if the size exceeds 65535 (2^16 - 1) bytes.
      // So a key as well as a value consist of multiple chunks in schema version 2.
      val json = converted.json
      val numKeyChunks = (json.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val jsonStringReader = new StringReader(json)
      outputStream.writeInt(numKeyChunks)
      (0 until numKeyChunks).foreach { _ =>
        val numRead = jsonStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }
    }
  }
}

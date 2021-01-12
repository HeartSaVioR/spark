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
package org.apache.spark.sql.execution.datasources.v2.state

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, SortOrder}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StateTable(
    session: SparkSession,
    override val schema: StructType,
    stateCheckpointLocation: String,
    version: Long,
    operatorId: Int,
    storeName: String)
  extends Table with SupportsRead with SupportsWrite {

  import StateTable._

  if (!isValidSchema(schema)) {
    throw new AnalysisException("The fields of schema should be 'key' and 'value', " +
      "and each field should have corresponding fields (they should be a StructType)")
  }

  override def name(): String =
    s"state-table-cp-$stateCheckpointLocation-ver-$version-operator-$operatorId-store-$storeName"

  override def capabilities(): util.Set[TableCapability] = CAPABILITY

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StateScanBuilder(session, schema, stateCheckpointLocation, version, operatorId, storeName)

  override def properties(): util.Map[String, String] = Map(
      "stateCheckpointLocation" -> stateCheckpointLocation,
      "version" -> version.toString,
      "operatorId" -> operatorId.toString,
      "storeName" -> storeName).asJava

  private def isValidSchema(schema: StructType): Boolean = {
    if (schema.fieldNames.toSeq != Seq("key", "value")) {
      false
    } else if (!SchemaUtil.getSchemaAsDataType(schema, "key").isInstanceOf[StructType]) {
      false
    } else if (!SchemaUtil.getSchemaAsDataType(schema, "value").isInstanceOf[StructType]) {
      false
    } else {
      true
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val options = info.options()
    // TODO: how we could support the case where end users want to retain the number of
    //  partitions during rewrite?
    require(options.containsKey("numPartitions"), "Option 'numPartitions' is missing.")
    val numPartitions = options.get("numPartitions").toInt

    new WriteBuilder {
      override def build(): Write = new Write with RequiresDistributionAndOrdering {
        override def requiredDistribution(): Distribution = {
          val keySchema = SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
          // TODO: expand this to cover multi-depth (nested) columns (do we want to cover it?)
          val fullPathsForKeyColumns = keySchema.map { key =>
            FieldReference(s"key.${key.name}")
          }.toArray[Expression]
          Distributions.clustered(fullPathsForKeyColumns, numPartitions)
        }

        override def requiredOrdering(): Array[SortOrder] = Array.empty[SortOrder]

        override def toBatch: BatchWrite = {
          new StateBatchWrite
        }
      }
    }
  }

  class StateBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      new StateBatchDataWriterFactory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      // FIXME: anything specific to do?
      //  1. updates schema file?
      //  2. updates the number of partitions?
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
      // FIXME: should we clean up the partial writes here?
    }
  }

  class StateBatchDataWriterFactory extends DataWriterFactory {
    override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
      // FIXME: ...fill here...
    }
  }
}

object StateTable {
  private val CAPABILITY = Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava
}

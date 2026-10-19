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

package org.apache.spark.sql.execution.python

import java.io.{File, FileWriter}
import java.util.{Arrays => JArrays}

import org.apache.spark.api.python.PythonException
import org.apache.spark.api.python.PythonUtils
import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.read.LookupParameter
import org.apache.spark.sql.execution.{FilterExec, LimitExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.DataSourceManager
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, ExternalLookupRelation}
import org.apache.spark.sql.execution.datasources.v2.python.{PythonScan, PythonScanBuilder}
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.util.Utils

abstract class PythonDataSourceSuiteBase
    extends SharedSparkSession
    with AdaptiveSparkPlanHelper {

  protected val simpleDataSourceReaderScript: String =
    """
      |from pyspark.sql.datasource import DataSourceReader, InputPartition
      |class SimpleDataSourceReader(DataSourceReader):
      |    def partitions(self):
      |        return [InputPartition(i) for i in range(2)]
      |    def read(self, partition):
      |        yield (0, partition.value)
      |        yield (1, partition.value)
      |        yield (2, partition.value)
      |""".stripMargin
  protected val staticSourceName = "custom_source"
  protected var tempDir: File = _

  override def beforeAll(): Unit = {
    // Create a Python Data Source package before starting up the Spark Session
    // that triggers automatic registration of the Python Data Source.
    val dataSourceScript =
    s"""
       |from pyspark.sql.datasource import DataSource, DataSourceReader
       |$simpleDataSourceReaderScript
       |
       |class DefaultSource(DataSource):
       |    def schema(self) -> str:
       |        return "id INT, partition INT"
       |
       |    def reader(self, schema):
       |        return SimpleDataSourceReader()
       |
       |    @classmethod
       |    def name(cls):
       |        return "$staticSourceName"
       |""".stripMargin
    tempDir = Utils.createTempDir()
    // Write a temporary package to test.
    // tmp/my_source
    // tmp/my_source/__init__.py
    val packageDir = new File(tempDir, "pyspark_mysource")
    assert(packageDir.mkdir())
    Utils.tryWithResource(
      new FileWriter(new File(packageDir, "__init__.py")))(_.write(dataSourceScript))
    // So Spark Session initialization can lookup this temporary directory.
    DataSourceManager.dataSourceBuilders = None
    PythonUtils.additionalTestingPath = Some(tempDir.toString)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
      PythonUtils.additionalTestingPath = None
    } finally {
      super.afterAll()
    }
  }

  setupTestData()

  protected def dataSourceName = "SimpleDataSource"
}

class PythonDataSourceSuite extends PythonDataSourceSuiteBase {
  import IntegratedUDFTestUtils._

  test("SPARK-50426: should not trigger static Python data source lookup") {
    assume(shouldTestPandasUDFs)
    val testAppender = new LogAppender("Python data source lookup")
    // Using builtin and Java data sources should not trigger a static
    // Python data source lookup
    withLogAppender(testAppender) {
      spark.read.format("org.apache.spark.sql.test").load()
      spark.range(3).write.mode("overwrite").format("noop").save()
    }
    assert(!testAppender.loggingEvents
      .exists(msg => msg.getMessage.getFormattedMessage.contains(
        "Loading static Python Data Sources.")))
    // Now trigger a Python data source lookup
    withLogAppender(testAppender) {
      spark.read.format(staticSourceName).load()
    }
    assert(testAppender.loggingEvents
      .exists(msg => msg.getMessage.getFormattedMessage.contains(
        "Loading static Python Data Sources.")))
  }

  test("SPARK-45917: automatic registration of Python Data Source") {
    assume(shouldTestPandasUDFs)
    val df = spark.read.format(staticSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))

    // Overwrite the static source
    val errorText = "static source overwritten"
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |
         |class $staticSourceName(DataSource):
         |    def schema(self) -> str:
         |        raise Exception("$errorText")
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(
      name = staticSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(staticSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(staticSourceName).load()
    }
    assert(err.getMessage.contains(errorText))
  }

  test("simple data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |from pyspark.sql.datasource import DataSource
        |$simpleDataSourceReaderScript
        |
        |class $dataSourceName(DataSource):
        |    def reader(self, schema):
        |        return SimpleDataSourceReader()
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).schema(schema).load()
    assert(df.rdd.getNumPartitions == 2)
    val plan = df.queryExecution.optimizedPlan
    plan match {
      case s: DataSourceV2ScanRelation
        if s.relation.table.getClass.toString.contains("PythonTable") =>
      case _ => fail(s"Plan did not match the expected pattern. Actual plan:\n$plan")
    }
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data source with string schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, partition INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("simple data source with StructType schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |from pyspark.sql.types import IntegerType, StructType, StructField
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return StructType([
         |            StructField("id", IntegerType()),
         |            StructField("partition", IntegerType())
         |        ])
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))
  }

  test("data source with invalid schema") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    checkError(
      exception = intercept[AnalysisException](spark.read.format(dataSourceName).load()),
      condition = "INVALID_SCHEMA.NON_STRUCT_TYPE",
      parameters = Map("inputSchema" -> "INT", "dataType" -> "\"INT\""))
  }

  test("data source reader with filter pushdown") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource,
         |    DataSourceReader,
         |    EqualTo,
         |    InputPartition,
         |)
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [InputPartition(i) for i in range(2)]
         |
         |    def pushFilters(self, filters):
         |        for filter in filters:
         |            if filter != EqualTo(("partition",), 0):
         |                yield filter
         |
         |    def read(self, partition):
         |        yield (0, partition.value)
         |        yield (1, partition.value)
         |        yield (2, partition.value)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int, partition int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df =
        spark.read.format(dataSourceName).schema(schema).load().filter("id = 1 and partition = 0")
      val plan = df.queryExecution.executedPlan

      /**
       * == Physical Plan ==
       * *(1) Project [id#261, partition#262]
       * +- *(1) Filter ((isnotnull(id#261) AND isnotnull(partition#262)) AND (id#261 = 1))
       *    +- BatchScan SimpleDataSource[id#261, partition#262] (Python)
       *       PushedFilters: [EqualTo(partition,0)],
       *       ReadSchema: struct<id:int,partition:int> RuntimeFilters: []
       */
      val filter = collectFirst(df.queryExecution.executedPlan) {
        case s: FilterExec =>
          val condition = s.condition.toString
          assert(!condition.contains("= 0")) // pushed filter is not in FilterExec
          assert(condition.contains("= 1")) // unsupported filter is in FilterExec
          s
      }.getOrElse(
        fail(s"Filter not found in the plan. Actual plan:\n$plan")
      )

      collectFirst(filter) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(p.getMetaData().get("PushedFilters").contains("[EqualTo(partition,0)]"))
      }.getOrElse(
        fail(s"PythonScan not found in the plan. Actual plan:\n$plan")
      )

      checkAnswer(df, Seq(Row(1, 0), Row(1, 1)))
    }
  }

  test("data source reader with limit pushdown") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def __init__(self):
         |        self.limit = None
         |
         |    def pushLimit(self, limit):
         |        self.limit = limit
         |        return True
         |
         |    def partitions(self):
         |        # A pushed limit lets the reader plan a single partition.
         |        assert self.limit == 2, self.limit
         |        return [InputPartition(0)]
         |
         |    def read(self, partition):
         |        for i in range(self.limit):
         |            yield (i,)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df = spark.read.format(dataSourceName).schema(schema).load().limit(2)
      val plan = df.queryExecution.executedPlan

      collectFirst(plan) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(p.getMetaData().get("PushedLimit").contains("LIMIT 2"))
      }.getOrElse(
        fail(s"PythonScan not found in the plan. Actual plan:\n$plan")
      )

      // Spark keeps its own limit: a Python data source is not trusted to honor the pushed limit.
      assert(collectFirst(plan) { case l: LimitExec => l }.isDefined,
        s"A limit operator should be retained. Actual plan:\n$plan")

      checkAnswer(df, Seq(Row(0), Row(1)))
    }
  }

  test("data source reader limit pushdown not supported by the reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def pushLimit(self, limit):
         |        return False
         |
         |    def read(self, partition):
         |        yield (0,)
         |        yield (1,)
         |        yield (2,)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df = spark.read.format(dataSourceName).schema(schema).load().limit(2)
      val plan = df.queryExecution.executedPlan

      collectFirst(plan) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(!p.getMetaData().contains("PushedLimit"))
      }.getOrElse(
        fail(s"PythonScan not found in the plan. Actual plan:\n$plan")
      )

      checkAnswer(df, Seq(Row(0), Row(1)))
    }
  }

  test("limit pushdown is not used for top-N (orderBy then limit)") {
    assume(shouldTestPandasUDFs)
    // A LIMIT after an ORDER BY is a top-N: the limit cannot be pushed to the scan, because
    // pushing it before the sort would drop rows the sort needs. The reader must not see it.
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def pushLimit(self, limit):
         |        raise AssertionError("pushLimit must not be called for a top-N query")
         |
         |    def partitions(self):
         |        return [InputPartition(0)]
         |
         |    def read(self, partition):
         |        yield from [(2,), (1,), (3,)]
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df = spark.read.format(dataSourceName).schema(schema).load().orderBy("id").limit(2)
      val plan = df.queryExecution.executedPlan

      collectFirst(plan) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(!p.getMetaData().contains("PushedLimit"))
      }.getOrElse(
        fail(s"PythonScan not found in the plan. Actual plan:\n$plan")
      )

      checkAnswer(df, Seq(Row(1), Row(2)))
    }
  }

  test("no limit is pushed for a filter-only query with both pushdowns enabled") {
    assume(shouldTestPandasUDFs)
    // The filter-pushdown worker also handles limit pushdown, but a query without a LIMIT sends
    // the -1 no-limit sentinel, so `pushLimit` must not be called even when the reader implements
    // it and limit pushdown is enabled.
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def pushFilters(self, filters):
         |        # Reject the filter so Spark re-applies it; this test is only about the limit.
         |        return filters
         |
         |    def pushLimit(self, limit):
         |        raise AssertionError("pushLimit must not be called when the query has no limit")
         |
         |    def partitions(self):
         |        return [InputPartition(0)]
         |
         |    def read(self, partition):
         |        yield from [(1,), (2,)]
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(
      SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df = spark.read.format(dataSourceName).schema(schema).load().filter("id = 1")
      val plan = df.queryExecution.executedPlan

      collectFirst(plan) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(!p.getMetaData().contains("PushedLimit"))
      }.getOrElse(
        fail(s"PythonScan not found in the plan. Actual plan:\n$plan")
      )

      checkAnswer(df, Seq(Row(1)))
    }
  }

  test("limit pushdown does not leak into a reused base scan") {
    assume(shouldTestPandasUDFs)
    // A base DataFrame and its `.limit(n)` share the same Python data source instance. The read
    // function produced while pushing the limit must stay scoped to the limited scan and not
    // leak into the base scan, which would make the base DataFrame read too few rows.
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def pushLimit(self, limit):
         |        self._limit = limit
         |        return True
         |
         |    def partitions(self):
         |        return [InputPartition(0)]
         |
         |    def read(self, partition):
         |        n = getattr(self, "_limit", 10)
         |        for i in range(n):
         |            yield (i,)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df = spark.read.format(dataSourceName).schema(schema).load()
      checkAnswer(df.limit(2), Seq(Row(0), Row(1)))
      checkAnswer(df, (0 until 10).map(Row(_)))
    }
  }

  test("filter pushdown does not leak into a reused base scan") {
    assume(shouldTestPandasUDFs)
    // Same as above for filter pushdown: the read function bound to the pushed filters must not
    // leak into the base scan, which would make the base DataFrame return only filtered rows.
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def pushFilters(self, filters):
         |        self._filtered = True
         |        return []
         |
         |    def partitions(self):
         |        return [InputPartition(0)]
         |
         |    def read(self, partition):
         |        if getattr(self, "_filtered", False):
         |            yield (1,)
         |        else:
         |            for i in range(10):
         |                yield (i,)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df = spark.read.format(dataSourceName).schema(schema).load()
      checkAnswer(df.filter("id = 1"), Seq(Row(1)))
      checkAnswer(df, (0 until 10).map(Row(_)))
    }
  }

  private def withPythonWorkerReuse(reuseWorker: Boolean)(body: => Unit): Unit = {
    withPythonRunnerConfs("spark.python.worker.reuse" -> reuseWorker.toString)(body)
  }

  private def withPythonRunnerConfs(settings: (String, String)*)(body: => Unit): Unit = {
    val conf = org.apache.spark.SparkEnv.get.conf
    val original = settings.map { case (key, _) => key -> conf.getOption(key) }
    settings.foreach { case (key, value) => conf.set(key, value) }
    try {
      body
    } finally {
      original.foreach {
        case (key, Some(previous)) => conf.set(key, previous)
        case (key, None) => conf.remove(key)
      }
    }
  }

  private def pythonLookupScript(dualCapability: Boolean = false): String = {
    val readerMethod = if (dualCapability) {
      """
        |    def reader(self, schema):
        |        return Reader()
        |""".stripMargin
    } else {
      ""
    }
    s"""
       |import json
       |from pyspark.sql.datasource import DataSource, DataSourceReader, ExternalLookup
       |
       |class Reader(DataSourceReader):
       |    def read(self, partition):
       |        yield (1, "scan-1")
       |        yield (2, "scan-2")
       |
       |class Lookup(ExternalLookup):
       |    def __init__(self, schema, options):
       |        self.fields = schema.fieldNames()
       |        self.options = options
       |
       |    def selectedPredicateIndexes(self):
       |        if self.options.get("change_after_pruning") and len(self.fields) == 1:
       |            return []
       |        return json.loads(self.options.get("selected", "[0]"))
       |
       |    def maxParameterRowsPerRequest(self):
       |        return int(self.options.get("max_rows", "2"))
       |
       |    def lookup(self, batches):
       |        try:
       |            yield from self.lookup_rows(batches)
       |        finally:
       |            path = self.options.get("close_path")
       |            if path:
       |                with open(path, "a") as output:
       |                    output.write("closed\\n")
       |
       |    def lookup_rows(self, batches):
       |        if self.options.get("failure"):
       |            raise RuntimeError("external lookup execution failed")
       |        for batch in batches:
       |            assert batch.num_rows <= 2
       |            assert batch.schema.names[0] == "input_ordinal"
       |            data = batch.to_pydict()
       |            if self.options.get("request_path"):
       |                import os
       |                with open(self.options["request_path"], "a") as output:
       |                    output.write("%s\\n" % os.getpid())
       |            if self.options.get("block_after_first") and data["parameter_0"][0] > 2:
       |                import time
       |                while True:
       |                    time.sleep(1)
       |            rows = list(zip(data["input_ordinal"], data["parameter_0"]))
       |            if self.options.get("ordinal") == "arrow":
       |                rows = list(zip(batch.column(0), batch.column(1)))
       |            elif self.options.get("ordinal") == "arrow64":
       |                import pyarrow as pa
       |                rows = list(zip(batch.column(0).cast(pa.int64()), batch.column(1)))
       |            elif self.options.get("ordinal") == "numpy":
       |                rows = list(zip(batch.column(0).to_numpy(), batch.column(1).to_numpy()))
       |            elif self.options.get("ordinal") == "pandas":
       |                rows = list(batch.to_pandas().to_records(index=False))
       |            for ordinal, value in reversed(rows):
       |                mode = self.options.get("ordinal")
       |                if mode == "null":
       |                    ordinal = None
       |                elif mode == "negative":
       |                    ordinal = -1
       |                elif mode == "too_large":
       |                    ordinal = batch.num_rows
       |                elif mode == "fractional":
       |                    ordinal = 0.5
       |                elif mode == "negative_fractional":
       |                    ordinal = -0.5
       |                values = {"id": value, "label": "lookup-%s" % value}
       |                if self.options.get("block_after_output"):
       |                    values["label"] *= 16384
       |                result = tuple([ordinal] + [values[field] for field in self.fields])
       |                yield result
       |                if self.options.get("block_after_output"):
       |                    import time
       |                    with open(self.options["block_path"], "w") as output:
       |                        output.write("blocked")
       |                    while True:
       |                        time.sleep(1)
       |                if self.options.get("unbounded"):
       |                    while True:
       |                        yield result
       |
       |class $dataSourceName(DataSource):
       |    def externalLookup(self, schema, predicates):
       |        if self.options.get("none_after_pruning") and len(schema) == 1:
       |            return None
       |        return Lookup(schema, self.options)
       |$readerMethod
       |""".stripMargin
  }

  test("external lookup Python dual-capability source requires a hint") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript(true))
    spark.dataSource.registerPython(dataSourceName, source)
    val left = spark.range(1, 3).selectExpr("cast(id as int) as id").as("left")
    val lookup = spark.read.format(dataSourceName)
      .schema("id INT, label STRING").load().as("lookup")

    val ordinary = left.join(lookup, col("left.id") === col("lookup.id"))
      .select(col("lookup.label"))
    checkAnswer(ordinary, Seq(Row("scan-1"), Row("scan-2")))
    assert(collectFirst(ordinary.queryExecution.executedPlan) {
      case exec: ExternalLookupJoinExec => exec
    }.isEmpty)

    val hinted = left.join(lookup.hint("EXTERNAL_LOOKUP"),
      col("left.id") === col("lookup.id")).select(col("lookup.label"))
    checkAnswer(hinted, Seq(Row("lookup-1"), Row("lookup-2")))
    assert(collectFirst(hinted.queryExecution.executedPlan) {
      case exec: ExternalLookupJoinExec => exec
    }.isDefined)
  }

  gridTest("Python data source ordinary reads preserve pipelined execution")(
      Seq(true, false)) { pipelined =>
    assume(shouldTestPandasUDFs)
    val script =
      s"""
         |import os
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |
         |class Reader(DataSourceReader):
         |    def read(self, partition):
         |        yield (os.environ.get("SPARK_PIPELINED_UDF") == "1",)
         |
         |class $dataSourceName(DataSource):
         |    def reader(self, schema):
         |        return Reader()
         |""".stripMargin
    val source = createUserDefinedPythonDataSource(dataSourceName, script)
    spark.dataSource.registerPython(dataSourceName, source)
    withPythonRunnerConfs("spark.python.udf.pipelined.enabled" -> pipelined.toString) {
      val result = spark.read.format(dataSourceName).schema("pipelined BOOLEAN").load()
      checkAnswer(result, Row(pipelined))
    }
  }

  test("external lookup Python literal filters remain residual with read pushdown enabled") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    withSQLConf(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true",
        SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      val left = spark.range(1, 8).selectExpr("cast(id as int) as id").as("left")
      val lookup = spark.read.format(dataSourceName).schema("id INT, label STRING")
        .load().filter("label = 'lookup-1'").as("lookup")
      val joined = left.join(lookup, col("left.id") === col("lookup.id"))
        .select(col("lookup.label"))
      checkAnswer(joined, Seq(Row("lookup-1")))
      assert(collectFirst(joined.queryExecution.executedPlan) {
        case exec: ExternalLookupJoinExec => exec
      }.isDefined)
    }
  }

  test("external lookup Python dual-capability source preserves ordinary limit pushdown") {
    assume(shouldTestPandasUDFs)
    val script = pythonLookupScript(true) +
      """
        |class Reader(DataSourceReader):
        |    def __init__(self):
        |        self.limit = None
        |
        |    def pushLimit(self, limit):
        |        self.limit = limit
        |        return True
        |
        |    def partitions(self):
        |        assert self.limit == 1, self.limit
        |        return super().partitions()
        |
        |    def read(self, partition):
        |        yield (1, "scan-1")
        |        yield (2, "scan-2")
        |""".stripMargin
    val source = createUserDefinedPythonDataSource(dataSourceName, script)
    spark.dataSource.registerPython(dataSourceName, source)
    withSQLConf(SQLConf.PYTHON_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      val ordinary = spark.read.format(dataSourceName).schema("id INT, label STRING").load()
        .limit(1)
      val scan = collectFirst(ordinary.queryExecution.executedPlan) {
        case exec: BatchScanExec => exec.scan.asInstanceOf[PythonScan]
      }.get
      assert(scan.getMetaData().get("PushedLimit").contains("LIMIT 1"))
      checkAnswer(ordinary, Seq(Row(1, "scan-1")))
    }
  }

  test("external lookup Python negotiation returns original predicate occurrences in order") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    val frame = spark.read.format(dataSourceName).option("selected", "[2,0]")
      .schema("id INT, label STRING").load()
    val relation = frame.queryExecution.analyzed.collectFirst {
      case relation: ExternalLookupRelation => relation.relation
    }.get
    val builder = relation.table.asInstanceOf[SupportsRead]
      .newScanBuilder(relation.options).asInstanceOf[PythonScanBuilder]
    val candidates = Array.fill(3) {
      new V2Predicate("=", Array[V2Expression](
        FieldReference.column("id"), new LookupParameter(IntegerType)))
    }
    val selected = builder.selectLookupPredicates(candidates)
    assert(selected.length == 2)
    assert(selected(0) eq candidates(2))
    assert(selected(1) eq candidates(0))
    builder.pruneColumns(StructType.fromDDL("label STRING"))
    val scan = builder.build()
    assert(scan.readSchema().fieldNames.toSeq == Seq("label"))
    assert(scan.toExternalLookup().maxParameterRowsPerRequest() == 2)
  }

  test("external lookup Python cleans up each batched request") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      withTempDir { directory =>
        val marker = new File(directory, "closed")
        val left = spark.range(1, 8).selectExpr("cast(id as int) as id").coalesce(1).as("left")
        val lookup = spark.read.format(dataSourceName).option("close_path", marker.toString)
          .schema("id INT, label STRING").load().as("lookup")
        val joined = left.join(lookup, col("left.id") === col("lookup.id"))
          .select(col("lookup.label"))
        assert(joined.collect().toSeq.sortBy(_.getString(0)) ==
          (1 to 7).map(value => Row(s"lookup-$value")))
        assert(java.nio.file.Files.readAllLines(marker.toPath).size() == 4)
      }
    }
  }

  gridTest("external lookup Python request resources stay bounded")(
      Seq(true, false)) { reuseWorker =>
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    withPythonWorkerReuse(reuseWorker) {
      withTempDir { directory =>
        val marker = new File(directory, "requests")
        val left = spark.range(1, 2).selectExpr("cast(id as int) as id").as("left")
        val lookup = spark.read.format(dataSourceName).option("request_path", marker.toString)
          .schema("id INT, label STRING").load().as("lookup")
        val joined = left.join(lookup, col("left.id") === col("lookup.id"))
        val factory = collectFirst(joined.queryExecution.executedPlan) {
          case exec: ExternalLookupJoinExec => exec.readerFactory
        }.get
        val requestCount = 24
        val measurements = org.apache.spark.sql.execution.SQLExecution
          .withSQLConfPropagated(spark) {
            spark.sparkContext.parallelize(Seq(1), 1).mapPartitions { _ =>
              val context = org.apache.spark.TaskContext.get()
              val field = context.getClass.getDeclaredFields
                .find(_.getName.endsWith("onCompleteCallbacks")).get
              field.setAccessible(true)
              def listeners: Int = field.get(context).asInstanceOf[java.util.Stack[_]].size()
              def monitors: Set[Long] = Thread.getAllStackTraces.keySet()
                .toArray(new Array[Thread](0)).iterator
                .filter(_.getName.startsWith("Worker Monitor for ")).map(_.getId).toSet
              val reader = factory.createReader()
              try {
                val listenersBefore = listeners
                val monitorsBefore = monitors
                (1 to requestCount).foreach { value =>
                  val cursor = reader.lookupBatch(Array(InternalRow(value)))
                  assert(cursor.next())
                  assert(cursor.inputOrdinal() == 0)
                  assert(cursor.get().getInt(0) == value)
                  assert(!cursor.next())
                  cursor.close()
                }
                Iterator.single((listeners - listenersBefore, (monitors -- monitorsBefore).size))
              } finally {
                reader.close()
              }
            }.collect().head
          }
        val processes = java.nio.file.Files.readAllLines(marker.toPath)
        assert(processes.size() == requestCount)
        val workerCount = processes.stream().distinct().count()
        if (reuseWorker) {
          assert(workerCount < requestCount)
        } else {
          assert(workerCount == requestCount)
        }
        info(s"$requestCount completed requests retained ${measurements._1} task listeners " +
          s"and ${measurements._2} worker monitors (reuse=$reuseWorker, workers=$workerCount)")
        assert(measurements._1 <= 1 && measurements._2 <= 1,
          s"$requestCount completed requests retained ${measurements._1} task listeners " +
            s"and ${measurements._2} worker monitors (reuse=$reuseWorker)")
      }
    }
  }

  gridTest("external lookup Python cancellation stops a blocked second request")(
      Seq(true, false)) { reuseWorker =>
    import scala.concurrent.{ExecutionContext, Future}
    import scala.concurrent.duration._

    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    withPythonWorkerReuse(reuseWorker) {
      withTempDir { directory =>
        val marker = new File(directory, "requests")
        val group = java.util.UUID.randomUUID().toString
        val left = spark.range(1, 8).selectExpr("cast(id as int) as id").coalesce(1).as("left")
        val lookup = spark.read.format(dataSourceName)
          .option("block_after_first", "true").option("request_path", marker.toString)
          .schema("id INT, label STRING").load().as("lookup")
        val joined = left.join(lookup, col("left.id") === col("lookup.id"))
          .select(col("lookup.label"))
        val result = Future {
          spark.sparkContext.setJobGroup(group, "cancel Python external lookup",
            interruptOnCancel = false)
          try {
            joined.collect()
          } finally {
            spark.sparkContext.clearJobGroup()
          }
        }(ExecutionContext.global)
        var worker: Option[ProcessHandle] = None
        try {
          eventually(timeout(30.seconds), interval(100.milliseconds)) {
            assert(marker.exists())
            val requests = java.nio.file.Files.readAllLines(marker.toPath)
            assert(requests.size() >= 2)
            assert((requests.get(0) == requests.get(1)) == reuseWorker)
            worker = Some(ProcessHandle.of(requests.get(1).toLong).orElseThrow())
          }
          spark.sparkContext.cancelJobGroup(group)
          intercept[Exception] {
            org.apache.spark.util.ThreadUtils.awaitResult(result, 15.seconds)
          }
          eventually(timeout(15.seconds), interval(100.milliseconds)) {
            assert(!worker.get.isAlive)
          }
        } finally {
          spark.sparkContext.cancelJobGroup(group)
          worker.filter(_.isAlive).foreach(_.destroyForcibly())
        }
      }
    }
  }

  test("external lookup Python successful limit stops a blocked worker") {
    import scala.concurrent.{ExecutionContext, Future}
    import scala.concurrent.duration._

    assume(shouldTestPandasUDFs)
    assert(spark.sparkContext.getConf.getBoolean("spark.python.worker.reuse", true))
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      withTempDir { directory =>
        val marker = new File(directory, "requests")
        val blocked = new File(directory, "blocked")
        val group = java.util.UUID.randomUUID().toString
        val left = spark.range(1, 2).selectExpr("cast(id as int) as id").coalesce(1).as("left")
        val lookup = spark.read.format(dataSourceName)
          .option("block_after_output", "true").option("request_path", marker.toString)
          .option("block_path", blocked.toString)
          .schema("id INT, label STRING").load().as("lookup")
        val joined = left.join(lookup, col("left.id") === col("lookup.id"))
          .select(col("lookup.label"))
        val result = Future {
          spark.sparkContext.setJobGroup(group, "limit Python external lookup",
            interruptOnCancel = false)
          try {
            joined.limit(1).rdd.mapPartitions { rows =>
              val first = rows.next()
              val deadline = 30.seconds.fromNow
              while (!blocked.exists() && deadline.hasTimeLeft()) {
                Thread.sleep(10)
              }
              require(blocked.exists(), "Python lookup did not block after flushing its result")
              Iterator.single(first)
            }.collect()
          } finally {
            spark.sparkContext.clearJobGroup()
          }
        }(ExecutionContext.global)
        var worker: Option[ProcessHandle] = None
        try {
          eventually(timeout(30.seconds), interval(100.milliseconds)) {
            assert(marker.exists())
            val processId = java.nio.file.Files.readAllLines(marker.toPath).get(0).toLong
            worker = Option(ProcessHandle.of(processId).orElse(null))
            assert(blocked.exists())
          }
          assert(org.apache.spark.util.ThreadUtils.awaitResult(result, 30.seconds).toSeq ==
            Seq(Row("lookup-1" * 16384)))
          eventually(timeout(15.seconds), interval(100.milliseconds)) {
            assert(worker.forall(process => !process.isAlive))
          }
        } finally {
          spark.sparkContext.cancelJobGroup(group)
          worker.filter(_.isAlive).foreach(_.destroyForcibly())
        }
      }
    }
  }

  test("external lookup Python honors unlimited Arrow batch sizes") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    Seq("0", "-1").foreach { batchSize =>
      withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> batchSize) {
        val left = spark.range(1, 8).selectExpr("cast(id as int) as id").coalesce(1).as("left")
        val lookup = spark.read.format(dataSourceName)
          .schema("id INT, label STRING").load().as("lookup")
        val joined = left.join(lookup, col("left.id") === col("lookup.id"))
          .select(col("lookup.label"))
        checkAnswer(joined, (1 to 7).map(value => Row(s"lookup-$value")))
      }
    }
  }

  test("external lookup Python early cursor close stops an unbounded worker") {
    import scala.concurrent.duration._

    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      withTempDir { directory =>
        val marker = new File(directory, "requests")
        val left = spark.range(1, 2).selectExpr("cast(id as int) as id").as("left")
        val lookup = spark.read.format(dataSourceName)
          .option("unbounded", "true").option("request_path", marker.toString)
          .schema("id INT, label STRING").load().as("lookup")
        val joined = left.join(lookup, col("left.id") === col("lookup.id"))
          .select(col("lookup.label"))
        val factory = collectFirst(joined.queryExecution.executedPlan) {
          case exec: ExternalLookupJoinExec => exec.readerFactory
        }.get
        org.apache.spark.sql.execution.SQLExecution.withSQLConfPropagated(spark) {
          val result = spark.sparkContext.parallelize(Seq(1), 1).mapPartitions { _ =>
            val reader = factory.createReader()
            try {
              val cursor = reader.lookupBatch(Array(InternalRow(1)))
              assert(cursor.next())
              cursor.close()
              cursor.close()
              reader.close()
              Iterator.single(1)
            } finally {
              reader.close()
            }
          }.collect()
          assert(result.toSeq == Seq(1))
        }
        val processId = java.nio.file.Files.readAllLines(marker.toPath).get(0).toLong
        eventually(timeout(15.seconds), interval(100.milliseconds)) {
          val process = ProcessHandle.of(processId)
          assert(process.isEmpty || !process.get().isAlive)
        }
      }
    }
  }

  gridTest("external lookup Python early close releases partially serialized input")(
      for (reuse <- Seq(true, false); pipeline <- Seq(true, false))
        yield (reuse, pipeline)) { case (reuseWorker, pipelined) =>
    assume(shouldTestPandasUDFs)
    val script =
      s"""
         |from pyspark.sql.datasource import DataSource, ExternalLookup
         |
         |class Lookup(ExternalLookup):
         |    def __init__(self, schema):
         |        self.fields = schema.fieldNames()
         |
         |    def selectedPredicateIndexes(self):
         |        return [0]
         |
         |    def lookup(self, batches):
         |        batch = next(batches)
         |        key = batch.column(1)[0].as_py()
         |        values = {"key": key, "label": "normal" if key == "normal" else "x" * 131072}
         |        result = tuple([0] + [values[field] for field in self.fields])
         |        yield result
         |        if key != "normal":
         |            while True:
         |                yield result
         |
         |class $dataSourceName(DataSource):
         |    def externalLookup(self, schema, predicates):
         |        return Lookup(schema)
         |""".stripMargin
    val source = createUserDefinedPythonDataSource(dataSourceName, script)
    spark.dataSource.registerPython(dataSourceName, source)
    withPythonRunnerConfs(
        "spark.python.worker.reuse" -> reuseWorker.toString,
        "spark.python.udf.pipelined.enabled" -> pipelined.toString) {
      withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
        val left = spark.range(1).selectExpr("cast(id as string) as key").as("left")
        val lookup = spark.read.format(dataSourceName).schema("key STRING, label STRING")
          .load().as("lookup")
        val joined = left.join(lookup, col("left.key") === col("lookup.key"))
          .select(col("lookup.label"))
        val factory = collectFirst(joined.queryExecution.executedPlan) {
          case exec: ExternalLookupJoinExec => exec.readerFactory
        }.get
        org.apache.spark.sql.execution.SQLExecution.withSQLConfPropagated(spark) {
          val result = spark.sparkContext.parallelize(Seq(1), 1).mapPartitions { _ =>
            val allocator = org.apache.spark.sql.util.ArrowUtils.rootAllocator
            val baseline = allocator.getAllocatedMemory
            val reader = factory.createReader()
            try {
              (1 to 3).foreach { _ =>
                val taskThread = Thread.currentThread()
                val serializedOnTaskThread = new java.util.concurrent.atomic.AtomicBoolean(false)
                val writerActive = new java.util.concurrent.atomic.AtomicBoolean(false)
                val writerEntered = new java.util.concurrent.CountDownLatch(1)
                val writerRelease = new java.util.concurrent.CountDownLatch(1)
                val writerExited = new java.util.concurrent.CountDownLatch(1)
                val lastKeyRead = new java.util.concurrent.atomic.AtomicBoolean(false)
                val value = org.apache.spark.unsafe.types.UTF8String.fromString("x" * 1048576)
                val keys = Array.tabulate[InternalRow](64) { index =>
                  new GenericInternalRow(Array[Any](value)) {
                    override def get(ordinal: Int, dataType: DataType): AnyRef = {
                      if (Thread.currentThread() == taskThread) {
                        serializedOnTaskThread.set(true)
                      } else if (index == 1) {
                        writerActive.set(true)
                        writerEntered.countDown()
                        try {
                          var released = false
                          while (!released) {
                            try {
                              writerRelease.await()
                              released = true
                            } catch {
                              case _: InterruptedException =>
                            }
                          }
                        } finally {
                          writerActive.set(false)
                          writerExited.countDown()
                        }
                        throw new IllegalStateException("Serialization released after early close")
                      }
                      if (index == 63) lastKeyRead.set(true)
                      super.get(ordinal, dataType)
                    }
                  }
                }
                val cursor = reader.lookupBatch(keys)
                try {
                  assert(cursor.next())
                  assert(cursor.get().getUTF8String(0).numBytes() == 131072)
                  assert(!lastKeyRead.get(), "Lookup serialized all input before early close")
                  if (!serializedOnTaskThread.get()) {
                    assert(writerEntered.await(10, java.util.concurrent.TimeUnit.SECONDS))
                  }
                  cursor.close()
                  cursor.close()
                  assert(!writerActive.get(), "Cleanup overtook an active input writer")
                  assert(allocator.getAllocatedMemory == baseline,
                    s"Early close retained ${allocator.getAllocatedMemory - baseline} Arrow bytes")
                } finally {
                  writerRelease.countDown()
                  if (writerEntered.getCount == 0) {
                    assert(writerExited.await(10, java.util.concurrent.TimeUnit.SECONDS))
                  }
                }
              }
              val cursor = reader.lookupBatch(Array(InternalRow(
                org.apache.spark.unsafe.types.UTF8String.fromString("normal"))))
              assert(cursor.next())
              assert(cursor.get().getUTF8String(0).toString == "normal")
              assert(!cursor.next())
              Iterator.single(1)
            } finally {
              reader.close()
            }
          }.collect()
          assert(result.toSeq == Seq(1))
        }
      }
    }
  }

  test("external lookup Python rejects invalid selections without read fallback") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript(true))
    spark.dataSource.registerPython(dataSourceName, source)
    val left = spark.range(1, 3).selectExpr("cast(id as int) as id").as("left")
    Seq("null", "[0,0]", "[-1]", "[1]", "[null]", "[true]").foreach { selected =>
      val lookup = spark.read.format(dataSourceName).option("selected", selected)
        .schema("id INT, label STRING").load().hint("EXTERNAL_LOOKUP").as("lookup")
      val joined = left.join(lookup, col("left.id") === col("lookup.id"))
      val error = intercept[Exception] { joined.queryExecution.optimizedPlan }
      assert(error.getMessage.contains("DATA_SOURCE_TYPE_MISMATCH"), error.getMessage)
    }
  }

  test("external lookup Python rejects access path changes after pruning") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    val left = spark.range(1, 3).selectExpr("cast(id as int) as id").as("left")
    Seq("change_after_pruning", "none_after_pruning").foreach { option =>
      val lookup = spark.read.format(dataSourceName).option(option, "true")
        .schema("id INT, label STRING").load().as("lookup")
      val joined = left.join(lookup, col("left.id") === col("lookup.id"))
        .select(col("lookup.label"))
      val error = intercept[AnalysisException] { joined.queryExecution.executedPlan }
      val message = if (option == "change_after_pruning") {
        "Python data source returned different external lookup predicates after pruning"
      } else {
        "Python data source did not return an external lookup"
      }
      checkError(
        exception = error,
        condition = "PYTHON_DATA_SOURCE_ERROR",
        parameters = Map("action" -> "plan", "type" -> "external lookup", "msg" -> message))
    }
  }

  gridTest("external lookup Python preserves integer column ordinals")(
      Seq("arrow", "arrow64", "numpy", "pandas")) { mode =>
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    val left = spark.range(1, 6, 1, 1).selectExpr("cast(id as int) as id").as("left")
    val lookup = spark.read.format(dataSourceName).option("ordinal", mode)
      .schema("id INT, label STRING").load().as("lookup")
    val joined = left.join(lookup, col("left.id") === col("lookup.id"))
      .select(col("left.id"), col("lookup.label"))
    checkAnswer(joined, (1 until 6).map(value => Row(value, s"lookup-$value")))
  }

  test("external lookup Python rejects invalid result ordinals and propagates execution errors") {
    assume(shouldTestPandasUDFs)
    val source = createUserDefinedPythonDataSource(dataSourceName, pythonLookupScript())
    spark.dataSource.registerPython(dataSourceName, source)
    val left = spark.range(1, 3).selectExpr("cast(id as int) as id").as("left")
    val modes = Seq("negative", "too_large", "null", "fractional", "negative_fractional", "failure")
    modes.foreach { mode =>
      val option = if (mode == "failure") "failure" else "ordinal"
      val lookup = spark.read.format(dataSourceName).option(option, mode)
        .schema("id INT, label STRING").load().as("lookup")
      val joined = left.join(lookup, col("left.id") === col("lookup.id"))
        .select(col("lookup.label"))
      val error = intercept[Exception] { joined.collect() }
      val message = Option(error.getCause).map(_.getMessage).getOrElse(error.getMessage)
      assert(message.contains("ordinal") || message.contains("external lookup execution failed"),
        message)
    }
  }

  test("external lookup with Python data source") {
    assume(shouldTestPandasUDFs)

    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, EqualTo, ExternalLookup, LookupParameter
         |
         |class CompositeLookup(ExternalLookup):
         |    def __init__(self, schema, selected):
         |        self.output_fields = schema.fieldNames()
         |        self.selected = selected
         |        self.values = {
         |            ("a", 1): ["one", "uno"],
         |            ("b", 2): ["two"],
         |        }
         |
         |    def selectedPredicateIndexes(self):
         |        return self.selected
         |
         |    def maxParameterRowsPerRequest(self):
         |        return 2
         |
         |    def lookup(self, batches):
         |        for batch in batches:
         |            data = batch.to_pydict()
         |            rows = zip(
         |                data["input_ordinal"], data["parameter_0"], data["parameter_1"]
         |            )
         |            for input_ordinal, group, item_id in rows:
         |                for label in self.values.get((group, item_id), []):
         |                    row = [input_ordinal]
         |                    for field in self.output_fields:
         |                        if field == "id":
         |                            row.append(item_id)
         |                        elif field == "label":
         |                            row.append(label)
         |                        elif field == "group":
         |                            row.append(group)
         |                    yield tuple(row)
         |
         |class $dataSourceName(DataSource):
         |    def externalLookup(self, schema, predicates):
         |        fields = [
         |            predicate.attribute
         |            if isinstance(predicate, EqualTo)
         |            and isinstance(predicate.value, LookupParameter)
         |            else None
         |            for predicate in predicates
         |        ]
         |        if ("id",) in fields and ("group",) in fields:
         |            return CompositeLookup(
         |                schema,
         |                [fields.index(("group",)), fields.index(("id",))]
         |            )
         |        return None
         |""".stripMargin
    val schema = StructType.fromDDL("id INT, label STRING, group STRING")
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)

    val leftSchema = StructType.fromDDL("id INT, `group` STRING")
    val left = spark.createDataFrame(
      JArrays.asList(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(1, "a")),
      leftSchema).as("left")
    val lookup = spark.read.format(dataSourceName).schema(schema).load().as("lookup")
    val joined = left.join(
      lookup,
      col("left.id") === col("lookup.id") && col("left.group") === col("lookup.group"),
      "left_outer")
      .select(col("left.id"), col("left.group"), col("lookup.label"))

    checkAnswer(
      joined,
      Seq(Row(1, "a", "one"), Row(1, "a", "uno"), Row(2, "b", "two"),
        Row(3, "c", null), Row(1, "a", "one"), Row(1, "a", "uno")))
    assert(joined.count() == 6)

    val lookupJoin = collectFirst(joined.queryExecution.executedPlan) {
      case exec: ExternalLookupJoinExec => exec
    }.getOrElse(fail(s"ExternalLookupJoinExec not found:\n${joined.queryExecution.executedPlan}"))
    assert(lookupJoin.maxParameterRowsPerRequest == 2)
    assert(lookupJoin.lookupOutput.map(_.name) == Seq("label"))

    val unsupported = left.join(
      lookup, col("left.id") === col("lookup.id"), "inner")
    val error = intercept[AnalysisException] {
      unsupported.queryExecution.optimizedPlan
    }
    assert(error.getCondition == "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE")
  }

  test("external lookup with Python data source supports null-safe keys") {
    assume(shouldTestPandasUDFs)

    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource, EqualNullSafe, ExternalLookup, LookupParameter
         |)
         |
         |class NullSafeLookup(ExternalLookup):
         |    def __init__(self, schema, selected):
         |        self.output_fields = schema.fieldNames()
         |        self.selected = selected
         |
         |    def selectedPredicateIndexes(self):
         |        return self.selected
         |
         |    def lookup(self, batches):
         |        for batch in batches:
         |            data = batch.to_pydict()
         |            for input_ordinal, item_id in zip(
         |                data["input_ordinal"], data["parameter_0"]
         |            ):
         |                if item_id is None:
         |                    label = "null-key"
         |                elif item_id == 1:
         |                    label = "one"
         |                else:
         |                    continue
         |                row = [input_ordinal]
         |                for field in self.output_fields:
         |                    if field == "id":
         |                        row.append(item_id)
         |                    elif field == "label":
         |                        row.append(label)
         |                yield tuple(row)
         |
         |class $dataSourceName(DataSource):
         |    def externalLookup(self, schema, predicates):
         |        if len(predicates) == 1:
         |            predicate = predicates[0]
         |            if (
         |                isinstance(predicate, EqualNullSafe)
         |                and predicate.attribute == ("id",)
         |                and isinstance(predicate.value, LookupParameter)
         |            ):
         |                return NullSafeLookup(schema, [0])
         |        return None
         |""".stripMargin
    val schema = StructType.fromDDL("id INT, label STRING")
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)

    val left = spark.createDataFrame(
      JArrays.asList(Row(null), Row(1), Row(2)),
      StructType.fromDDL("id INT")).as("left")
    val lookup = spark.read.format(dataSourceName).schema(schema).load().as("lookup")
    val joined = left.join(
      lookup,
      col("left.id") <=> col("lookup.id"),
      "inner")
      .select(col("left.id"), col("lookup.label"))

    checkAnswer(joined, Seq(Row(null, "null-key"), Row(1, "one")))

    val unsupported = left.join(
      lookup, col("left.id") === col("lookup.id"), "inner")
    val error = intercept[AnalysisException] {
      unsupported.queryExecution.optimizedPlan
    }
    assert(error.getCondition == "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE")
  }

  test("external lookup with Python data source supports mixed lookup predicates") {
    assume(shouldTestPandasUDFs)

    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource,
         |    EqualTo,
         |    ExternalLookup,
         |    GreaterThanOrEqual,
         |    LessThan,
         |    LookupParameter,
         |)
         |
         |class TemporalLookup(ExternalLookup):
         |    def __init__(self, schema, selected):
         |        self.output_fields = schema.fieldNames()
         |        self.selected = selected
         |        self.values = [
         |            (1, 12, "a"),
         |            (1, 18, "b"),
         |            (1, 22, "b"),
         |            (2, 5, "c"),
         |        ]
         |
         |    def selectedPredicateIndexes(self):
         |        return self.selected
         |
         |    def lookup(self, batches):
         |        for batch in batches:
         |            data = batch.to_pydict()
         |            rows = zip(
         |                data["input_ordinal"],
         |                data["parameter_0"],
         |                data["parameter_1"],
         |                data["parameter_2"],
         |            )
         |            for input_ordinal, tenant, start, end in rows:
         |                for value_tenant, event_time, label in self.values:
         |                    if value_tenant != tenant or not start <= event_time < end:
         |                        continue
         |                    row = [input_ordinal]
         |                    for field in self.output_fields:
         |                        if field == "tenant":
         |                            row.append(value_tenant)
         |                        elif field == "event_time":
         |                            row.append(event_time)
         |                        elif field == "label":
         |                            row.append(label)
         |                    yield tuple(row)
         |
         |class $dataSourceName(DataSource):
         |    def externalLookup(self, schema, predicates):
         |        expected = [
         |            (EqualTo, ("tenant",)),
         |            (GreaterThanOrEqual, ("event_time",)),
         |            (LessThan, ("event_time",)),
         |        ]
         |        selected = []
         |        for predicate_type, field in expected:
         |            matches = [
         |                index
         |                for index, predicate in enumerate(predicates)
         |                if isinstance(predicate, predicate_type)
         |                and predicate.attribute == field
         |                and isinstance(predicate.value, LookupParameter)
         |            ]
         |            if len(matches) != 1:
         |                return None
         |            selected.append(matches[0])
         |        return TemporalLookup(schema, selected)
         |""".stripMargin
    val schema = StructType.fromDDL("tenant INT, event_time INT, label STRING")
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)

    val leftSchema = StructType.fromDDL(
      "tenant INT, start INT, end INT, expected STRING")
    val left = spark.createDataFrame(
      JArrays.asList(
        Row(1, 10, 20, "a"),
        Row(1, 15, 25, "b"),
        Row(2, 0, 10, "c"),
        Row(1, 10, 20, "missing")),
      leftSchema).as("left")
    val lookup = spark.read.format(dataSourceName).schema(schema).load().as("lookup")
    val condition = col("left.tenant") === col("lookup.tenant") &&
      col("lookup.event_time") >= col("left.start") &&
      col("lookup.event_time") < col("left.end") &&
      col("left.expected") === col("lookup.label")
    val joined = left.join(lookup, condition, "inner")
      .select(
        col("left.tenant"),
        col("left.start"),
        col("left.end"),
        col("left.expected"),
        col("lookup.event_time"),
        col("lookup.label"))

    checkAnswer(
      joined,
      Seq(
        Row(1, 10, 20, "a", 12, "a"),
        Row(1, 15, 25, "b", 18, "b"),
        Row(1, 15, 25, "b", 22, "b"),
        Row(2, 0, 10, "c", 5, "c")))
  }

  test("SQLite-backed Python data source implements external lookup") {
    assume(shouldTestPandasUDFs)

    withTempDir { directory =>
      val database = new File(directory, "lookup.db")
      val requests = new File(directory, "requests")
      val dataSourceScript =
        s"""
           |import sqlite3
           |
           |from pyspark.sql.datasource import (
           |    DataSource, EqualTo, ExternalLookup, GreaterThanOrEqual, LookupParameter
           |)
           |
           |class SqliteLookup(ExternalLookup):
           |    def __init__(self, options, schema, selected):
           |        self.options = options
           |        self.output_fields = schema.fieldNames()
           |        self.selected = selected
           |
           |    def selectedPredicateIndexes(self):
           |        return self.selected
           |
           |    def maxParameterRowsPerRequest(self):
           |        return 2
           |
           |    def lookup(self, batches):
           |        conn = sqlite3.connect(self.options["path"])
           |        try:
           |            for batch in batches:
           |                data = batch.to_pydict()
           |                parameters = list(zip(
           |                    data["input_ordinal"], data["parameter_0"], data["parameter_1"]
           |                ))
           |                with open(self.options["requests"], "a") as output:
           |                    output.write(str(len(parameters)) + "\\n")
           |                placeholders = ", ".join(["(?, ?, ?)"] * len(parameters))
           |                columns = ", ".join(f'e."{field}"' for field in self.output_fields)
           |                query = (
           |                    "WITH parameters(input_ordinal, id, lower_bound) AS "
           |                    f"(VALUES {placeholders}) "
           |                    f"SELECT p.input_ordinal, {columns} "
           |                    "FROM parameters p JOIN enrichments e "
           |                    "ON e.id = p.id AND e.score >= p.lower_bound"
           |                )
           |                values = [value for row in parameters for value in row]
           |                yield from conn.execute(query, values)
           |        finally:
           |            conn.close()
           |
           |class $dataSourceName(DataSource):
           |    def schema(self):
           |        return "id INT, score INT, label STRING"
           |
           |    def externalLookup(self, schema, predicates):
           |        selected = []
           |        for predicate_type, field in [
           |            (EqualTo, ("id",)), (GreaterThanOrEqual, ("score",))
           |        ]:
           |            matches = [
           |                index for index, predicate in enumerate(predicates)
           |                if isinstance(predicate, predicate_type)
           |                and predicate.attribute == field
           |                and isinstance(predicate.value, LookupParameter)
           |            ]
           |            if len(matches) != 1:
           |                return None
           |            selected.append(matches[0])
           |        conn = sqlite3.connect(self.options["path"])
           |        try:
           |            conn.execute(
           |                "CREATE TABLE IF NOT EXISTS enrichments "
           |                "(id INTEGER, score INTEGER, label TEXT, PRIMARY KEY (id, score))"
           |            )
           |            conn.executemany(
           |                "INSERT OR IGNORE INTO enrichments VALUES (?, ?, ?)",
           |                [(1, 12, "one"), (1, 18, "uno"), (2, 5, "two")]
           |            )
           |            conn.commit()
           |        finally:
           |            conn.close()
           |        return SqliteLookup(self.options, schema, selected)
           |""".stripMargin
      val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
      spark.dataSource.registerPython(dataSourceName, dataSource)

      val leftSchema = StructType.fromDDL("id INT, lower_bound INT, rejected STRING")
      val left = spark.createDataFrame(
        JArrays.asList(
          Row(1, 12, "missing"), Row(1, 15, "missing"), Row(2, 0, "missing"),
          Row(3, 0, "missing"), Row(1, 12, "uno"), Row(1, 12, "missing"),
          Row(2, 0, "two"), Row(1, 20, "missing"),
          Row(null, 0, "missing"), Row(1, null, "missing")),
        leftSchema).coalesce(1).as("left")
      val lookup = spark.read.format(dataSourceName)
        .option("path", database.toString).option("requests", requests.toString)
        .load().hint("external_lookup", "cache_enabled=false").as("lookup")
      val condition = col("left.id") === col("lookup.id") &&
        col("lookup.score") >= col("left.lower_bound") &&
        col("lookup.label") =!= col("left.rejected")
      val matched = Seq(
        Row(1, 12, "missing", "one"), Row(1, 12, "missing", "uno"),
        Row(1, 15, "missing", "uno"), Row(2, 0, "missing", "two"),
        Row(1, 12, "uno", "one"),
        Row(1, 12, "missing", "one"), Row(1, 12, "missing", "uno"))
      val unmatched = Seq(
        Row(3, 0, "missing", null), Row(2, 0, "two", null), Row(1, 20, "missing", null),
        Row(null, 0, "missing", null), Row(1, null, "missing", null))

      Seq("inner", "left_outer").foreach { joinType =>
        val joined = left.join(lookup, condition, joinType)
          .select(col("left.id"), col("left.lower_bound"), col("left.rejected"),
            col("lookup.label"))
        checkAnswer(joined, matched ++ (if (joinType == "left_outer") unmatched else Nil))

        val lookupJoin = collectFirst(joined.queryExecution.executedPlan) {
          case exec: ExternalLookupJoinExec => exec
        }.getOrElse(
          fail(s"ExternalLookupJoinExec not found:\n${joined.queryExecution.executedPlan}"))
        assert(lookupJoin.maxParameterRowsPerRequest == 2)
        assert(lookupJoin.lookupOutput.map(_.name) == Seq("label"))
        assert(lookupJoin.residualCondition.nonEmpty)
        assert(!lookupJoin.cacheOptions.enabled)
      }

      val batchSizes = java.nio.file.Files.readAllLines(requests.toPath)
      assert(batchSizes.size() > 2)
      batchSizes.forEach(size => assert(size.toInt > 0 && size.toInt <= 2))
      assert(batchSizes.contains("2"))
    }
  }

  test("register data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |$simpleDataSourceReaderScript
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT, partition INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin

    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    checkAnswer(
      spark.read.format(dataSourceName).load(),
      Seq(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1)))

    // Should be able to override an already registered data source.
    val newScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def read(self, partition):
         |        yield (0, )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val newDataSource = createUserDefinedPythonDataSource(dataSourceName, newScript)
    spark.dataSource.registerPython(dataSourceName, newDataSource)
    assert(spark.sessionState.dataSourceManager.dataSourceExists(dataSourceName))
    checkAnswer(
      spark.read.format(dataSourceName).load(),
      Seq(Row(0)))
  }

  test("load data source") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |import json
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def partitions(self):
         |        if "paths" in self.options:
         |            paths = json.loads(self.options["paths"])
         |        elif "path" in self.options:
         |            paths = [self.options["path"]]
         |        else:
         |            paths = []
         |        return [InputPartition(p) for p in paths]
         |
         |    def read(self, part):
         |        if part is not None:
         |            assert isinstance(part, InputPartition)
         |            yield (part.value, 1)
         |        else:
         |            yield (part, 1)
         |
         |class $dataSourceName(DataSource):
         |    @classmethod
         |    def name(cls) -> str:
         |        return "test"
         |
         |    def schema(self) -> str:
         |        return "id STRING, value INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython("test", dataSource)
    checkAnswer(spark.read.format("test").load(), Seq(Row(null, 1)))
    checkAnswer(spark.read.format("test").load("1"), Seq(Row("1", 1)))
    checkAnswer(spark.read.format("test").load("1", "2"), Seq(Row("1", 1), Row("2", 1)))

    withTable("tblA") {
      sql("CREATE TABLE tblA USING test")
      // The path will be the actual temp path.
      checkAnswer(spark.table("tblA").selectExpr("value"), Seq(Row(1)))
    }
  }

  test("SPARK-46522: data source name conflicts") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource
         |
         |class $dataSourceName(DataSource):
         |    ...
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    Seq(
      "text", "json", "csv", "avro", "orc", "parquet", "jdbc",
      "binaryFile", "xml", "kafka", "noop",
      "org.apache.spark.sql.test",
      "org.apache.spark.sql.hive.orc"
    ).foreach { provider =>
      withClue(s"Data source: $provider") {
        checkError(
          exception = intercept[AnalysisException] {
            spark.dataSource.registerPython(provider, dataSource)
          },
          condition = "DATA_SOURCE_ALREADY_EXISTS",
          parameters = Map("provider" -> provider))
      }
    }
  }

  test("reader not implemented") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
       s"""
        |from pyspark.sql.datasource import DataSource, DataSourceReader
        |class $dataSourceName(DataSource):
        |    pass
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("PySparkNotImplementedError"))
  }

  test("error creating reader") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |from pyspark.sql.datasource import DataSource
        |class $dataSourceName(DataSource):
        |    def reader(self, schema):
        |        raise Exception("error creating reader")
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("error creating reader"))
  }

  test("data source assertion error") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
        |class $dataSourceName:
        |   def __init__(self, options):
        |       ...
        |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource = createUserDefinedPythonDataSource(
      name = dataSourceName, pythonScript = dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val err = intercept[AnalysisException] {
      spark.read.format(dataSourceName).schema(schema).load().collect()
    }
    assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
    assert(err.getMessage.contains("DATA_SOURCE_TYPE_MISMATCH"))
    assert(err.getMessage.contains("PySparkAssertionError"))
  }

  test("data source read with custom partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
         |class RangePartition(InputPartition):
         |    def __init__(self, start, end):
         |        self.start = start
         |        self.end = end
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [RangePartition(1, 2), RangePartition(3, 4)]
         |
         |    def read(self, partition: RangePartition):
         |        start, end = partition.start, partition.end
         |        for i in range(start, end):
         |            yield (i, )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "id INT"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(1), Row(3)))
  }

  test("data source read with empty partitions") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return []
         |
         |    def read(self, partition):
         |        if partition.value is None:
         |            yield ("success", )
         |        else:
         |            yield ("failed", )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "status STRING"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Row("success"))
  }

  test("data source read with invalid partitions") {
    assume(shouldTestPandasUDFs)
    val reader1 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return 1
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    val reader2 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [1, 2]
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    val reader3 =
      s"""
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        raise Exception("error")
         |    def read(self, partition):
         |        ...
         |""".stripMargin

    Seq(reader1, reader2, reader3).foreach { readerScript =>
      val dataSourceScript =
        s"""
           |from pyspark.sql.datasource import DataSource, DataSourceReader
           |$readerScript
           |
           |class $dataSourceName(DataSource):
           |    def schema(self) -> str:
           |        return "id INT"
           |
           |    def reader(self, schema):
           |        return SimpleDataSourceReader()
           |""".stripMargin
      val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val err = intercept[AnalysisException](
        spark.read.format(dataSourceName).load().collect())
      assert(err.getCondition == "PYTHON_DATA_SOURCE_ERROR")
      assert(err.getMessage.contains("partitions"))
    }
  }

  test("SPARK-46540: data source read output named rows") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def read(self, partition):
         |        from pyspark.sql import Row
         |        yield Row(x = 0, y = 1)
         |        yield Row(y = 2, x = 1)
         |        yield Row(2, 3)
         |        yield (3, 4)
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "x int, y int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()
    checkAnswer(df, Seq(Row(0, 1), Row(1, 2), Row(2, 3), Row(3, 4)))
  }

  test("SPARK-46424: Support Python metrics") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceReader
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return []
         |
         |    def read(self, partition):
         |        if partition is None:
         |            yield ("success", )
         |        else:
         |            yield ("failed", )
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "status STRING"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.format(dataSourceName).load()

    val statusStore = spark.sharedState.statusStore
    val execId = runAndWaitForExecution(df.collect())

    val executedPlan = df.queryExecution.executedPlan.collectFirst {
      case p: BatchScanExec => p
    }
    assert(executedPlan.isDefined)

    val metrics = statusStore.executionMetrics(execId)
    val pythonDataSent = executedPlan.get.metrics("pythonDataSent")
    val pythonDataReceived = executedPlan.get.metrics("pythonDataReceived")
    assert(metrics.contains(pythonDataSent.id))
    assert(metrics(pythonDataSent.id).asInstanceOf[String].endsWith("B"))
    assert(metrics.contains(pythonDataReceived.id))
    assert(metrics(pythonDataReceived.id).asInstanceOf[String].endsWith("B"))
  }

  test("simple data source write") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def write(self, iterator):
         |        context = TaskContext.get()
         |        partition_id = context.partitionId()
         |        path = self.options.get("path")
         |        assert path is not None
         |        output_path = os.path.join(path, f"{partition_id}.json")
         |        cnt = 0
         |        with open(output_path, "w") as file:
         |            for row in iterator:
         |                file.write(json.dumps(row.asDict()) + "\\n")
         |                cnt += 1
         |        return WriterCommitMessage()
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, overwrite):
         |        return SimpleDataSourceWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    Seq(
      "SELECT * FROM range(0, 5, 1, 3)",
      "SELECT * FROM testData LIMIT 5",
      "SELECT * FROM testData3",
      "SELECT * FROM arrayData"
    ).foreach { query =>
      withTempDir { dir =>
        val df = sql(query)
        val path = dir.getAbsolutePath
        df.write.format(dataSourceName).mode("append").save(path)
        val df2 = spark.read.json(path)
        checkAnswer(df, df2)
      }
    }
  }

  test("data source write - error cases") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import DataSource, DataSourceWriter
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def write(self, iterator):
         |        num_rows = 0
         |        for row in iterator:
         |            num_rows += 1
         |            if num_rows > 2:
         |                raise Exception("something is wrong")
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, saveMode):
         |        return SimpleDataSourceWriter()
         |""".stripMargin
    spark.dataSource.registerPython(dataSourceName,
      createUserDefinedPythonDataSource(dataSourceName, dataSourceScript))

    withClue("user error") {
      val error = intercept[PythonException] {
        spark.range(10).write.format(dataSourceName).mode("append").save()
      }
      assert(error.getMessage.contains("something is wrong"))
    }

    withClue("no commit message") {
      val error = intercept[PythonException] {
        spark.range(1).write.format(dataSourceName).mode("append").save()
      }
      assert(error.getMessage.contains("DATA_SOURCE_TYPE_MISMATCH"))
      assert(error.getMessage.contains("WriterCommitMessage"))
    }

    withClue("without mode") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).write.format(dataSourceName).save()
        },
        condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
        parameters = Map("source" -> "SimpleDataSource", "createMode" -> "\"ErrorIfExists\""))
    }

    withClue("with unsupported mode") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).write.format(dataSourceName).mode("ignore").save()
        },
        condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
        parameters = Map("source" -> "SimpleDataSource", "createMode" -> "\"Ignore\""))
    }

    withClue("invalid mode") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).write.format(dataSourceName).mode("foo").save()
        },
        condition = "INVALID_SAVE_MODE",
        parameters = Map("mode" -> "\"foo\""))
    }
  }

  test("data source write - overwrite mode") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options, overwrite):
         |        self.options = options
         |        self.overwrite = overwrite
         |
         |    def write(self, iterator):
         |        context = TaskContext.get()
         |        partition_id = context.partitionId()
         |        path = self.options.get("path")
         |        assert path is not None
         |        output_path = os.path.join(path, f"{partition_id}.json")
         |        cnt = 0
         |        mode = "w" if self.overwrite else "a"
         |        with open(output_path, mode) as file:
         |            for row in iterator:
         |                file.write(json.dumps(row.asDict()) + "\\n")
         |                cnt += 1
         |        return WriterCommitMessage()
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, overwrite):
         |        return SimpleDataSourceWriter(self.options, overwrite)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(1).write.format(dataSourceName).mode("append").save(path)
      checkAnswer(
        spark.read.json(path),
        Seq(Row(0)))
      spark.range(1).write.format(dataSourceName).mode("append").save(path)
      checkAnswer(
        spark.read.json(path),
        Seq(Row(0), Row(0)))
      spark.range(2, 3).write.format(dataSourceName).mode("overwrite").save(path)
      checkAnswer(
        spark.read.json(path),
        Seq(Row(2)))
    }
  }

  test("data source write commit and abort") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |import json
         |import os
         |from dataclasses import dataclass
         |from pyspark import TaskContext
         |from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
         |
         |@dataclass
         |class SimpleCommitMessage(WriterCommitMessage):
         |    partition_id: int
         |    count: int
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options):
         |        self.options = options
         |        self.path = self.options.get("path")
         |        assert self.path is not None
         |
         |    def write(self, iterator):
         |        context = TaskContext.get()
         |        partition_id = context.partitionId()
         |        output_path = os.path.join(self.path, f"{partition_id}.json")
         |        cnt = 0
         |        with open(output_path, "w") as file:
         |            for row in iterator:
         |                if row.id >= 10:
         |                    raise Exception("invalid value")
         |                file.write(json.dumps(row.asDict()) + "\\n")
         |                cnt += 1
         |        return SimpleCommitMessage(partition_id=partition_id, count=cnt)
         |
         |    def commit(self, messages) -> None:
         |        status = dict(num_files=len(messages), count=sum(m.count for m in messages))
         |
         |        with open(os.path.join(self.path, "success.json"), "a") as file:
         |            file.write(json.dumps(status) + "\\n")
         |
         |    def abort(self, messages) -> None:
         |        with open(os.path.join(self.path, "failed.txt"), "a") as file:
         |            file.write("failed")
         |
         |class SimpleDataSource(DataSource):
         |    def writer(self, schema, saveMode):
         |        return SimpleDataSourceWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      withClue("commit") {
        sql("SELECT * FROM range(0, 5, 1, 3)")
          .write.format(dataSourceName)
          .mode("append")
          .save(path)
        checkAnswer(
          spark.read.format("json")
            .schema("num_files bigint, count bigint")
            .load(path + "/success.json"),
          Seq(Row(3, 5)))
      }

      withClue("commit again") {
        sql("SELECT * FROM range(5, 7, 1, 1)")
          .write.format(dataSourceName)
          .mode("append")
          .save(path)
        checkAnswer(
          spark.read.format("json")
            .schema("num_files bigint, count bigint")
            .load(path + "/success.json"),
          Seq(Row(3, 5), Row(1, 2)))
      }

      withClue("abort") {
        intercept[PythonException] {
          sql("SELECT * FROM range(8, 12, 1, 4)")
            .write.format(dataSourceName)
            .mode("append")
            .save(path)
        }
        checkAnswer(
          spark.read.text(path + "/failed.txt"),
          Seq(Row("failed")))
      }
    }
  }

  test("SPARK-46568: case insensitive options") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource, DataSourceReader, DataSourceWriter, WriterCommitMessage)
         |class SimpleDataSourceReader(DataSourceReader):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def read(self, partition):
         |        foo = self.options.get("Foo")
         |        bar = self.options.get("BAR")
         |        baz = "BaZ" in self.options
         |        yield (foo, bar, baz)
         |
         |class SimpleDataSourceWriter(DataSourceWriter):
         |    def __init__(self, options):
         |        self.options = options
         |
         |    def write(self, row):
         |        if "FOO" not in self.options or "BAR" not in self.options:
         |            raise Exception("FOO or BAR not found")
         |        return WriterCommitMessage()
         |
         |class $dataSourceName(DataSource):
         |    def schema(self) -> str:
         |        return "a string, b string, c string"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader(self.options)
         |
         |    def writer(self, schema, overwrite):
         |        return SimpleDataSourceWriter(self.options)
         |""".stripMargin
    val dataSource = createUserDefinedPythonDataSource(dataSourceName, dataSourceScript)
    spark.dataSource.registerPython(dataSourceName, dataSource)
    val df = spark.read.option("foo", 1).option("bar", 2).option("BAZ", 3)
      .format(dataSourceName).load()
    checkAnswer(df, Row("1", "2", "true"))
    df.write.option("foo", 1).option("bar", 2).format(dataSourceName).mode("append").save()
  }
}

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

package org.apache.spark.sql.connector

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkException, SparkIllegalArgumentException,
  SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupCacheOptions,
  ExternalLookupJoin, Join, JoinHint, LocalRelation, PreparedExternalLookupJoin}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, Table, TableCapability,
  TableCatalog, TableChange, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, ExternalLookup, ExternalLookupReader,
  ExternalLookupReaderFactory, ExternalLookupResultCursor, InputPartition, LookupParameter,
  PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsExternalLookup,
  SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExternalLookupRelation,
  PrepareExternalLookupJoin}
import org.apache.spark.sql.execution.joins.ExternalLookupJoinExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.functions.{coalesce, col, concat, expr, lit, rand, upper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamRealTimeModeE2ESuiteBase
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ThreadUtils

class ExternalLookupJoinSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val ExternalLookupCatalogName = "external_lookup_cat"

  test("external lookup capability produces a unary external lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")
    val joined = left.join(lookup, left("id") === lookup("id"), "inner")

    assert(joined.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    assert(!joined.queryExecution.optimizedPlan.toString.contains("SimpleExternalLookupScan"))
  }

  test("a dual-capability source keeps an ordinary join unless the hint opts in") {
    val lookup = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")
    val expected = Seq(Row(1, 1, "engineering"), Row(1, 1, "research"))

    assert(lookup.queryExecution.analyzed.isInstanceOf[DataSourceV2Relation])
    assert(!lookup.queryExecution.analyzed.isInstanceOf[ExternalLookupRelation])
    SimpleExternalLookupDataSource.resetCounters()
    val ordinary = left.join(lookup, left("id") === lookup("id"), "inner")
    assert(!ordinary.queryExecution.optimizedPlan.exists(
      _.isInstanceOf[PreparedExternalLookupJoin]))
    checkAnswer(ordinary, expected)
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)

    SimpleExternalLookupDataSource.resetCounters()
    val hinted = lookup.hint("external_lookup")
    val lookupJoin = left.join(hinted, left("id") === hinted("id"), "inner")
    assert(lookupJoin.queryExecution.optimizedPlan.exists(
      _.isInstanceOf[PreparedExternalLookupJoin]))
    checkAnswer(lookupJoin, expected)
    assert(SimpleExternalLookupDataSource.requestCount.get() > 0)
  }

  test("a physical join strategy hint overrides the external lookup hint") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[DualCapabilityExternalLookupDataSource].getName)
      .load()
      .hint("external_lookup")
      .hint("merge")
    val left = Seq(1).toDF("id")
    val joined = left.join(lookup, left("id") === lookup("id"), "inner")

    assert(!joined.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    checkAnswer(joined, Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
    assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
  }

  test("a hinted dual-capability source fails when no lookup access path exists") {
    val lookup = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = Seq((1, "sales")).toDF("id", "department")

    checkAnswer(
      left.join(lookup, left("department") === lookup("department")),
      Seq(Row(1, "sales", 2, "sales")))
    val hinted = lookup.hint("external_lookup")
    checkError(
      exception = intercept[AnalysisException] {
        left.join(hinted, left("department") === hinted("department"))
          .queryExecution.optimizedPlan
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
      parameters = Map("table" -> "`DualCapabilityExternalLookupTable`"))
  }

  test("a physical strategy hint does not disable lookup for a lookup-only table") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1, 3).toDF("id")
    val expected = Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null))
    Seq(
      left.join(lookup.hint("broadcast"), left("id") === lookup("id"), "left_outer"),
      left.hint("merge").join(lookup, left("id") === lookup("id"), "left_outer")).foreach { df =>
      assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
      checkAnswer(df, expected)
    }
  }

  test("an ordinary join over an enriched lookup join result stays ordinary") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val events = Seq(1, 2).toDF("id")
    val enriched = events.join(lookup, events("id") === lookup("id"))
      .select(events("id").as("event_id"), lookup("department"))
    val base = Seq((1, "a"), (2, "b")).toDF("id", "tag")
    val expected = Seq(
      Row(1, "a", 1, "engineering"), Row(1, "a", 1, "research"), Row(2, "b", 2, "sales"))
    def lookupJoinCount(df: DataFrame): Int =
      df.queryExecution.optimizedPlan.collect { case j: PreparedExternalLookupJoin => j }.size

    val joined = base.join(enriched, base("id") === enriched("event_id"))
    assert(lookupJoinCount(joined) === 1)
    checkAnswer(joined, expected)

    withTempView("external_lookup_base", "external_lookup_events", "external_lookup_right") {
      base.createOrReplaceTempView("external_lookup_base")
      events.createOrReplaceTempView("external_lookup_events")
      lookup.createOrReplaceTempView("external_lookup_right")
      val sqlJoined = sql(
        """
          |SELECT b.id, b.tag, e.event_id, e.department
          |FROM external_lookup_base b JOIN (
          |  SELECT v.id AS event_id, r.department
          |  FROM external_lookup_events v JOIN external_lookup_right r ON v.id = r.id) e
          |ON b.id = e.event_id
          |""".stripMargin)
      assert(lookupJoinCount(sqlJoined) === 1)
      checkAnswer(sqlJoined, expected)
    }
  }

  test("an external lookup hint outside any join is ignored") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales"))

    checkAnswer(dual.hint("external_lookup"), expected)
    assert(dual.hint("external_lookup", "cache_enabled=false").count() === 3)
    withTempView("external_lookup_right") {
      dual.createOrReplaceTempView("external_lookup_right")
      checkAnswer(sql("SELECT /*+ EXTERNAL_LOOKUP */ * FROM external_lookup_right"), expected)
      checkAnswer(sql("SELECT /*+ EXTERNAL_LOOKUP(r) */ * FROM external_lookup_right r"), expected)
      checkAnswer(
        sql("SELECT /*+ EXTERNAL_LOOKUP(r) */ id, count(*) FROM external_lookup_right r " +
          "GROUP BY id"),
        Seq(Row(1, 2), Row(2, 1)))
    }
  }

  test("a strategy hint on a nested join does not disable an outer lookup join") {
    val left = Seq((1, 10), (2, 20)).toDF("id", "k")
    val other = Seq((10, "a"), (20, "b")).toDF("k", "v")
    val nested = left.join(other.hint("merge"), "k")
    def usesLookup(df: DataFrame): Boolean =
      df.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
      .hint("external_lookup")
    val dualJoin = nested.join(dual, nested("id") === dual("id"))
    assert(usesLookup(dualJoin))
    checkAnswer(dualJoin, Seq(
      Row(10, 1, "a", 1, "engineering"), Row(10, 1, "a", 1, "research"),
      Row(20, 2, "b", 2, "sales")))

    // Falling back to an ordinary join would fail: a lookup-only table cannot be scanned.
    val lookupOnly = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookupOnlyJoin = nested.join(lookupOnly, nested("id") === lookupOnly("id"))
    assert(usesLookup(lookupOnlyJoin))
    checkAnswer(lookupOnlyJoin, Seq(
      Row(10, 1, "a", 1, "engineering"), Row(10, 1, "a", 1, "research"),
      Row(20, 2, "b", 2, "sales")))
  }

  test("an explicit external lookup option marker rejects unsupported join shapes") {
    val lookup = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")

    checkError(
      exception = intercept[AnalysisException] {
        left.join(
          lookup.hint("external_lookup", "cache_enabled=false"),
          left("id") === lookup("id"),
          "right_outer").queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.JOIN_TYPE",
      parameters = Map(
        "joinType" -> "RIGHT OUTER",
        "supportedJoinTypes" -> "INNER, LEFT OUTER, LEFT SEMI, LEFT ANTI"))

    checkError(
      exception = intercept[AnalysisException] {
        left.crossJoin(lookup.hint("external_lookup")).queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)

    SimpleExternalLookupDataSource.resetCounters()
    val optedOut = left.join(
      lookup.hint("external_lookup", "cache_enabled=false").hint("merge"),
      left("id") === lookup("id"),
      "right_outer")
    assert(!optedOut.queryExecution.optimizedPlan.exists(
      _.isInstanceOf[PreparedExternalLookupJoin]))
    checkAnswer(optedOut, Seq(
      Row(1, 1, "engineering"),
      Row(1, 1, "research"),
      Row(null, 2, "sales")))
    assert(SimpleExternalLookupDataSource.requestCount.get() === 0)
  }

  test("external lookup predicate negotiation includes type and null semantics") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq[java.lang.Integer](null, 1).toDF("id")

    left.join(lookup, left("id") <=> lookup("id"), "inner").collect()
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq(("<=>", "id", IntegerType)))
  }

  test("external lookup validates connector planning and request contracts") {
    val left = Seq(1).toDF("id")
    Seq(
      ("nullKeySelection", "true", "NULL_PREDICATE_SELECTION", Map.empty[String, String]),
      ("selectionMode", "nullElement", "NULL_PREDICATE_SELECTION", Map.empty[String, String]),
      ("selectionMode", "duplicate", "DUPLICATE_SELECTED_PREDICATE", Map("predicate" -> "id = ?")),
      ("selectionMode", "copy", "UNOFFERED_SELECTED_PREDICATE", Map("predicate" -> "id = ?")),
      ("maxBatchSize", "0", "NON_POSITIVE_MAX_PARAMETER_ROWS", Map("value" -> "0"))).foreach {
      case (option, value, subClass, parameters) =>
        val lookup = spark.read
          .format(classOf[SimpleExternalLookupDataSource].getName)
          .option(option, value)
          .load()
          .hint("external_lookup")
        checkError(
          exception = intercept[AnalysisException] {
            left.join(lookup, left("id") === lookup("id")).queryExecution.optimizedPlan
          },
          condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
          parameters = Map("table" -> "`SimpleExternalLookupTable`") ++ parameters)
    }

    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("nullCursor", "true")
      .load()
      .hint("external_lookup")
    val error = intercept[SparkException] {
      left.join(lookup, left("id") === lookup("id")).collect()
    }
    assert(error.getMessage.contains("External lookup returned a null result cursor"))

    def misconfigured(kind: String): DataFrame = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", kind)
      .load()
    val table = Map("table" -> "`MisconfiguredExternalLookupTable`")
    Seq(
      ("unknownField", "UNKNOWN_SCAN_FIELD", Map("field" -> "`unknown`")),
      ("duplicateField", "DUPLICATE_SCAN_FIELD", Map("field" -> "`id`"))).foreach {
      case (kind, subClass, parameters) =>
        val lookup = misconfigured(kind)
        checkError(
          exception = intercept[AnalysisException] {
            left.join(lookup, left("id") === lookup("id")).queryExecution.optimizedPlan
          },
          condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
          parameters = table ++ parameters)
    }

    val noExternalLookup = misconfigured("noExternalLookup")
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        left.join(noExternalLookup, left("id") === noExternalLookup("id"))
          .queryExecution.optimizedPlan
      },
      condition = "DATA_SOURCE_EXTERNAL_LOOKUP_NOT_SUPPORTED",
      parameters = Map("description" -> "NoExternalLookupScan"))

    val invalidOrdinal = misconfigured("invalidOrdinal")
    val ordinalError = intercept[SparkException] {
      left.join(invalidOrdinal, left("id") === invalidOrdinal("id")).collect()
    }
    assert(ordinalError.getMessage.contains(
      "External lookup returned invalid input ordinal 5 for 1 requested parameter rows"))
  }

  test("an external lookup hinted DataFrame can be reused by multiple joins") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      .hint("external_lookup")

    val first = Seq(1).toDF("id")
    val second = Seq(2).toDF("id")
    checkAnswer(first.join(lookup, first("id") === lookup("id")),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
    checkAnswer(second.join(lookup, second("id") === lookup("id")),
      Seq(Row(2, 2, "sales")))
  }

  test("external lookup finalizes expressions on the stream side after analysis") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq[java.lang.Integer](null, 1).toDF("id")

    checkAnswer(
      left.join(
        lookup.hint("external_lookup"),
        coalesce(left("id"), lit(0)) === lookup("id"),
        "inner"),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research")))
  }

  test("external lookup prunes unused throwable lookup expressions") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup.select($"id", expr("raise_error('unused')").as("unused"))
    val left = Seq(1).toDF("id")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "left_semi"),
      Seq(Row(1)))

    val projected = left.as("left").join(
      lookup.hint("external_lookup").as("lookup"),
      $"left.id" === $"lookup.id",
      "inner").select($"left.id")
    checkAnswer(projected, Seq(Row(1), Row(1)))
  }

  test("external lookup preserves non-deterministic projection and filter boundaries") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup
      .select($"id", rand(0).as("sample"))
      .filter($"sample" >= 0)
      .select($"id")
    val left = Seq(1).toDF("id")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1), Row(1, 1)))
  }

  test("external lookup hint rejects duplicate and malformed options") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val positiveInteger = Map("expected" -> "a positive integer")
    val duration = Map("expected" -> "a non-negative duration, such as 500ms, 5s, or 10m")
    val supportedOptions = Map("supportedOptions" ->
      "\"cache_enabled\", \"cache_max_entries\", \"cache_max_rows_per_key\", \"cache_ttl\"")
    Seq(
      (Seq("cache_enabled=true", "cache_enabled=false"), "cache_enabled", "DUPLICATE", Map()),
      (Seq("cache_enabled=unknown"), "cache_enabled", "INVALID_VALUE",
        Map("expected" -> "true or false")),
      (Seq("cache_enabled"), "cache_enabled", "INVALID_FORMAT", Map()),
      (Seq("cache_enabled="), "cache_enabled=", "INVALID_FORMAT", Map()),
      (Seq("cache_mode=enabled"), "cache_mode", "UNSUPPORTED", supportedOptions),
      (Seq("cache_max_entries=0"), "cache_max_entries", "INVALID_VALUE", positiveInteger),
      (Seq("cache_max_entries=abc"), "cache_max_entries", "INVALID_VALUE", positiveInteger),
      (Seq("cache_ttl=-1ms"), "cache_ttl", "INVALID_VALUE", duration),
      (Seq("cache_ttl=abc"), "cache_ttl", "INVALID_VALUE", duration),
      (Seq("cache_max_rows_per_key=0"), "cache_max_rows_per_key", "INVALID_VALUE",
        positiveInteger),
      (Seq("unknown_option=1"), "unknown_option", "UNSUPPORTED", supportedOptions)).foreach {
      case (options, option, subClass, parameters) =>
        checkError(
          exception = intercept[AnalysisException] {
            lookup.hint("external_lookup", options: _*).queryExecution.analyzed
          },
          condition = s"INVALID_EXTERNAL_LOOKUP_OPTION.$subClass",
          parameters = Map("option" -> ("\"" + option + "\"")) ++ parameters)
    }
    checkError(
      exception = intercept[AnalysisException] {
        lookup.hint("external_lookup", 1).queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.UNSUPPORTED_PARAMETER",
      parameters = Map("parameter" -> "\"1\""))
  }

  test("external lookup hint rejects invalid placement and relation targets") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")

    checkError(
      exception = intercept[AnalysisException] {
        left.hint("external_lookup").join(lookup, left("id") === lookup("id"))
          .queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      parameters = Map.empty)
    checkError(
      exception = intercept[AnalysisException] {
        val hinted = lookup.hint("external_lookup").hint("external_lookup")
        left.join(hinted, left("id") === hinted("id")).queryExecution.analyzed
      },
      condition = "INVALID_EXTERNAL_LOOKUP_HINT.MULTIPLE_HINTS",
      parameters = Map.empty)

    withTempView("external_lookup_left", "external_lookup_right") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      def query(hint: String): DataFrame = sql(
        s"""
          |SELECT /*+ $hint */ l.id, r.department
          |FROM external_lookup_left l JOIN external_lookup_right r ON l.id = r.id
          |""".stripMargin)
      checkError(
        exception = intercept[AnalysisException] {
          query("EXTERNAL_LOOKUP(missing)").queryExecution.analyzed
        },
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
        parameters = Map("relations" -> "`missing`"))
      checkError(
        exception = intercept[AnalysisException] {
          query("EXTERNAL_LOOKUP(r, r)").queryExecution.analyzed
        },
        condition = "INVALID_EXTERNAL_LOOKUP_HINT.DUPLICATE_RELATION",
        parameters = Map("relation" -> "`r`"))
    }

    val stream = MemoryStream[Int].toDF().toDF("id")
    checkError(
      exception = intercept[AnalysisException] {
        left.join(stream.hint("external_lookup"), left("id") === stream("id"))
          .queryExecution.analyzed
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.STREAMING_LOOKUP_SIDE",
      parameters = Map.empty)
  }

  test("external lookup reports lookup sides and conditions it cannot plan") {
    val left = Seq((1, "sales")).toDF("id", "department")
    def plan(lookup: DataFrame, condition: Column): Unit =
      left.join(lookup, condition).queryExecution.optimizedPlan

    val local = Seq(1).toDF("id").hint("external_lookup")
    checkError(
      exception = intercept[AnalysisException] { plan(local, left("id") === local("id")) },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
      parameters = Map.empty)

    val batchOnly = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", "batchOnly")
      .load()
      .hint("external_lookup")
    checkError(
      exception = intercept[AnalysisException] {
        plan(batchOnly, left("id") === batchOnly("id"))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.TABLE",
      parameters = Map("table" -> "`MisconfiguredExternalLookupTable`"))

    val noLookupBuilder = spark.read
      .format(classOf[MisconfiguredExternalLookupDataSource].getName)
      .option("kind", "noLookupBuilder")
      .load()
    checkError(
      exception = intercept[AnalysisException] {
        plan(noLookupBuilder, left("id") === noLookupBuilder("id"))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.SCAN_BUILDER",
      parameters = Map("table" -> "`MisconfiguredExternalLookupTable`"))

    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    checkError(
      exception = intercept[AnalysisException] {
        plan(lookup, left("department") === lookup("department"))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE",
      parameters = Map("table" -> "`SimpleExternalLookupTable`"))
  }

  test("external lookup maps a selected predicate to its own occurrence") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("selectionMode", "last")
      .load()
      .hint("external_lookup", "cache_enabled=false")
    val left = Seq((1, 2), (2, 2), (2, 1)).toDF("a", "b").coalesce(1)

    // Both candidates have the shape `id = ?`; only the second one, bound to `b`, is selected.
    val joined =
      left.join(lookup, lookup("id") === left("a") && lookup("id") === left("b"), "left_outer")
    assert(joined.collect().toSet ===
      Set(Row(1, 2, null, null), Row(2, 2, 2, "sales"), Row(2, 1, null, null)))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq(("=", "id", IntegerType), ("=", "id", IntegerType)))
    assert(SimpleExternalLookupDataSource.lookupCandidateDescriptions.get() ===
      Seq("id = ?", "id = ?"))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 1))
  }

  test("external lookup join supports inner and left outer joins") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "label")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "inner"),
      Seq(Row(1, "one", 1, "engineering"), Row(1, "one", 1, "research"),
        Row(2, "two", 2, "sales")))
    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "left_outer"),
      Seq(Row(1, "one", 1, "engineering"), Row(1, "one", 1, "research"),
        Row(2, "two", 2, "sales"), Row(3, "three", null, null)))
  }

  test("external lookup-only source joins without an explicit hint") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")

    assert(lookup.queryExecution.analyzed.isInstanceOf[ExternalLookupRelation])
    val joined = left.join(lookup, "id")
    assert(joined.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin]))
    checkAnswer(joined, Seq(Row(1, "engineering"), Row(1, "research")))
  }

  test("an external lookup-only source cannot be read outside an eligible lookup join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")
    Seq(
      lookup,
      left.crossJoin(lookup),
      left.join(lookup, left("id") === lookup("id"), "full_outer")).foreach { df =>
      checkError(
        exception = intercept[AnalysisException] { df.queryExecution.optimizedPlan },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))
    }
  }

  test("catalog-resolved tables follow the same lookup selection") {
    withSQLConf(
        s"spark.sql.catalog.$ExternalLookupCatalogName" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("external_lookup_left") {
        Seq(1, 3).toDF("id").createOrReplaceTempView("external_lookup_left")
        def query(table: String, hint: String = ""): DataFrame = sql(
          s"""
            |SELECT $hint l.id, r.department
            |FROM external_lookup_left l
            |LEFT JOIN $ExternalLookupCatalogName.ns.$table r ON l.id = r.id
            |""".stripMargin)
        val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(3, null))
        def usesLookup(df: DataFrame): Boolean =
          df.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

        SimpleExternalLookupDataSource.resetCounters()
        val lookupOnly = query("lookup")
        assert(usesLookup(lookupOnly))
        checkAnswer(lookupOnly, expected)
        assert(SimpleExternalLookupDataSource.requestCount.get() > 0)

        SimpleExternalLookupDataSource.resetCounters()
        val dual = query("dual")
        assert(!usesLookup(dual))
        checkAnswer(dual, expected)
        assert(SimpleExternalLookupDataSource.readerCount.get() === 0)

        val hintedDual = query("dual", "/*+ EXTERNAL_LOOKUP(r) */")
        assert(usesLookup(hintedDual))
        checkAnswer(hintedDual, expected)
      }
    }
  }

  test("a named external lookup hint targets unaliased, multipart, and aliased relations") {
    withSQLConf(
        s"spark.sql.catalog.$ExternalLookupCatalogName" ->
          classOf[ExternalLookupTestCatalog].getName) {
      withTempView("external_lookup_left", "external_lookup_right") {
        Seq(1, 3).toDF("id").createOrReplaceTempView("external_lookup_left")
        spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
          .createOrReplaceTempView("external_lookup_right")
        val expected = Seq(Row(1, "engineering"), Row(1, "research"), Row(3, null))
        def query(hintTarget: String, right: String, rightName: String): DataFrame = sql(
          s"""
            |SELECT /*+ EXTERNAL_LOOKUP($hintTarget) */ l.id, $rightName.department
            |FROM external_lookup_left l LEFT JOIN $right ON l.id = $rightName.id
            |""".stripMargin)

        val catalogTable = s"$ExternalLookupCatalogName.ns.dual"
        Seq(
          ("external_lookup_right", "external_lookup_right", "external_lookup_right"),
          ("dual", catalogTable, "dual"),
          ("ns.dual", catalogTable, "dual"),
          (catalogTable, catalogTable, "dual"),
          ("r", s"$catalogTable r", "r"),
          ("s", "(SELECT * FROM external_lookup_right) s", "s")).foreach {
          case (hintTarget, right, rightName) =>
            val df = query(hintTarget, right, rightName)
            assert(df.queryExecution.optimizedPlan.exists(
              _.isInstanceOf[PreparedExternalLookupJoin]), hintTarget)
            checkAnswer(df, expected)
        }

        // Like join strategy hints, a target name does not reach into an aliased subquery.
        checkError(
          exception = intercept[AnalysisException] {
            query("external_lookup_right", "(SELECT * FROM external_lookup_right) s", "s")
              .queryExecution.analyzed
          },
          condition = "INVALID_EXTERNAL_LOOKUP_HINT.RELATION_NOT_FOUND",
          parameters = Map("relations" -> "`external_lookup_right`"))
      }
    }
  }

  test("a catalog-resolved table can be composed into DataFrame joins") {
    withSQLConf(
        s"spark.sql.catalog.$ExternalLookupCatalogName" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val left = Seq(1, 3).toDF("id")
      val expected = Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null))
      def usesLookup(df: DataFrame): Boolean =
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[PreparedExternalLookupJoin])

      val lookupOnly = spark.table(s"$ExternalLookupCatalogName.ns.lookup")
      val lookupJoin = left.join(lookupOnly, left("id") === lookupOnly("id"), "left_outer")
      assert(usesLookup(lookupJoin))
      checkAnswer(lookupJoin, expected)

      val dual = spark.table(s"$ExternalLookupCatalogName.ns.dual")
      val ordinary = left.join(dual, left("id") === dual("id"), "left_outer")
      assert(!usesLookup(ordinary))
      checkAnswer(ordinary, expected)
      val hinted = dual.hint("external_lookup")
      val hintedJoin = left.join(hinted, left("id") === hinted("id"), "left_outer")
      assert(usesLookup(hintedJoin))
      checkAnswer(hintedJoin, expected)

      checkError(
        exception = intercept[AnalysisException] { lookupOnly.collect() },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`SimpleExternalLookupTable`",
          "operation" -> "batch scan"))
    }
  }

  test("a lookup-only relation shared by a lookup join and a standalone scan is rejected") {
    val left = LocalRelation(AttributeReference("id", IntegerType)())
    val relation = DataSourceV2Relation.create(
      SimpleExternalLookupTable, None, None, CaseInsensitiveStringMap.empty)
    val lookupJoin = ExternalLookupJoin(
      left,
      relation,
      Inner,
      Some(EqualTo(left.output.head, relation.output.head)),
      ExternalLookupCacheOptions(enabled = false, 1, 0, 1),
      relation.output)

    assert(PrepareExternalLookupJoin(lookupJoin).isInstanceOf[PreparedExternalLookupJoin])
    checkError(
      exception = intercept[AnalysisException] {
        PrepareExternalLookupJoin(Join(lookupJoin, relation, Cross, None, JoinHint.NONE))
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`SimpleExternalLookupTable`",
        "operation" -> "batch scan"))
  }

  test("a catalog-resolved external lookup-only table cannot be read outside a lookup join") {
    withSQLConf(
        s"spark.sql.catalog.$ExternalLookupCatalogName" ->
          classOf[ExternalLookupTestCatalog].getName) {
      val table = s"$ExternalLookupCatalogName.ns.lookup"
      val left = Seq(1).toDF("id")
      Seq[() => DataFrame](
        () => spark.table(table),
        () => sql(s"SELECT * FROM $table WHERE id = 1"),
        () => left.crossJoin(spark.read.table(table)),
        () => sql(s"SELECT * FROM $table r FULL OUTER JOIN $table s ON r.id = s.id")).foreach {
        df =>
          checkError(
            exception = intercept[AnalysisException] { df().queryExecution.optimizedPlan },
            condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
            parameters = Map(
              "tableName" -> "`SimpleExternalLookupTable`",
              "operation" -> "batch scan"))
      }
    }
  }

  test("external lookup join applies lookup-side projection and filter before joining") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup
      .filter($"department" =!= "research")
      .select($"id", upper($"department").as("department"))
    val left = Seq(1, 2).toDF("id")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1, "ENGINEERING"), Row(2, 2, "SALES")))
  }

  test("external lookup rebinds projections after required-column pruning") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1, 2).toDF("id")

    val result = left.join(
      lookup.hint("external_lookup"), left("id") === lookup("id"), "inner")
      .select(upper(lookup("department")))

    checkAnswer(result, Seq(Row("ENGINEERING"), Row("RESEARCH"), Row("SALES")))
  }

  test("external lookup join resolves a projected key alias") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup.select($"id".as("lookup_id"), $"department")
    val left = Seq(1, 3).toDF("id")

    checkAnswer(
      left.join(
        lookup.hint("external_lookup"), left("id") === lookup("lookup_id"), "left_outer"),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null)))
  }

  test("external lookup join composes nested aliases and filters") {
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val once = rawLookup.select($"id".as("lookup_id"), $"department")
    val lookup = once.filter($"lookup_id" > 0).select($"lookup_id", $"department")
    val left = Seq(1, 3).toDF("id")

    checkAnswer(
      left.join(
        lookup.hint("external_lookup"), left("id") === lookup("lookup_id"), "left_outer"),
      Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(3, null, null)))
  }

  test("bare SQL external lookup hint targets the right side of the join") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")

    withTempView("external_lookup_left", "external_lookup_right") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      checkAnswer(
        sql("""
          |SELECT /*+ EXTERNAL_LOOKUP */ l.id, r.department
          |FROM external_lookup_left l JOIN external_lookup_right r ON l.id = r.id
          |""".stripMargin),
        Seq(Row(1, "engineering"), Row(1, "research")))
    }
  }

  test("SQL external lookup hint accepts cache options") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
    val left = Seq(1, 2).toDF("id").coalesce(1)

    withTempView("external_lookup_left", "external_lookup_right") {
      left.createOrReplaceTempView("external_lookup_left")
      lookup.createOrReplaceTempView("external_lookup_right")
      val query = sql("""
          |SELECT /*+ EXTERNAL_LOOKUP(
          |  r,
          |  'cache_enabled=false',
          |  'cache_max_entries=123',
          |  'cache_ttl=2s',
          |  'cache_max_rows_per_key=5'
          |) */ l.id, r.department
          |FROM external_lookup_left l JOIN external_lookup_right r ON l.id = r.id
          |""".stripMargin)
      checkAnswer(
        query,
        Seq(Row(1, "engineering"), Row(1, "research"), Row(2, "sales")))
      val prepared = query.queryExecution.optimizedPlan.collectFirst {
        case externalLookup: PreparedExternalLookupJoin => externalLookup
      }.get
      assert(prepared.cacheOptions === ExternalLookupCacheOptions(
        enabled = false,
        maxEntries = 123,
        ttlMillis = 2000,
        maxRowsPerKey = 5))
    }
    assert(SimpleExternalLookupDataSource.requestCount.get() > 0)
  }

  test("external lookup pushes accepted filters and required columns to the source") {
    SimpleExternalLookupDataSource.resetCounters()
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val lookup = rawLookup.filter($"department" =!= "research").select($"id")
    val left = Seq(1, 2).toDF("id")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1), Row(2, 2)))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("id"))
    assert(SimpleExternalLookupDataSource.pushedPredicateCount.get() > 0)
  }

  test("external lookup canonicalizes filters through a relation alias") {
    SimpleExternalLookupDataSource.resetCounters()
    val rawLookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
      .alias("lookup_source")
    val lookup = rawLookup
      .filter($"lookup_source.DEPARTMENT" =!= "research")
      .select($"lookup_source.id")
    val left = Seq(1, 2).toDF("id")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "inner"),
      Seq(Row(1, 1), Row(2, 2)))
    assert(SimpleExternalLookupDataSource.pushedPredicateFields.get() === Seq("department"))
  }

  test("external lookup join supports left semi and left anti joins") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1, 2, 3).toDF("id")

    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "left_semi"),
      Seq(Row(1), Row(2)))
    assert(SimpleExternalLookupDataSource.requiredColumns.get().isEmpty)
    checkAnswer(
      left.join(lookup.hint("external_lookup"), left("id") === lookup("id"), "left_anti"),
      Seq(Row(3)))
  }

  test("external lookup prunes request keys from inner and outer responses") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1, 3).toDF("id")

    Seq("inner", "left_outer").foreach { joinType =>
      SimpleExternalLookupDataSource.resetCounters()
      val result = left.join(
        lookup.hint("external_lookup"), left("id") === lookup("id"), joinType)
        .select(left("id"))
      val expected = if (joinType == "inner") Seq(Row(1), Row(1)) else {
        Seq(Row(1), Row(1), Row(3))
      }
      checkAnswer(result, expected)
      assert(SimpleExternalLookupDataSource.requiredColumns.get().isEmpty)
    }
  }

  test("external lookup evaluates semi and anti residuals on the pruned response layout") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq((1, "engineering"), (1, "missing"), (3, "missing"))
      .toDF("id", "department")
    val condition = left("id") === lookup("id") &&
      left("department") === lookup("department")

    SimpleExternalLookupDataSource.resetCounters()
    checkAnswer(
      left.join(lookup.hint("external_lookup"), condition, "left_semi"),
      Seq(Row(1, "engineering")))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("department"))

    SimpleExternalLookupDataSource.resetCounters()
    checkAnswer(
      left.join(lookup.hint("external_lookup"), condition, "left_anti"),
      Seq(Row(1, "missing"), Row(3, "missing")))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("department"))
  }

  test("external lookup join handles null, missing, duplicate, and partial batches") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "3")
      .load()
      .hint("external_lookup", "cache_enabled=false")
    val left = Seq[java.lang.Integer](1, 1, null, 3, 2).toDF("id").coalesce(1)

    val rows = left.join(lookup, left("id") === lookup("id"), "left_outer").collect().toSeq
    assert(rows.toSet === Set(
      Row(1, 1, "engineering"),
      Row(1, 1, "research"),
      Row(null, null, null),
      Row(3, null, null),
      Row(2, 2, "sales")))
    assert(rows.count(_.get(0) == 1) === 4)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1, 2))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 3, 2))
    assert(SimpleExternalLookupDataSource.nullKeyRequests.get() === 0)
  }

  test("external lookup join supports null-safe lookup keys") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
      .hint("external_lookup")
    val left = Seq[java.lang.Integer](1, null, 3).toDF("id").coalesce(1)

    checkAnswer(
      left.join(lookup, left("id") <=> lookup("id"), "left_outer"),
      Seq(
        Row(1, 1, "engineering"),
        Row(1, 1, "research"),
        Row(null, null, "unknown"),
        Row(3, null, null)))
    assert(SimpleExternalLookupDataSource.nullKeyRequests.get() > 0)
  }

  test("external lookup join supports complete composite equi-keys") {
    val lookup = spark.read
      .format(classOf[CompositeExternalLookupDataSource].getName)
      .load()
      .hint("external_lookup")
    val left = Seq((1, 1), (1, 2), (2, 1)).toDF("tenant", "id")

    checkAnswer(
      left.join(
        lookup,
        left("tenant") === lookup("tenant") && left("id") === lookup("id"),
        "left_outer"),
      Seq(Row(1, 1, 1, 1, "one-one"), Row(1, 2, null, null, null),
        Row(2, 1, 2, 1, "two-one")))
  }

  test("external lookup combines equality and range predicates in one access path") {
    CompositeExternalLookupDataSource.selectedLookupPredicates.set(Seq.empty)
    val lookup = spark.read
      .format(classOf[CompositeExternalLookupDataSource].getName)
      .load()
    val left = Seq(
      (1, 1, 3),
      (1, 2, 4),
      (2, 0, 2),
      (3, 0, 10)).toDF("tenant", "start", "end")
    val condition = lookup("tenant") === left("tenant") &&
      lookup("id") >= left("start") &&
      lookup("id") < left("end")

    checkAnswer(
      left.join(lookup, condition, "inner"),
      Seq(
        Row(1, 1, 3, 1, 1, "one-one"),
        Row(1, 2, 4, 1, 3, "one-three"),
        Row(2, 0, 2, 2, 1, "two-one")))
    assert(CompositeExternalLookupDataSource.selectedLookupPredicates.get() === Seq(
      ("=", "tenant"),
      (">=", "id"),
      ("<", "id")))
  }

  test("external lookup join supports multiple range predicates on one field") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
    val left = Seq((1, 2), (1, 3)).toDF("start", "end")

    checkAnswer(
      left.join(
        lookup,
        lookup("id") >= left("start") && lookup("id") < left("end"),
        "inner"),
      Seq(
        Row(1, 2, 1, "engineering"),
        Row(1, 2, 1, "research"),
        Row(1, 3, 1, "engineering"),
        Row(1, 3, 1, "research"),
        Row(1, 3, 2, "sales")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq((">=", "id", IntegerType), ("<", "id", IntegerType)))

    checkAnswer(
      left.join(
        lookup,
        left("start") < lookup("id") && left("end") >= lookup("id"),
        "inner"),
      Seq(
        Row(1, 2, 2, "sales"),
        Row(1, 3, 2, "sales")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() ===
      Seq((">", "id", IntegerType), ("<=", "id", IntegerType)))
  }

  test("external lookup leaves unselected criteria as Spark residuals") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
    val left = Seq(
      (1, 3, "sales"),
      (1, 3, "research"),
      (1, 2, "missing")).toDF("start", "end", "department")
    val condition = lookup("id") >= left("start") &&
      lookup("id") < left("end") &&
      lookup("department") === left("department")

    checkAnswer(
      left.join(lookup, condition, "left_semi"),
      Seq(Row(1, 3, "sales"), Row(1, 3, "research")))
    assert(SimpleExternalLookupDataSource.lookupCandidates.get() === Seq(
      (">=", "id", IntegerType),
      ("<", "id", IntegerType),
      ("=", "department", StringType)))
    assert(SimpleExternalLookupDataSource.requiredColumns.get() === Seq("department"))
  }

  test("external lookup cache can be disabled without disabling request deduplication") {
    def run(cacheHint: Seq[Any]): Int = {
      SimpleExternalLookupDataSource.requestCount.set(0)
      val lookup = spark.read
        .format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", "1")
        .load()
      val hinted = lookup.hint("external_lookup", cacheHint: _*)
      val left = Seq(1, 2, 1).toDF("id").coalesce(1)
      checkAnswer(
        left.join(hinted, left("id") === hinted("id"), "inner"),
        Seq(Row(1, 1, "engineering"), Row(1, 1, "research"), Row(2, 2, "sales"),
          Row(1, 1, "engineering"), Row(1, 1, "research")))
      SimpleExternalLookupDataSource.requestCount.get()
    }

    val cachedRequests = run(Nil)
    val uncachedRequests = run(Seq("cache_enabled=false"))
    assert(uncachedRequests > cachedRequests)
    assert(uncachedRequests * 2 === cachedRequests * 3)
  }

  test("external lookup cache uses SQLConf defaults and hint overrides") {
    def run(hintParameters: Seq[Any]): Int = {
      SimpleExternalLookupDataSource.resetCounters()
      val lookup = spark.read
        .format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", "1")
        .load()
      val hinted = if (hintParameters.isEmpty) lookup else {
        lookup.hint("external_lookup", hintParameters: _*)
      }
      val left = Seq(1, 1).toDF("id").coalesce(1)
      left.join(hinted, left("id") === hinted("id"), "inner").collect()
      SimpleExternalLookupDataSource.requestCount.get()
    }

    withSQLConf(SQLConf.JOIN_EXTERNAL_LOOKUP_CACHE_ENABLED.key -> "false") {
      assert(run(Nil) === 2)
      assert(run(Seq("cache_enabled=true")) === 1)
    }
  }

  test("external lookup cache controls freshness and caches misses") {
    def run(keys: Seq[Int], cacheDisabled: Boolean, mutate: Boolean): (Seq[Row], Int) = {
      SimpleExternalLookupDataSource.resetCounters()
      val unhintedLookup = spark.read
        .format(classOf[SimpleExternalLookupDataSource].getName)
        .option("maxBatchSize", "1")
        .option("mutateAfterRequest", mutate.toString)
        .load()
      val hintParameters: Seq[Any] = if (cacheDisabled) Seq("cache_enabled=false") else Nil
      val lookup = unhintedLookup.hint("external_lookup", hintParameters: _*)
      val left = keys.toDF("id").coalesce(1)
      val rows = left.join(lookup, left("id") === lookup("id"), "left_outer").collect().toSeq
      rows -> SimpleExternalLookupDataSource.requestCount.get()
    }

    val (cachedRows, cachedRequests) = run(Seq(2, 2), cacheDisabled = false, mutate = true)
    assert(cachedRows === Seq(Row(2, 2, "sales"), Row(2, 2, "sales")))
    assert(cachedRequests === 1)

    val (freshRows, freshRequests) = run(Seq(2, 2), cacheDisabled = true, mutate = true)
    assert(freshRows === Seq(Row(2, 2, "sales"), Row(2, 2, "sales-v2")))
    assert(freshRequests === 2)

    val (missingRows, missingRequests) = run(Seq(3, 3), cacheDisabled = false, mutate = false)
    assert(missingRows === Seq(Row(3, null, null), Row(3, null, null)))
    assert(missingRequests === 1)
  }

  test("external lookup cache TTL expires entries") {
    SimpleExternalLookupDataSource.resetCounters()
    val clock = new AtomicLong(0L)
    val expiredAt = TimeUnit.MILLISECONDS.toNanos(2)
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup", "cache_ttl=1ms")
    val left = Seq(2, 2).toDF("id").coalesce(1)

    ExternalLookupJoinExec.withCacheTimeNanos { () =>
      val now = clock.get()
      clock.set(expiredAt)
      now
    } {
      assert(left.join(lookup, left("id") === lookup("id"), "left_outer").count() === 2)
    }
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 2))
  }

  test("a cache entry expiring between duplicate keys in one batch is looked up once") {
    SimpleExternalLookupDataSource.resetCounters()
    val calls = new AtomicInteger(0)
    val expired = TimeUnit.MILLISECONDS.toNanos(2)
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "2")
      .load()
      .hint("external_lookup", "cache_ttl=1ms")
    // Batches [2, 1] and [2, 2]. The clock is read when the first batch writes the cache and on
    // each cache probe; it expires the entry for key 2 between the two rows of the second batch.
    val left = Seq(2, 1, 2, 2).toDF("id").coalesce(1)

    val rows = ExternalLookupJoinExec.withCacheTimeNanos { () =>
      if (calls.incrementAndGet() <= 2) 0L else expired
    } {
      left.join(lookup, left("id") === lookup("id"), "inner").collect().toSeq
    }
    assert(rows.map(_.getInt(0)).sorted === Seq(1, 1, 2, 2, 2))
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 1))
  }

  test("external lookup reports cache metrics") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup")
    val left = Seq(1, 1).toDF("id").coalesce(1)
    val query = left.join(lookup, left("id") === lookup("id"), "inner")

    query.collect()
    val operator = query.queryExecution.executedPlan.collectFirst {
      case externalLookup: ExternalLookupJoinExec => externalLookup
    }.get
    assert(operator.metrics("numInputRows").value === 2)
    assert(operator.metrics("numInputBatches").value === 2)
    assert(operator.metrics("numLookupRequests").value === 1)
    assert(operator.metrics("numCacheHits").value === 1)
    assert(operator.metrics("batchFillTime").value >= 0)
    assert(operator.metrics("lookupTime").value >= 0)
  }

  test("external lookup cache evicts least recently used entries at the configured bound") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "1")
      .load()
      .hint("external_lookup", "cache_max_entries=1")
    val left = Seq(1, 2, 1).toDF("id").coalesce(1)

    left.join(lookup, left("id") === lookup("id"), "left_outer").collect()
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(1, 2, 1))
  }

  test("an upstream limit stops one-to-many cursor consumption") {
    SimpleExternalLookupDataSource.resetCounters()
    SimpleExternalLookupDataSource.values.set(Map(1 -> (1 to 2000).map(_.toString)))
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id")

    assert(left.join(
      lookup.hint("external_lookup"), left("id") === lookup("id"), "inner")
      .limit(1).count() === 1)
    assert(SimpleExternalLookupDataSource.resultRowsRead.get() === 1)
  }

  test("oversized one-to-many results are streamed but not cached") {
    SimpleExternalLookupDataSource.resetCounters()
    SimpleExternalLookupDataSource.values.set(Map(1 -> Seq("engineering", "research")))
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", 1)
      .load()
      .hint("external_lookup", "cache_max_rows_per_key=1")
    val left = Seq(1, 1).toDF("id").coalesce(1)

    assert(left.join(
      lookup, left("id") === lookup("id"), "inner").count() === 4)
    assert(SimpleExternalLookupDataSource.requestCount.get() === 2)
  }

  test("external lookup samples connector metrics while consuming a large response") {
    SimpleExternalLookupDataSource.resetCounters()
    SimpleExternalLookupDataSource.values.set(Map(1 -> (1 to 250).map(_.toString)))
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = Seq(1).toDF("id").coalesce(1)

    assert(left.join(
      lookup.hint("external_lookup"), left("id") === lookup("id"), "inner").count() === 250)
    // Two periodic samples, followed by request-completion and reader-close samples.
    assert(SimpleExternalLookupDataSource.metricSamples.get() >= 4)
  }

  test("external lookup deduplicates floating-point keys using Spark equality") {
    SemanticKeyExternalLookupDataSource.requestSizes.clear()
    val nan1 = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
    val nan2 = java.lang.Double.longBitsToDouble(0x7ff8000000000002L)
    // Put equivalent keys in different input batches to exercise cache identity as well.
    val left = Seq(-0.0d, nan1, 0.0d, nan2).toDF("key").coalesce(1)
    val lookup = semanticLookup("double")

    assert(left.join(lookup, left("key") === lookup("key")).count() === 4)
    assert(SemanticKeyExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(2))
  }

  test("external lookup deduplicates keys using Spark collation equality") {
    SemanticKeyExternalLookupDataSource.requestSizes.clear()
    val left = Seq("lookup", "LOOKUP").toDF("key")
      .selectExpr("CAST(key AS STRING COLLATE UTF8_LCASE) AS key")
      .coalesce(1)
    val lookup = semanticLookup("string collate utf8_lcase")

    assert(left.join(lookup, left("key") === lookup("key")).count() === 2)
    assert(SemanticKeyExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1))
  }

  private def semanticLookup(keyType: String): DataFrame = {
    spark.read
      .format(classOf[SemanticKeyExternalLookupDataSource].getName)
      .option("keyType", keyType)
      .load()
      .hint("external_lookup")
  }

  test("external lookup closes task-local readers after success and failure") {
    def lookup(options: (String, String)*): DataFrame = {
      var reader = spark.read.format(classOf[SimpleExternalLookupDataSource].getName)
      options.foreach { case (key, value) => reader = reader.option(key, value) }
      reader.load().hint("external_lookup")
    }
    val left = Seq(1).toDF("id").coalesce(1)

    SimpleExternalLookupDataSource.resetCounters()
    val successfulLookup = lookup()
    left.join(successfulLookup, left("id") === successfulLookup("id")).collect()
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    assert(SimpleExternalLookupDataSource.closeCount.get() === 1)

    SimpleExternalLookupDataSource.resetCounters()
    val failingLookup = lookup("failRequests" -> "true")
    val failure = intercept[SparkException] {
      left.join(failingLookup, left("id") === failingLookup("id")).collect()
    }
    assert(failure.getMessage.contains("injected external lookup failure"))
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    assert(SimpleExternalLookupDataSource.closeCount.get() === 1)
  }

  test("external lookup failures propagate from outer joins instead of becoming misses") {
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("failRequests", "true")
      .load()
    val left = Seq(1, 3).toDF("id")

    Seq("left_outer", "left_anti").foreach { joinType =>
      SimpleExternalLookupDataSource.resetCounters()
      val failure = intercept[SparkException] {
        left.join(lookup, left("id") === lookup("id"), joinType).collect()
      }
      assert(failure.getMessage.contains("injected external lookup failure"))
    }
  }

  test("external lookup bounds each request by the engine batch size") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("maxBatchSize", "5000")
      .load()
      .hint("external_lookup", "cache_enabled=false")
    val left = (1 to 1500).toDF("id").coalesce(1)

    assert(left.join(lookup, left("id") === lookup("id"), "left_semi").count() === 2)
    assert(SimpleExternalLookupDataSource.requestSizes.asScala.toSeq === Seq(1000, 500))
  }

  test("external lookup cache is scoped to a task") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val left = spark.sparkContext.parallelize(Seq(2, 2, 2, 2), numSlices = 2).toDF("id")

    assert(left.join(lookup, left("id") === lookup("id")).count() === 4)
    assert(SimpleExternalLookupDataSource.readerCount.get() === 2)
    assert(SimpleExternalLookupDataSource.requestedKeys.asScala.toSeq === Seq(2, 2))
  }

  test("external lookup does not create readers without requestable keys") {
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val empty = Seq.empty[java.lang.Integer].toDF("id").coalesce(1)
    val nullOnly = Seq[java.lang.Integer](null).toDF("id").coalesce(1)

    Seq(empty, nullOnly).foreach { left =>
      SimpleExternalLookupDataSource.resetCounters()
      left.join(
        lookup.hint("external_lookup"), left("id") === lookup("id"), "left_outer").collect()
      assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
      assert(SimpleExternalLookupDataSource.closeCount.get() === 0)
    }
  }

  test("external lookup cleanup attempts every resource and preserves the primary failure") {
    val attempted = mutable.ArrayBuffer.empty[String]
    val cursorFailure = new IllegalStateException("cursor close")
    val metricsFailure = new IllegalStateException("metrics")
    val readerFailure = new IllegalStateException("reader close")

    val failure = intercept[IllegalStateException] {
      ExternalLookupJoinExec.closeResources(
        { attempted += "cursor"; throw cursorFailure },
        { attempted += "metrics"; throw metricsFailure },
        { attempted += "reader"; throw readerFailure })
    }
    assert(failure eq cursorFailure)
    assert(attempted === Seq("cursor", "metrics", "reader"))
    assert(failure.getSuppressed.toSeq === Seq(metricsFailure))
    assert(metricsFailure.getSuppressed.toSeq === Seq(readerFailure))

    val lookupFailure = new IllegalStateException("lookup")
    val cleanupFailure = new IllegalStateException("cleanup")
    val preserved = intercept[IllegalStateException] {
      org.apache.spark.util.Utils.tryWithSafeFinally(throw lookupFailure) {
        ExternalLookupJoinExec.closeResources(throw cleanupFailure, (), ())
      }
    }
    assert(preserved eq lookupFailure)
    assert(preserved.getSuppressed.toSeq === Seq(cleanupFailure))
  }

  test("external lookup responds to task cancellation and closes the reader") {
    SimpleExternalLookupDataSource.resetCounters()
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .option("responseDelayMillis", "60000")
      .load()
      .hint("external_lookup")
    val left = Seq(1).toDF("id").coalesce(1)
    val groupId = "external-lookup-cancellation"
    val executor = ThreadUtils.newDaemonSingleThreadExecutor(groupId)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val query = Future {
      spark.sparkContext.setJobGroup(groupId, groupId, interruptOnCancel = true)
      left.join(lookup, left("id") === lookup("id")).collect()
    }

    assert(SimpleExternalLookupDataSource.requestStarted.get().await(10, TimeUnit.SECONDS))
    spark.sparkContext.cancelJobGroup(groupId)
    intercept[Exception] { ThreadUtils.awaitResult(query, 10.seconds) }
    executor.shutdownNow()
    assert(SimpleExternalLookupDataSource.readerCount.get() === 1)
    eventually(timeout(10.seconds)) {
      assert(SimpleExternalLookupDataSource.closeCount.get() === 1)
    }
  }

  test("micro-batch stream-static joins follow the same lookup selection") {
    val dual = spark.read.format(classOf[DualCapabilityExternalLookupDataSource].getName).load()
    Seq(false, true).foreach { hinted =>
      SimpleExternalLookupDataSource.resetCounters()
      val input = MemoryStream[Int]
      val left = input.toDF().toDF("id")
      val lookup = if (hinted) dual.hint("external_lookup") else dual
      val queryName = s"external_lookup_dual_mbm_$hinted"
      val query = left.join(lookup, left("id") === lookup("id"))
        .writeStream.format("memory").queryName(queryName).start()
      try {
        input.addData(2)
        query.processAllAvailable()
        checkAnswer(spark.table(queryName), Seq(Row(2, 2, "sales")))
        val usesLookup = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
          .lastExecution.executedPlan.exists(_.isInstanceOf[ExternalLookupJoinExec])
        assert(usesLookup === hinted)
        assert((SimpleExternalLookupDataSource.requestCount.get() > 0) === hinted)
      } finally {
        query.stop()
        spark.catalog.dropTempView(queryName)
      }
    }
  }

  test("external lookup join runs unchanged in micro-batch streaming") {
    val input = MemoryStream[Int]
    val left = input.toDF().toDF("id")
    val lookup = spark.read
      .format(classOf[SimpleExternalLookupDataSource].getName)
      .load()
      .hint("external_lookup")
    val joined = left.join(lookup, left("id") === lookup("id"), "left_outer")
    val queryName = "external_lookup_mbm"
    val query = joined.writeStream.format("memory").queryName(queryName).start()
    try {
      input.addData(1, 2, 3)
      query.processAllAvailable()
      checkAnswer(
        spark.table(queryName),
        Seq(Row(1, 1, "engineering"), Row(1, 1, "research"),
          Row(2, 2, "sales"), Row(3, null, null)))
    } finally {
      query.stop()
      spark.catalog.dropTempView(queryName)
    }
  }
}

/** A catalog serving `ns.lookup` (lookup-only) and `ns.dual` (dual-capability). */
class ExternalLookupTestCatalog extends TableCatalog {
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    catalogName = name
  override def name(): String = catalogName
  override def listTables(namespace: Array[String]): Array[Identifier] =
    Array("lookup", "dual").map(Identifier.of(Array("ns"), _))
  override def loadTable(ident: Identifier): Table = (ident.namespace().toSeq, ident.name()) match {
    case (Seq("ns"), "lookup") => SimpleExternalLookupTable
    case (Seq("ns"), "dual") => DualCapabilityExternalLookupTable
    case _ => throw new NoSuchTableException(ident.namespace().mkString("."), ident.name())
  }
  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException()
  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException()
}

class ExternalLookupJoinRealTimeModeSuite extends StreamRealTimeModeE2ESuiteBase {
  import testImplicits._

  test("real-time mode rejects external lookup join") {
    SimpleExternalLookupDataSource.resetCounters()
    val input = LowLatencyMemoryStream[Int](2)
    val left = input.toDF().toDF("id")
    val lookup = spark.read.format(classOf[SimpleExternalLookupDataSource].getName).load()
    val joined = left.join(lookup, left("id") === lookup("id"))
      .select(concat(col("department"), lit("-"), left("id").cast("string")).as("output"))

    val query = runStreamingQuery("external_lookup_rtm", joined)
    try {
      eventually(timeout(60.seconds)) {
        checkError(
          exception = query.exception.get.getCause.asInstanceOf[SparkIllegalArgumentException],
          condition = "STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST",
          parameters = Map(
            "errorType" -> "operator",
            "message" -> "org.apache.spark.sql.execution.joins.ExternalLookupJoinExec is"))
      }
    } finally {
      query.stop()
    }
    assert(SimpleExternalLookupDataSource.readerCount.get() === 0)
  }
}

class SimpleExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SimpleExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = SimpleExternalLookupTable
}

class DualCapabilityExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SimpleExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = DualCapabilityExternalLookupTable
}

object DualCapabilityExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "DualCapabilityExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(BATCH_READ, EXTERNAL_LOOKUP)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    SimpleExternalLookupTable.newScanBuilder(options)
}

object SimpleExternalLookupDataSource {
  val requestCount = new AtomicInteger()
  val readerCount = new AtomicInteger()
  val closeCount = new AtomicInteger()
  val requestStarted = new AtomicReference(new CountDownLatch(1))
  val requiredColumns = new AtomicReference(Seq.empty[String])
  val pushedPredicateCount = new AtomicInteger()
  val pushedPredicateFields = new AtomicReference(Seq.empty[String])
  val requestSizes = new ConcurrentLinkedQueue[Int]()
  val requestedKeys = new ConcurrentLinkedQueue[Int]()
  val nullKeyRequests = new AtomicInteger(0)
  val resultRowsRead = new AtomicInteger(0)
  val metricSamples = new AtomicInteger(0)
  val lookupCandidates =
    new AtomicReference(Seq.empty[(String, String, DataType)])
  val lookupCandidateDescriptions = new AtomicReference(Seq.empty[String])
  val keySchema: StructType = new StructType().add("id", "int", nullable = true)
  val outputSchema: StructType = keySchema.add("department", "string", nullable = false)
  private val InitialValues: Map[Int, Seq[String]] = Map(
    1 -> Seq("engineering", "research"),
    2 -> Seq("sales"))
  val values = new AtomicReference(InitialValues)

  def resetCounters(): Unit = {
    requestCount.set(0)
    readerCount.set(0)
    closeCount.set(0)
    requestStarted.set(new CountDownLatch(1))
    requiredColumns.set(Seq.empty)
    pushedPredicateCount.set(0)
    pushedPredicateFields.set(Seq.empty)
    requestSizes.clear()
    requestedKeys.clear()
    nullKeyRequests.set(0)
    resultRowsRead.set(0)
    metricSamples.set(0)
    lookupCandidates.set(Seq.empty)
    lookupCandidateDescriptions.set(Seq.empty)
    values.set(InitialValues)
  }
}

object SimpleExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "SimpleExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val configuredMaxBatchSize = options.getInt("maxBatchSize", 100)
    val responseDelayMillis = options.getLong("responseDelayMillis", 0)
    val failRequests = options.getBoolean("failRequests", false)
    val mutateAfterRequest = options.getBoolean("mutateAfterRequest", false)
    val nullKeySelection = options.getBoolean("nullKeySelection", false)
    val selectionMode = options.getOrDefault("selectionMode", "all")
    val nullCursor = options.getBoolean("nullCursor", false)
    new SupportsExternalLookup
      with SupportsPushDownRequiredColumns
      with SupportsPushDownV2Filters {
      private var requiredSchema = SimpleExternalLookupDataSource.outputSchema
      private var excludeResearch = false
      private var acceptedPredicates = Array.empty[Predicate]
      private var selectedLookupPredicates = Seq.empty[String]

      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] = {
        if (nullKeySelection) return null
        SimpleExternalLookupDataSource.lookupCandidates.set(candidates.map { candidate =>
          val field = candidate.children()(0).asInstanceOf[NamedReference]
          val parameter = candidate.children()(1).asInstanceOf[LookupParameter]
          (candidate.name(), field.fieldNames().mkString("."), parameter.dataType())
        }.toSeq)
        SimpleExternalLookupDataSource.lookupCandidateDescriptions.set(
          candidates.map(_.toString).toSeq)
        val supportedPredicates = Set("=", "<=>", "<", "<=", ">", ">=")
        val supported = candidates.filter { candidate =>
          supportedPredicates.contains(candidate.name()) &&
            candidate.children()(0).asInstanceOf[NamedReference]
              .fieldNames().sameElements(Array("id"))
        }
        val selected = selectionMode match {
          case "all" => supported
          case "last" => supported.takeRight(1)
          case "duplicate" => supported.take(1) ++ supported.take(1)
          // An equal-shaped predicate that is not one of the offered instances.
          case "copy" => supported.take(1).map(p => new Predicate(p.name(), p.children()))
          case "nullElement" => Array[Predicate](null)
        }
        selectedLookupPredicates = selected.filter(_ != null).map(_.name()).toSeq
        selected
      }

      override def pruneColumns(schema: StructType): Unit = {
        requiredSchema = schema
        SimpleExternalLookupDataSource.requiredColumns.set(schema.fieldNames.toSeq)
      }
      override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
        def fields(predicate: Predicate): Seq[NamedReference] =
          predicate.children().toSeq.flatMap {
            case reference: NamedReference => Seq(reference)
            case child: Predicate => fields(child)
            case _ => Seq.empty
          }
        val (accepted, rejected) = predicates.partition { predicate =>
          fields(predicate).exists(_.fieldNames().sameElements(Array("department")))
        }
        if (accepted.nonEmpty) {
          acceptedPredicates = accepted
          excludeResearch = true
          SimpleExternalLookupDataSource.pushedPredicateCount.addAndGet(accepted.length)
          SimpleExternalLookupDataSource.pushedPredicateFields.set(
            accepted.flatMap(fields).map(_.fieldNames().mkString(".")).toSeq)
        }
        rejected
      }
      override def pushedPredicates(): Array[Predicate] = acceptedPredicates
      override def build(): Scan = SimpleExternalLookupScan(
        configuredMaxBatchSize, responseDelayMillis, failRequests,
        mutateAfterRequest, nullCursor, requiredSchema, excludeResearch,
        selectedLookupPredicates)
    }
  }
}

case class SimpleExternalLookupScan(
    configuredMaxBatchSize: Int,
    responseDelayMillis: Long,
    failRequests: Boolean,
    mutateAfterRequest: Boolean,
    nullCursor: Boolean,
    requiredSchema: StructType,
    excludeResearch: Boolean,
    selectedLookupPredicates: Seq[String]) extends Scan {

  override def readSchema(): StructType = requiredSchema
  override def toBatch(): Batch =
    SimpleExternalLookupBatch(requiredSchema.fieldNames.toSeq, excludeResearch)
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(configuredMaxBatchSize, SimpleExternalLookupReaderFactory(
      responseDelayMillis,
      failRequests,
      mutateAfterRequest,
      nullCursor,
      requiredSchema.fieldNames.toSeq,
      excludeResearch,
      selectedLookupPredicates))
}

case class SimpleExternalLookupBatch(
    requiredColumns: Seq[String],
    excludeResearch: Boolean) extends Batch {
  override def planInputPartitions(): Array[InputPartition] =
    Array(SimpleExternalLookupInputPartition)

  override def createReaderFactory(): PartitionReaderFactory =
    SimpleExternalLookupBatchReaderFactory(requiredColumns, excludeResearch)
}

case object SimpleExternalLookupInputPartition extends InputPartition

case class SimpleExternalLookupBatchReaderFactory(
    requiredColumns: Seq[String],
    excludeResearch: Boolean) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new PartitionReader[InternalRow] {
      private val rows = SimpleExternalLookupDataSource.values.get().toSeq
        .sortBy(_._1)
        .iterator
        .flatMap { case (id, departments) =>
          departments.iterator
            .filterNot(department => excludeResearch && department == "research")
            .map { department =>
              InternalRow.fromSeq(requiredColumns.map {
                case "id" => Int.box(id)
                case "department" => UTF8String.fromString(department)
              })
            }
        }
      private var current: InternalRow = _

      override def next(): Boolean = {
        if (rows.hasNext) {
          current = rows.next()
          true
        } else {
          false
        }
      }

      override def get(): InternalRow = current

      override def close(): Unit = {}
    }
}

case class SimpleExternalLookupReaderFactory(
    responseDelayMillis: Long,
    failRequests: Boolean,
    mutateAfterRequest: Boolean,
    nullCursor: Boolean,
    requiredColumns: Seq[String],
    excludeResearch: Boolean,
    selectedLookupPredicates: Seq[String]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    SimpleExternalLookupDataSource.readerCount.incrementAndGet()

    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      SimpleExternalLookupDataSource.requestStarted.get().countDown()
      if (responseDelayMillis > 0) Thread.sleep(responseDelayMillis)
      if (failRequests) throw new IllegalStateException("injected external lookup failure")
      if (nullCursor) return null
      SimpleExternalLookupDataSource.requestCount.incrementAndGet()
      SimpleExternalLookupDataSource.requestSizes.add(keys.length)
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        if (key.isNullAt(0)) {
          SimpleExternalLookupDataSource.nullKeyRequests.incrementAndGet()
        } else {
          val value = key.getInt(0)
          SimpleExternalLookupDataSource.requestedKeys.add(value)
        }
        val values = SimpleExternalLookupDataSource.values.get().toSeq.map { case (id, values) =>
          Some(id) -> values
        } :+ (None -> Seq("unknown"))
        values.iterator.filter { case (id, _) =>
          selectedLookupPredicates.zipWithIndex.forall { case (predicate, index) =>
            val parameter = if (key.isNullAt(index)) None else Some(key.getInt(index))
            predicate match {
              case "=" => id.nonEmpty && parameter.nonEmpty && id == parameter
              case "<=>" => id == parameter
              case "<" => id.nonEmpty && parameter.nonEmpty && id.get < parameter.get
              case "<=" => id.nonEmpty && parameter.nonEmpty && id.get <= parameter.get
              case ">" => id.nonEmpty && parameter.nonEmpty && id.get > parameter.get
              case ">=" => id.nonEmpty && parameter.nonEmpty && id.get >= parameter.get
            }
          }
        }.flatMap { case (id, departments) => departments.iterator.map(id -> _) }
          .filterNot { case (_, department) => excludeResearch && department == "research" }
          .map { case (id, department) =>
          ordinal -> InternalRow.fromSeq(requiredColumns.map {
              case "id" => id.map(Int.box).orNull
              case "department" => UTF8String.fromString(department)
            })
        }
      }.toArray.iterator
      if (mutateAfterRequest) {
        SimpleExternalLookupDataSource.values.updateAndGet { current =>
          current.updated(2, Seq("sales-v2"))
        }
      }
      new TestExternalLookupResultCursor(
        results, () => SimpleExternalLookupDataSource.resultRowsRead.incrementAndGet())
    }

    override def currentMetricsValues():
        Array[org.apache.spark.sql.connector.metric.CustomTaskMetric] = {
      SimpleExternalLookupDataSource.metricSamples.incrementAndGet()
      Array.empty
    }

    override def close(): Unit = SimpleExternalLookupDataSource.closeCount.incrementAndGet()
  }
}

class CompositeExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    CompositeExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = CompositeExternalLookupTable
}

object CompositeExternalLookupDataSource {
  val keySchema: StructType = new StructType()
    .add("tenant", "int", nullable = false)
    .add("id", "int", nullable = false)
  val outputSchema: StructType = keySchema.add("value", "string", nullable = false)
  val values: Map[(Int, Int), String] = Map(
    (1, 1) -> "one-one",
    (1, 3) -> "one-three",
    (2, 1) -> "two-one")
  val selectedLookupPredicates =
    new AtomicReference(Seq.empty[(String, String)])
}

object CompositeExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "CompositeExternalLookupTable"
  override def schema(): StructType = CompositeExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup with SupportsPushDownRequiredColumns {
    private var requiredSchema = CompositeExternalLookupDataSource.outputSchema
    private var selectedLookupPredicates = Seq.empty[(String, String)]

    override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] = {
      val supported = candidates.filter { predicate =>
        val field = predicate.children()(0).asInstanceOf[NamedReference]
          .fieldNames().mkString(".")
        (field == "tenant" && predicate.name() == "=") ||
          (field == "id" && Set("=", "<", "<=", ">", ">=").contains(predicate.name()))
      }
      val selected = supported.map { predicate =>
        predicate.name() -> predicate.children()(0).asInstanceOf[NamedReference]
          .fieldNames().mkString(".")
      }.toSeq
      if (selected.contains("=" -> "tenant") && selected.exists(_._2 == "id")) {
        selectedLookupPredicates = selected
        CompositeExternalLookupDataSource.selectedLookupPredicates.set(selected)
        supported
      } else {
        Array.empty
      }
    }

    override def pruneColumns(schema: StructType): Unit = requiredSchema = schema
    override def build(): Scan =
      CompositeExternalLookupScan(requiredSchema, selectedLookupPredicates)
  }
}

case class CompositeExternalLookupScan(
    requiredSchema: StructType,
    selectedLookupPredicates: Seq[(String, String)]) extends Scan {
  override def readSchema(): StructType = requiredSchema
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(
      100,
      CompositeExternalLookupReaderFactory(requiredSchema, selectedLookupPredicates))
}

case class CompositeExternalLookupReaderFactory(
    requiredSchema: StructType,
    selectedLookupPredicates: Seq[(String, String)]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      val results = keys.iterator.zipWithIndex.flatMap { case (key, ordinal) =>
        CompositeExternalLookupDataSource.values.iterator.filter { case ((tenant, id), _) =>
          selectedLookupPredicates.zipWithIndex.forall {
            case ((predicate, field), index) =>
              val lookupValue = if (field == "tenant") tenant else id
              val parameter = key.getInt(index)
              predicate match {
                case "=" => lookupValue == parameter
                case "<" => lookupValue < parameter
                case "<=" => lookupValue <= parameter
                case ">" => lookupValue > parameter
                case ">=" => lookupValue >= parameter
              }
          }
        }.map { case ((tenant, id), lookupValue) =>
          ordinal -> InternalRow.fromSeq(requiredSchema.fieldNames.toSeq.map {
            case "tenant" => Int.box(tenant)
            case "id" => Int.box(id)
            case "value" => UTF8String.fromString(lookupValue)
          })
        }
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class TestExternalLookupResultCursor(
    results: Iterator[(Int, InternalRow)],
    onNext: () => Unit = () => ())
    extends ExternalLookupResultCursor {
  private var current: (Int, InternalRow) = _

  override def next(): Boolean = if (results.hasNext) {
    current = results.next()
    onNext()
    true
  } else {
    current = null
    false
  }

  override def get(): InternalRow = current._2
  override def inputOrdinal(): Int = current._1
  override def close(): Unit = {}
}

class SemanticKeyExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SemanticKeyExternalLookupDataSource.schema(options.get("keyType"))

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    SemanticKeyExternalLookupTable(options.get("keyType"))
  }
}

object SemanticKeyExternalLookupDataSource {
  val requestSizes = new ConcurrentLinkedQueue[Int]()

  def schema(keyType: String): StructType = new StructType()
    .add("key", DataType.fromDDL(keyType), nullable = false)
    .add("payload", "string", nullable = false)
}

case class SemanticKeyExternalLookupTable(keyType: String) extends Table with SupportsRead {
  override def name(): String = "SemanticKeyExternalLookupTable"
  override def schema(): StructType = SemanticKeyExternalLookupDataSource.schema(keyType)
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup with SupportsPushDownRequiredColumns {
      private var requiredSchema = schema()

      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter { candidate =>
          candidate.name() == "=" &&
            candidate.children()(0).asInstanceOf[NamedReference]
              .fieldNames().sameElements(Array("key"))
        }.take(1)

      override def pruneColumns(schema: StructType): Unit = requiredSchema = schema
      override def build(): Scan = SemanticKeyExternalLookupScan(
        DataType.fromDDL(keyType), requiredSchema)
    }
}

case class SemanticKeyExternalLookupScan(
    keyType: DataType,
    requiredSchema: StructType) extends Scan {
  override def readSchema(): StructType = requiredSchema
  override def toExternalLookup(): ExternalLookup =
    TestExternalLookup(2, SemanticKeyExternalLookupReaderFactory(keyType, requiredSchema))
}

case class SemanticKeyExternalLookupReaderFactory(
    keyType: DataType,
    requiredSchema: StructType) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor = {
      SemanticKeyExternalLookupDataSource.requestSizes.add(keys.length)
      val results = keys.iterator.zipWithIndex.map { case (key, ordinal) =>
        val value = key.get(0, keyType)
        ordinal -> InternalRow.fromSeq(requiredSchema.fieldNames.toSeq.map {
          case "key" => value
          case "payload" => UTF8String.fromString("value")
        })
      }
      new TestExternalLookupResultCursor(results)
    }

    override def close(): Unit = {}
  }
}

class MisconfiguredExternalLookupDataSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    SimpleExternalLookupDataSource.outputSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table =
    MisconfiguredExternalLookupTable(new CaseInsensitiveStringMap(properties).get("kind"))
}

case class MisconfiguredExternalLookupTable(kind: String) extends Table with SupportsRead {
  override def name(): String = "MisconfiguredExternalLookupTable"
  override def schema(): StructType = SimpleExternalLookupDataSource.outputSchema
  override def capabilities(): java.util.Set[TableCapability] = kind match {
    case "batchOnly" => java.util.EnumSet.of(BATCH_READ)
    case _ => java.util.EnumSet.of(EXTERNAL_LOOKUP)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = kind match {
    case "batchOnly" | "noLookupBuilder" => () =>
      SimpleExternalLookupScan(100, 0, failRequests = false, mutateAfterRequest = false,
        nullCursor = false, schema(), excludeResearch = false, Seq("="))
    case _ => new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.take(1)
      override def build(): Scan = kind match {
        case "unknownField" => NoExternalLookupScan(new StructType().add("unknown", "int"))
        case "duplicateField" =>
          NoExternalLookupScan(new StructType().add("id", "int").add("id", "int"))
        case "noExternalLookup" => NoExternalLookupScan(schema())
        case "invalidOrdinal" => new Scan {
          override def readSchema(): StructType = schema()
          override def toExternalLookup(): ExternalLookup =
            TestExternalLookup(10, InvalidOrdinalExternalLookupReaderFactory)
        }
      }
    }
  }
}

case class NoExternalLookupScan(readSchema: StructType) extends Scan {
  override def description(): String = "NoExternalLookupScan"
}

object InvalidOrdinalExternalLookupReaderFactory extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new ExternalLookupReader {
    override def lookupBatch(keys: Array[InternalRow]): ExternalLookupResultCursor =
      new TestExternalLookupResultCursor(
        Iterator(5 -> InternalRow(1, UTF8String.fromString("value"))))
    override def close(): Unit = {}
  }
}

case class TestExternalLookup(
    maxRequestSize: Int,
    factory: ExternalLookupReaderFactory) extends ExternalLookup {
  override def maxParameterRowsPerRequest(): Int = maxRequestSize
  override def createReaderFactory(): ExternalLookupReaderFactory = factory
}

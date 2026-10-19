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

package org.apache.spark.sql.connector.read;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * A task-local reader that owns client resources and executes synchronous lookup requests. One
 * reader may serve multiple sequential request batches in its task.
 *
 * <p>Spark does not pass nulls for ordinary comparison parameters. It may pass nulls for null-safe
 * equality parameters; a connector that selects such a predicate must implement Spark's null-safe
 * equality semantics. Calls are not snapshot boundaries: separate calls, and values within one
 * call, may observe different external source versions unless the connector documents stronger
 * semantics. Implementations should respond to thread interruption when the underlying client
 * supports cancellation.</p>
 *
 * @since 4.4.0
 */
@Evolving
public interface ExternalLookupReader extends Closeable {

  /**
   * Looks up one finite batch of unique parameter rows. Results may be unordered and may contain
   * multiple values for an input ordinal. All values returned for one ordinal collectively form the
   * complete result for that parameter row; an ordinal with no result is a lookup miss. Spark may
   * cache that complete result, including an empty result, using the exact selected parameter row.
   * Conditions not selected as lookup predicates are residual predicates and do not participate in
   * cache identity.
   *
   * <p>Parameter fields are ordered as selected by
   * {@link SupportsExternalLookup#selectLookupPredicates} and use Spark's internal representation
   * for each {@link LookupParameter#dataType() parameter data type}. The input array and rows are
   * read-only. The returned cursor must be exhausted or closed before another lookup starts. The
   * reader must not retain input rows after that cursor is closed.</p>
   *
   * <p>{@code uniqueParameters} is non-null and non-empty, contains no null rows, and has no more
   * than {@link ExternalLookup#maxParameterRowsPerRequest()} entries. Every row has exactly the
   * field count and internal field representation established by the selected lookup predicates.
   * Implementations must return a non-null cursor.</p>
   *
   * <p>A request either returns a complete cursor or fails; Spark does not consume partial results
   * after a failure. Tasks, and therefore requests, may be retried or speculated. Implementations
   * must tolerate duplicate requests. Spark closes cursors and readers on a best-effort basis.</p>
   */
  ExternalLookupResultCursor lookupBatch(InternalRow[] uniqueParameters) throws IOException;

  /**
   * Returns lightweight snapshots of the current cumulative values for custom task metrics
   * declared by {@link Scan#supportedCustomMetrics()}.
   */
  default CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[0];
  }
}

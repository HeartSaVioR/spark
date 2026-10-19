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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A mix-in for a {@link ScanBuilder} that can select a parameterized lookup access path.
 *
 * @since 4.4.0
 */
@Evolving
public interface SupportsExternalLookup extends ScanBuilder {
  /**
   * Selects one complete access path from the candidates offered by Spark. Candidates contain
   * lookup-side {@link NamedReference references} and runtime {@link LookupParameter parameters}.
   * Spark initially offers binary comparisons with the lookup field normalized to the left.
   * Connectors must reject predicate names or shapes they do not support. Selecting a predicate
   * promises that every result for its input ordinal satisfies Spark's semantics for that predicate
   * and that the results are complete for the conjunction of all selected predicates. Spark removes
   * selected predicates from residual evaluation and evaluates every unselected candidate, together
   * with the rest of the join condition, after lookup.
   *
   * <p>The returned array contains the selected predicates, in the canonical predicate order
   * expected by lookup requests. Each element must be one of the {@code candidates} instances
   * returned as is; Spark maps a selection back to its originating join condition by object
   * identity, not by {@link Object#equals}. Each candidate is a distinct occurrence: two candidates
   * may have the same shape, for example when the same lookup field is compared with two different
   * join-side values, and selecting one of them does not select the other. A field may occur more
   * than once, for example as the lower and upper bounds of a range. Parameter fields follow the
   * returned order, then depth-first expression order within each predicate. The input array and
   * its elements are read-only. An empty array means that no access path can serve the offered join
   * predicates. The result must be non-null and must not contain null or the same candidate more
   * than once.</p>
   *
   * <p>This method is called exactly once, after filter pushdown and before column pruning. A
   * non-empty selection configures this builder and must remain effective through subsequent
   * pruning and {@link #build()}. Selected predicate fields may be absent from the schema later
   * passed to column pruning and from the resulting scan read schema, so the predicates and their
   * parameter ordering must be retained independently of response-column state. Pushed literal
   * predicates may contribute fixed fields needed to select a composite access path.</p>
   */
  Predicate[] selectLookupPredicates(Predicate[] candidates);
}

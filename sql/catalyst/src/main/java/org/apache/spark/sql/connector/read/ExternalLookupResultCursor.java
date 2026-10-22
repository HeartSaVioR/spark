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

/**
 * A request-local cursor over the results of one external lookup request.
 *
 * <p>{@link #inputOrdinal()} and {@link #get()} are valid only after {@link #next()} returns
 * {@code true} and until the next call to {@code next} or {@link #close()}. Implementations may
 * reuse the returned row; Spark copies a row before retaining it. Repeated ordinals represent
 * one-to-many results, while an absent ordinal represents a miss. A match with a zero-column
 * result schema must still return one empty row.</p>
 *
 * @since 4.4.0
 */
@Evolving
public interface ExternalLookupResultCursor extends Closeable {
  /** Advances to the next result. */
  boolean next() throws IOException;

  /**
   * Returns the input parameter-row ordinal for the current result. While positioned on a result,
   * this must satisfy {@code 0 <= inputOrdinal() < uniqueParameters.length} for the corresponding
   * lookup request.
   */
  int inputOrdinal();

  /** Returns the current row, whose schema is the {@link Scan#readSchema()}. */
  InternalRow get();
}

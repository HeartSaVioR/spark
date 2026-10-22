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

/**
 * The physical representation of a parameterized external lookup operation.
 *
 * <p>This driver-side operation is the lookup counterpart of {@link Batch}. It creates the
 * serializable factory that Spark sends to executor tasks.</p>
 *
 * @since 4.4.0
 */
@Evolving
public interface ExternalLookup {
  /**
   * The largest number of parameter rows accepted by one lookup request. This must return a
   * positive value that remains stable for this operation.
   */
  int maxParameterRowsPerRequest();

  /** Creates the serializable factory used to open task-local lookup readers. */
  ExternalLookupReaderFactory createReaderFactory();
}

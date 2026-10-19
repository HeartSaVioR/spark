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

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.types.DataType;

/**
 * A runtime-bound value in a predicate offered for external lookup.
 *
 * <p>If the connector selects a predicate containing this expression, its value is supplied in
 * each parameter row passed to {@link ExternalLookupReader#lookupBatch}. Parameter fields follow
 * selected-predicate order, then depth-first expression order within each predicate.</p>
 *
 * @since 4.4.0
 */
@Evolving
public final class LookupParameter implements Expression, Serializable {
  private static final long serialVersionUID = 1L;
  private final DataType dataType;

  public LookupParameter(DataType dataType) {
    this.dataType = dataType;
  }

  /** The Spark SQL data type and internal representation of the runtime value. */
  public DataType dataType() {
    return dataType;
  }

  @Override
  public Expression[] children() {
    return EMPTY_EXPRESSION;
  }

  @Override
  public String describe() {
    return "?";
  }

  @Override
  public String toString() {
    return describe();
  }
}

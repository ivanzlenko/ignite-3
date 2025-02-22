/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.framework;

import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * Base interface describing the complete set of table-related fields.
 *
 * <p>The sole purpose of this interface is to keep in sync both variants of table's builders.
 *
 * @param <ChildT> An actual type of builder that should be exposed to the user.
 * @see ClusterTableBuilder
 * @see TableBuilder
 */
public interface TableBuilderBase<ChildT> {
    /** Sets the name of the table. */
    ChildT name(String name);

    /** Adds a key column to the table. */
    ChildT addKeyColumn(String name, NativeType type);

    /** Adds a column to the table. */
    ChildT addColumn(String name, NativeType type);

    /** Adds a column with given nullability to the table. */
    ChildT addColumn(String name, NativeType type, boolean nullable);

    /** Adds a column with the given default value to the table. */
    ChildT addColumn(String name, NativeType type, @Nullable Object defaultValue);
}

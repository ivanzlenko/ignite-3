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

import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;

/**
 * Base interface describing the set of sorted-index related fields.
 *
 * @param <ChildT> An actual type of builder that should be exposed to the user.
 */
public interface SortedIndexBuilderBase<ChildT> extends IndexBuilderBase<ChildT> {
    /** Adds a column with specified collation to the index. */
    ChildT addColumn(String columnName, Collation collation);
}

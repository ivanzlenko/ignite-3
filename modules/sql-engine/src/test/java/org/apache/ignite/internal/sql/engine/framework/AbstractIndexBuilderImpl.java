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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;

public abstract class AbstractIndexBuilderImpl<ChildT> implements SortedIndexBuilderBase<ChildT>, HashIndexBuilderBase<ChildT> {
    String name;
    final List<String> columns = new ArrayList<>();
    List<Collation> collations;

    /** {@inheritDoc} */
    @Override
    public ChildT name(String name) {
        this.name = name;

        return self();
    }

    /** {@inheritDoc} */
    @Override
    public ChildT addColumn(String columnName) {
        columns.add(columnName);

        return self();
    }

    /** {@inheritDoc} */
    @Override
    public ChildT addColumn(String columnName, Collation collation) {
        if (collations == null) {
            collations = new ArrayList<>();
        }

        columns.add(columnName);
        collations.add(collation);

        return self();
    }

    abstract ChildT self();
}

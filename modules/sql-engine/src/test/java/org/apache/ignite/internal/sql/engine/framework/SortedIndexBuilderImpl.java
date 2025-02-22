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

import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

class SortedIndexBuilderImpl extends AbstractTableIndexBuilderImpl<SortedIndexBuilder> implements SortedIndexBuilder {
    private final TableBuilderImpl parent;

    private boolean primary;

    SortedIndexBuilderImpl(TableBuilderImpl parent) {
        this.parent = parent;
    }

    /** {@inheritDoc} */
    @Override
    SortedIndexBuilder self() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder end() {
        parent.indexBuilders().add(this);

        return parent;
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexBuilder primaryKey(boolean value) {
        this.primary = value;
        return self();
    }

    /** {@inheritDoc} */
    @Override
    public TestIndex build(TableDescriptor desc) {
        if (name == null) {
            throw new IllegalArgumentException("Name is not specified");
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Index must contain at least one column");
        }

        if (collations.size() != columns.size()) {
            throw new IllegalArgumentException("Collation must be specified for each of columns.");
        }

        return TestIndex.createSorted(name, columns, collations, desc, primary);
    }
}

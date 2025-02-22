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

import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * A builder to create a test table object.
 */
public interface TableBuilder extends TableBuilderBase<TableBuilder> {
    /** Returns a builder of the test sorted-index object. */
    SortedIndexBuilder sortedIndex();

    /** Returns a builder of the test hash-index object. */
    HashIndexBuilder hashIndex();

    /** Sets the distribution of the table. */
    TableBuilder distribution(IgniteDistribution distribution);

    /** Sets the size of the table. */
    TableBuilder size(int size);

    /** Sets id for the table. The caller must guarantee that provided id is unique. */
    TableBuilder tableId(int id);

    /** Sets the number of partitions fot this table. Default value is equal to {@link CatalogUtils#DEFAULT_PARTITION_COUNT}. */
    TableBuilder partitions(int num);

    /**
     * Builds a table.
     *
     * @return Created table object.
     */
    IgniteTable build();
}

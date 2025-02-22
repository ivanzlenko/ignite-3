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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

public class TableBuilderImpl implements TableBuilder {
    private static final AtomicInteger TABLE_ID_GEN = new AtomicInteger();

    private final List<AbstractTableIndexBuilderImpl<?>> indexBuilders = new ArrayList<>();
    private final List<ColumnDescriptor> columns = new ArrayList<>();

    private String name;
    private IgniteDistribution distribution;
    private int size = 100_000;
    private Integer tableId;
    private int partitions = CatalogUtils.DEFAULT_PARTITION_COUNT;

    /** {@inheritDoc} */
    @Override
    public TableBuilder name(String name) {
        this.name = name;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexBuilder sortedIndex() {
        return new SortedIndexBuilderImpl(this);
    }

    /** {@inheritDoc} */
    @Override
    public HashIndexBuilder hashIndex() {
        return new HashIndexBuilderImpl(this);
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder distribution(IgniteDistribution distribution) {
        this.distribution = distribution;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder addColumn(String name, NativeType type, boolean nullable) {
        columns.add(new ColumnDescriptorImpl(
                name, false, false, false, nullable, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
        ));

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder addColumn(String name, NativeType type) {
        return addColumn(name, type, true);
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder addColumn(String name, NativeType type, @Nullable Object defaultValue) {
        if (defaultValue == null) {
            return addColumn(name, type);
        } else {
            ColumnDescriptorImpl desc = new ColumnDescriptorImpl(
                    name, false, false, false, true, columns.size(), type, DefaultValueStrategy.DEFAULT_CONSTANT, () -> defaultValue
            );
            columns.add(desc);
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder addKeyColumn(String name, NativeType type) {
        columns.add(new ColumnDescriptorImpl(
                name, true, false, false, false, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
        ));

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder size(int size) {
        this.size = size;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder tableId(int id) {
        this.tableId = id;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableBuilder partitions(int num) {
        this.partitions = num;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTable build() {
        if (distribution == null) {
            throw new IllegalArgumentException("Distribution is not specified");
        }

        if (name == null) {
            throw new IllegalArgumentException("Name is not specified");
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Table must contain at least one column");
        }

        TableDescriptorImpl tableDescriptor = new TableDescriptorImpl(CollectionUtils.concat(columns,
                List.of(new ColumnDescriptorImpl(
                        Commons.PART_COL_NAME,
                        false,
                        true,
                        true,
                        false,
                        columns.size(),
                        NativeTypes.INT32,
                        DefaultValueStrategy.DEFAULT_COMPUTED,
                        () -> {
                            throw new AssertionError("Partition virtual column is generated by a function");
                        }
                ))), distribution);

        Map<String, IgniteIndex> indexes = indexBuilders.stream()
                .map(idx -> idx.build(tableDescriptor))
                .collect(Collectors.toUnmodifiableMap(IgniteIndex::name, Function.identity()));

        return new IgniteTableImpl(
                Objects.requireNonNull(name),
                tableId != null ? tableId : TABLE_ID_GEN.incrementAndGet(),
                1,
                tableDescriptor,
                findPrimaryKey(tableDescriptor, indexes.values()),
                new TestStatistic(size),
                indexes,
                partitions
        );
    }

    private static ImmutableIntList findPrimaryKey(TableDescriptor descriptor, Collection<IgniteIndex> indexList) {
        IgniteIndex primaryKey = indexList.stream()
                .filter(IgniteIndex::primaryKey)
                .findFirst()
                .orElse(null);

        if (primaryKey != null) {
            return primaryKey.collation().getKeys();
        }

        List<Integer> list = new ArrayList<>();
        for (ColumnDescriptor column : descriptor) {
            if (column.key()) {
                list.add(column.logicalIndex());
            }
        }

        return ImmutableIntList.copyOf(list);
    }

    List<AbstractTableIndexBuilderImpl<?>> indexBuilders() {
        return indexBuilders;
    }

    /** Returns a builder of the test table object. */
    public static TableBuilder table() {
        return new TableBuilderImpl();
    }
}

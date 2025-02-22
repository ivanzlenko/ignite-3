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
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.jetbrains.annotations.Nullable;

public class ClusterTableBuilderImpl implements ClusterTableBuilder {
    private final List<AbstractClusterTableIndexBuilderImpl<?>> indexBuilders = new ArrayList<>();

    private final List<ColumnParams> columns = new ArrayList<>();
    private final List<String> keyColumns = new ArrayList<>();

    private final ClusterBuilderImpl parent;

    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    private String name;

    ClusterTableBuilderImpl(ClusterBuilderImpl parent) {
        this.parent = parent;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterTableBuilder name(String name) {
        this.name = name;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterSortedIndexBuilder addSortedIndex() {
        return new ClusterSortedIndexBuilderImpl(this);
    }

    /** {@inheritDoc} */
    @Override
    public ClusterHashIndexBuilder addHashIndex() {
        return new ClusterHashIndexBuilderImpl(this);
    }

    @Override
    public ClusterTableBuilder addColumn(String name, NativeType type, boolean nullable) {
        columns.add(columnParams(name, type, nullable, null));

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterTableBuilder addColumn(String name, NativeType type) {
        return addColumn(name, type, true);
    }

    @Override
    public ClusterTableBuilder addColumn(String name, NativeType type, @Nullable Object defaultValue) {
        columns.add(columnParams(name, type, true, defaultValue));

        return this;
    }

    @Override
    public ClusterTableBuilder addKeyColumn(String name, NativeType type) {
        keyColumns.add(name);

        return addColumn(name, type, false);
    }

    /** {@inheritDoc} */
    @Override
    public ClusterBuilder end() {
        parent.tableBuilders().add(this);

        return parent;
    }

    List<CatalogCommand> build() {
        List<CatalogCommand> commands = new ArrayList<>(1 + indexBuilders.size());

        // TODO https://issues.apache.org/jira/browse/IGNITE-21715 Update after TestFramework provides API
        //  to specify type of a primary key index.
        // Use sorted index by default.
        TablePrimaryKey primaryKey = TableHashPrimaryKey.builder()
                .columns(keyColumns)
                .build();

        commands.add(
                CreateTableCommand.builder()
                        .schemaName(SCHEMA_NAME)
                        .tableName(name)
                        .columns(columns)
                        .primaryKey(primaryKey)
                        .build()
        );

        for (AbstractClusterTableIndexBuilderImpl<?> builder : indexBuilders) {
            commands.add(builder.build(SCHEMA_NAME, name));
        }

        return commands;
    }

    private static ColumnParams columnParams(String name, NativeType type, boolean nullable, @Nullable Object defaultValue) {
        NativeTypeSpec typeSpec = type.spec();

        Builder builder = ColumnParams.builder()
                .name(name)
                .type(typeSpec.asColumnType())
                .nullable(nullable)
                .defaultValue(DefaultValue.constant(defaultValue));

        switch (typeSpec) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case UUID:
            case BOOLEAN:
                break;
            case DECIMAL:
                assert type instanceof DecimalNativeType : type.getClass().getCanonicalName();

                builder.precision(((DecimalNativeType) type).precision());
                builder.scale(((DecimalNativeType) type).scale());
                break;
            case STRING:
            case BYTES:
                assert type instanceof VarlenNativeType : type.getClass().getCanonicalName();

                builder.length(((VarlenNativeType) type).length());
                break;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                assert type instanceof TemporalNativeType : type.getClass().getCanonicalName();

                builder.precision(((TemporalNativeType) type).precision());
                break;
            default:
                throw new IllegalArgumentException("Unsupported native type: " + typeSpec);
        }

        return builder.build();
    }

    List<AbstractClusterTableIndexBuilderImpl<?>> indexBuilders() {
        return indexBuilders;
    }
}

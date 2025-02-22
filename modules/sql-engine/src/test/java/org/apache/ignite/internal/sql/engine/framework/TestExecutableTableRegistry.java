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

import java.lang.reflect.Proxy;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTable;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

public class TestExecutableTableRegistry implements ExecutableTableRegistry {
    private final Function<String, ScannableTable> scannableTablesByName;
    private final Function<String, UpdatableTable> updatableTablesByName;
    private final SqlSchemaManager schemaManager;

    TestExecutableTableRegistry(
            Function<String, ScannableTable> scannableTablesByName,
            Function<String, UpdatableTable> updatableTablesByName,
            SqlSchemaManager schemaManager
    ) {
        this.scannableTablesByName = scannableTablesByName;
        this.updatableTablesByName = updatableTablesByName;
        this.schemaManager = schemaManager;
    }

    @Override
    public ExecutableTable getTable(int catalogVersion, int tableId) {
        IgniteTable table = schemaManager.table(catalogVersion, tableId);

        assert table != null;

        return new ExecutableTable() {
            @Override
            public ScannableTable scannableTable() {
                ScannableTable scannableTable = scannableTablesByName.apply(table.name());

                assert scannableTable != null;

                return scannableTable;
            }

            @Override
            public UpdatableTable updatableTable() {
                if (Blackhole.TABLE_NAME.equals(table.name())) {
                    return Blackhole.INSTANCE;
                }

                UpdatableTable updatableTable = updatableTablesByName.apply(table.name());

                assert updatableTable != null;

                return (UpdatableTable) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?> [] {UpdatableTable.class},
                        (proxy, method, args) -> {
                            if ("descriptor".equals(method.getName())) {
                                return table.descriptor();
                            }

                            return method.invoke(updatableTable, args);
                        }
                );
            }

            @Override
            public TableDescriptor tableDescriptor() {
                return table.descriptor();
            }

            @Override
            public Supplier<PartitionCalculator> partitionCalculator() {
                return table.partitionCalculator();
            }
        };
    }
}

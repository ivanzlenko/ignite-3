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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;

public class Blackhole implements UpdatableTable {
    static final String TABLE_NAME = "BLACKHOLE";

    private static final TableDescriptor DESCRIPTOR = new TableDescriptorImpl(
            List.of(new ColumnDescriptorImpl("X", true, false, false, false, 0,
                    NativeTypes.INT32, DefaultValueStrategy.DEFAULT_NULL, null)), IgniteDistributions.single()
    );

    static final UpdatableTable INSTANCE = new Blackhole();

    @Override
    public TableDescriptor descriptor() {
        return DESCRIPTOR;
    }

    @Override
    public <RowT> CompletableFuture<?> insertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
        return nullCompletedFuture();
    }

    @Override
    public <RowT> CompletableFuture<Void> insert(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx, RowT row) {
        return nullCompletedFuture();
    }

    @Override
    public <RowT> CompletableFuture<?> upsertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
        return nullCompletedFuture();
    }

    @Override
    public <RowT> CompletableFuture<?> deleteAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
        return nullCompletedFuture();
    }
}

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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiFunction;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.jetbrains.annotations.Nullable;

public final class ScannableTableFactory {
    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#scan table scan}.
     */
    public static ScannableTable tableScan(DataProvider<Object[]> dataProvider) {
        return new AbstractScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns
            ) {

                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        dataProvider.iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#scan table scan}.
     */
    public static ScannableTable tableScan(BiFunction<String, Integer, Iterable<Object[]>> generatorFunction) {
        return new AbstractScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns
            ) {

                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        generatorFunction.apply(ctx.localNode().name(), partWithConsistencyToken.partId()).iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#indexRangeScan index range scan}.
     */
    public static ScannableTable indexRangeScan(DataProvider<Object[]> dataProvider) {
        return new ScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns
            ) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                    @Nullable BitSet requiredColumns) {
                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        dataProvider.iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }

            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx, InternalTransaction explicitTx,
                    RowFactory<RowT> rowFactory, RowT key, @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Long> estimatedSize() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#indexLookup index lookup}.
     */
    public static ScannableTable indexLookup(DataProvider<Object[]> dataProvider) {
        return new ScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns
            ) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                    @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        dataProvider.iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }

            @Override
            public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx, InternalTransaction explicitTx,
                    RowFactory<RowT> rowFactory, RowT key, @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Long> estimatedSize() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static Object[] project(Object[] row, @Nullable BitSet requiredElements) {
        if (requiredElements == null) {
            return row;
        }

        Object[] newRow = new Object[requiredElements.cardinality()];

        int idx = 0;
        for (int i = requiredElements.nextSetBit(0); i != -1; i = requiredElements.nextSetBit(i + 1)) {
            newRow[idx++] = row[i];
        }

        return newRow;
    }
}

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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.ExecutionId;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.network.ClusterNode;

public class ExecutionContextBuilderImpl implements ExecutionContextBuilder {
    private FragmentDescription description = new FragmentDescription(0, true, null, null, null, null);

    private UUID queryId = null;
    private QueryTaskExecutor executor = null;
    private ClusterNode node = null;
    private Object[] dynamicParams = ArrayUtils.OBJECT_EMPTY_ARRAY;

    /** {@inheritDoc} */
    @Override
    public ExecutionContextBuilder queryId(UUID queryId) {
        this.queryId = Objects.requireNonNull(queryId, "queryId");

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContextBuilder fragment(FragmentDescription fragmentDescription) {
        this.description = Objects.requireNonNull(fragmentDescription, "fragmentDescription");

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContextBuilder executor(QueryTaskExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContextBuilder localNode(ClusterNode node) {
        this.node = Objects.requireNonNull(node, "node");

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContextBuilder dynamicParameters(Object... params) {
        this.dynamicParams = params;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContext<Object[]> build() {
        return new ExecutionContext<>(
                new ExpressionFactoryImpl<>(
                        Commons.typeFactory(), 1024, CaffeineCacheFactory.INSTANCE
                ),
                Objects.requireNonNull(executor, "executor"),
                new ExecutionId(queryId, 0),
                Objects.requireNonNull(node, "node"),
                node.name(),
                description,
                ArrayRowHandler.INSTANCE,
                Commons.parametersMap(dynamicParams),
                TxAttributes.fromTx(new NoOpTransaction(node.name(), false)),
                SqlQueryProcessor.DEFAULT_TIME_ZONE_ID
        );
    }

    /** Returns a builder of the execution context. */
    public static ExecutionContextBuilder executionContext() {
        return new ExecutionContextBuilderImpl();
    }
}

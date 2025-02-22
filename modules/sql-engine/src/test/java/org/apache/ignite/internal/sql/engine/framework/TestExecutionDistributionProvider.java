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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionDistributionProvider;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

public class TestExecutionDistributionProvider implements ExecutionDistributionProvider {
    final Function<String, List<String>> owningNodesBySystemViewName;

    final Function<String, AssignmentsProvider> owningNodesByTableName;

    final boolean useTablePartitions;

    TestExecutionDistributionProvider(
            Function<String, List<String>> owningNodesBySystemViewName,
            Function<String, AssignmentsProvider> owningNodesByTableName,
            boolean useTablePartitions
    ) {
        this.owningNodesBySystemViewName = owningNodesBySystemViewName;
        this.owningNodesByTableName = owningNodesByTableName;
        this.useTablePartitions = useTablePartitions;
    }

    private static TokenizedAssignments partitionNodesToAssignment(List<String> nodes, long token) {
        return new TokenizedAssignmentsImpl(
                nodes.stream().map(Assignment::forPeer).collect(Collectors.toSet()),
                token
        );
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> forTable(
            HybridTimestamp operationTime,
            IgniteTable table,
            boolean includeBackups
    ) {
        AssignmentsProvider provider = owningNodesByTableName.apply(table.name());

        if (provider == null) {
            return CompletableFuture.failedFuture(
                    new AssertionError("AssignmentsProvider is not configured for table " + table.name())
            );
        }
        List<List<String>> owningNodes = provider.get(table.partitions(), includeBackups);

        if (nullOrEmpty(owningNodes) || owningNodes.size() != table.partitions()) {
            throw new AssertionError("Configured AssignmentsProvider returns less assignment than expected "
                    + "[table=" + table.name() + ", expectedNumberOfPartitions=" + table.partitions()
                    + ", returnedAssignmentSize=" + (owningNodes == null ? "<null>" : owningNodes.size()) + "]");
        }

        List<TokenizedAssignments> assignments;

        if (useTablePartitions) {
            int p = table.partitions();

            assignments = IntStream.range(0, p).mapToObj(n -> {
                List<String> nodes = owningNodes.get(n % owningNodes.size());
                return partitionNodesToAssignment(nodes, p);
            }).collect(Collectors.toList());
        } else {
            assignments = owningNodes.stream()
                    .map(nodes -> partitionNodesToAssignment(nodes, 1))
                    .collect(Collectors.toList());
        }

        return CompletableFuture.completedFuture(assignments);
    }

    @Override
    public List<String> forSystemView(IgniteSystemView view) {
        List<String> nodes = owningNodesBySystemViewName.apply(view.name());

        if (nullOrEmpty(nodes)) {
            throw new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                    + " any active nodes in the cluster", view));
        }

        return view.distribution() == IgniteDistributions.single() ? List.of(nodes.get(0)) : nodes;
    }

    /** Returns a builder for {@link ExecutionDistributionProvider}. */
    public static ExecutionDistributionProviderBuilder executionDistributionProviderBuilder() {
        return new ExecutionDistributionProviderBuilder();
    }

    /** A builder to create instances of {@link ExecutionDistributionProvider}. */
    public static final class ExecutionDistributionProviderBuilder {

        private final Map<String, List<List<String>>> owningNodesByTableName = new HashMap<>();

        private Function<String, List<String>> owningNodesBySystemViewName = (n) -> null;

        private boolean useTablePartitions;

        private ExecutionDistributionProviderBuilder() {

        }

        /** Adds tables to list of nodes mapping. */
        public ExecutionDistributionProviderBuilder addTables(Map<String, List<List<String>>> tables) {
            this.owningNodesByTableName.putAll(tables);
            return this;
        }

        /**
         * Sets a function that returns system views. Function accepts a view name and returns a list of nodes a system view is available
         * at.
         */
        public ExecutionDistributionProviderBuilder setSystemViews(Function<String, List<String>> systemViews) {
            this.owningNodesBySystemViewName = systemViews;
            return this;
        }

        /** Use table partitions to build mapping targets. Default is {@code false}. */
        public ExecutionDistributionProviderBuilder useTablePartitions(boolean value) {
            useTablePartitions = value;
            return this;
        }

        /** Creates an instance of {@link ExecutionDistributionProvider}. */
        public ExecutionDistributionProvider build() {
            Map<String, List<List<String>>> owningNodesByTableName = Map.copyOf(this.owningNodesByTableName);

            Function<String, AssignmentsProvider> sourceProviderFunction = tableName ->
                    (AssignmentsProvider) (partitionsCount, includeBackups) -> {
                        List<List<String>> assignments = owningNodesByTableName.get(tableName);

                        if (nullOrEmpty(assignments)) {
                            throw new AssertionError("Assignments are not configured for table " + tableName);
                        }

                        return assignments;
                    };

            return new TestExecutionDistributionProvider(
                    owningNodesBySystemViewName,
                    sourceProviderFunction,
                    useTablePartitions
            );
        }
    }
}

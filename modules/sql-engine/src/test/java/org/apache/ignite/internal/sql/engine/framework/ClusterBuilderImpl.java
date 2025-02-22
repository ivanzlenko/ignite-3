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

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.PLANNING_THREAD_COUNT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPrunerImpl;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManager;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

public class ClusterBuilderImpl implements ClusterBuilder {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterBuilderImpl.class);

    private final List<ClusterTableBuilderImpl> tableBuilders = new ArrayList<>();
    private List<String> nodeNames;
    private final List<SystemView<?>> systemViews = new ArrayList<>();
    private final Map<String, Set<String>> nodeName2SystemView = new HashMap<>();

    private long planningTimeout = TimeUnit.SECONDS.toMillis(15);
    private Function<CatalogManager, CatalogManager> catalogManagerDecorator = Function.identity();
    private OperationKillHandler @Nullable [] killHandlers = null;

    private @Nullable DefaultDataProvider defaultDataProvider = null;
    private @Nullable DefaultAssignmentsProvider defaultAssignmentsProvider = null;

    /** {@inheritDoc} */
    @Override
    public ClusterBuilder nodes(String firstNodeName, String... otherNodeNames) {
        this.nodeNames = new ArrayList<>();

        nodeNames.add(firstNodeName);
        nodeNames.addAll(Arrays.asList(otherNodeNames));

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterBuilder catalogManagerDecorator(Function<CatalogManager, CatalogManager> decorator) {
        this.catalogManagerDecorator = Objects.requireNonNull(decorator);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterBuilder operationKillHandlers(OperationKillHandler... handlers) {
        this.killHandlers = handlers;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterTableBuilder addTable() {
        return new ClusterTableBuilderImpl(this);
    }

    @Override
    public <T> ClusterBuilder addSystemView(SystemView<T> systemView) {
        systemViews.add(systemView);
        return this;
    }

    @Override
    public ClusterBuilder registerSystemView(String nodeName, String systemViewName) {
        nodeName2SystemView.computeIfAbsent(nodeName, key -> new HashSet<>()).add(systemViewName);

        return this;
    }

    @Override
    public ClusterBuilder defaultDataProvider(DefaultDataProvider defaultDataProvider) {
        this.defaultDataProvider = defaultDataProvider;

        return this;
    }

    @Override
    public ClusterBuilder defaultAssignmentsProvider(DefaultAssignmentsProvider defaultAssignmentsProvider) {
        this.defaultAssignmentsProvider = defaultAssignmentsProvider;

        return this;
    }

    @Override
    public ClusterBuilder planningTimeout(long value, TimeUnit timeUnit) {
        this.planningTimeout = timeUnit.toMillis(value);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TestCluster build() {
        var clusterService = new ClusterServiceFactory(nodeNames);

        var clusterName = "test_cluster";

        HybridClock clock = new HybridClockImpl();
        CatalogManager catalogManager = catalogManagerDecorator.apply(
                CatalogTestUtils.createCatalogManagerWithTestUpdateLog(clusterName, clock)
        );

        var parserService = new ParserServiceImpl();

        ConcurrentMap<String, Long> tablesSize = new ConcurrentHashMap<>();
        var schemaManager = createSqlSchemaManager(catalogManager, tablesSize);
        var prepareService = new PrepareServiceImpl(clusterName, 0, CaffeineCacheFactory.INSTANCE,
                new DdlSqlToCommandConverter(), planningTimeout, PLANNING_THREAD_COUNT,
                new NoOpMetricManager(), schemaManager);

        Map<String, List<String>> systemViewsByNode = new HashMap<>();

        for (Entry<String, Set<String>> entry : nodeName2SystemView.entrySet()) {
            String nodeName = entry.getKey();
            for (String systemViewName : entry.getValue()) {
                systemViewsByNode.computeIfAbsent(nodeName, (k) -> new ArrayList<>()).add(systemViewName);
            }
        }

        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create("test", "common-scheduled-executors", LOG)
        );

        var clockWaiter = new ClockWaiter("test", clock, scheduledExecutor);
        var ddlHandler = new DdlCommandHandler(catalogManager, new TestClockService(clock, clockWaiter));

        Runnable initClosure = () -> {
            assertThat(clockWaiter.startAsync(new ComponentContext()), willCompleteSuccessfully());

            initAction(catalogManager);
        };

        RunnableX stopClosure = () -> IgniteUtils.shutdownAndAwaitTermination(scheduledExecutor, 10, TimeUnit.SECONDS);

        List<LogicalNode> logicalNodes = nodeNames.stream()
                .map(name -> {
                    List<String> systemViewForNode = systemViewsByNode.getOrDefault(name, List.of());
                    NetworkAddress addr = NetworkAddress.from("127.0.0.1:10000");
                    LogicalNode logicalNode = new LogicalNode(randomUUID(), name, addr);

                    if (systemViewForNode.isEmpty()) {
                        return logicalNode;
                    } else {
                        String attrName = SystemViewManagerImpl.NODE_ATTRIBUTES_KEY;
                        String nodeNameSep = SystemViewManagerImpl.NODE_ATTRIBUTES_LIST_SEPARATOR;
                        String nodeNamesString = String.join(nodeNameSep, systemViewForNode);

                        return new LogicalNode(logicalNode, Map.of(), Map.of(attrName, nodeNamesString), List.of());
                    }
                })
                .collect(Collectors.toList());

        ConcurrentMap<String, ScannableTable> dataProvidersByTableName = new ConcurrentHashMap<>();
        ConcurrentMap<String, UpdatableTable> updatableTablesByName = new ConcurrentHashMap<>();
        ConcurrentMap<String, AssignmentsProvider> assignmentsProviderByTableName = new ConcurrentHashMap<>();

        assignmentsProviderByTableName.put(
                Blackhole.TABLE_NAME,
                (partCount, ignored) -> IntStream.range(0, partCount)
                        .mapToObj(partNo -> nodeNames)
                        .collect(Collectors.toList())
        );

        DefaultDataProvider defaultDataProvider = this.defaultDataProvider;
        Map<String, TestNode> nodes = nodeNames.stream()
                .map(name -> {
                    var systemViewManager = new SystemViewManagerImpl(name, catalogManager);
                    var executionProvider = new TestExecutionDistributionProvider(
                            systemViewManager::owningNodes,
                            tableName -> resolveProvider(
                                    tableName,
                                    assignmentsProviderByTableName,
                                    defaultAssignmentsProvider != null ? defaultAssignmentsProvider::get : null
                            ),
                            false
                    );
                    var partitionPruner = new PartitionPrunerImpl();
                    var mappingService = new MappingServiceImpl(
                            name,
                            new TestClockService(clock, clockWaiter),
                            EmptyCacheFactory.INSTANCE,
                            0,
                            partitionPruner,
                            () -> 1L,
                            executionProvider
                    );

                    systemViewManager.register(() -> systemViews);

                    LogicalTopologySnapshot newTopology = new LogicalTopologySnapshot(1L, logicalNodes);
                    systemViewManager.onTopologyLeap(newTopology);

                    return new TestNode(
                            name,
                            catalogManager,
                            clusterService.forNode(name),
                            parserService,
                            prepareService,
                            schemaManager,
                            mappingService,
                            new TestExecutableTableRegistry(
                                    name0 -> resolveProvider(
                                            name0,
                                            dataProvidersByTableName,
                                            defaultDataProvider != null ? defaultDataProvider::get : null
                                    ),
                                    updatableTablesByName::get,
                                    schemaManager
                            ),
                            ddlHandler,
                            systemViewManager,
                            killHandlers
                    );
                })
                .collect(Collectors.toMap(TestNode::name, Function.identity()));

        return new TestCluster(
                tablesSize,
                dataProvidersByTableName,
                updatableTablesByName,
                assignmentsProviderByTableName,
                nodes,
                catalogManager,
                prepareService,
                clockWaiter,
                initClosure,
                stopClosure
        );
    }

    private void initAction(CatalogManager catalogManager) {
        List<CatalogCommand> initialSchema = tableBuilders.stream()
                .flatMap(builder -> builder.build().stream())
                .collect(Collectors.toList());

        // Every time an index is created add `start building `and `make available` commands
        // to make that index accessible to the SQL engine.
        Consumer<CreateIndexEventParameters> createIndexHandler = (params) -> {
            CatalogIndexDescriptor index = params.indexDescriptor();

            if (index.status() == CatalogIndexStatus.AVAILABLE) {
                return;
            }

            int indexId = index.id();

            CatalogCommand startBuildIndexCommand = StartBuildingIndexCommand.builder().indexId(indexId).build();
            CatalogCommand makeIndexAvailableCommand = MakeIndexAvailableCommand.builder().indexId(indexId).build();

            LOG.info("Index has been created. Sending commands to make index available. id: {}, name: {}, status: {}",
                    indexId, index.name(), index.status());

            catalogManager.execute(List.of(startBuildIndexCommand, makeIndexAvailableCommand))
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Catalog command execution error", e);
                        }
                    });
        };
        catalogManager.listen(CatalogEvent.INDEX_CREATE, EventListener.fromConsumer(createIndexHandler));

        // Init schema.
        await(catalogManager.execute(initialSchema));
    }

    private static <T> @Nullable T resolveProvider(
            String tableName,
            Map<String, T> providersByTableName,
            @Nullable Function<String, T> defaultProvider
    ) {
        T provider = providersByTableName.get(tableName);

        if (provider == null && defaultProvider != null) {
            return defaultProvider.apply(tableName);
        }

        return provider;
    }

    private static SqlSchemaManagerImpl createSqlSchemaManager(CatalogManager catalogManager, ConcurrentMap<String, Long> tablesSize) {
        SqlStatisticManager sqlStatisticManager = tableId -> {
            CatalogTableDescriptor descriptor = catalogManager.activeCatalog(Long.MAX_VALUE).table(tableId);
            long fallbackSize = 10_000;

            if (descriptor == null) {
                return fallbackSize;
            }

            return tablesSize.getOrDefault(descriptor.name(), 10_000L);
        };

        return new SqlSchemaManagerImpl(catalogManager, sqlStatisticManager, CaffeineCacheFactory.INSTANCE, 0);
    }

    List<ClusterTableBuilderImpl> tableBuilders() {
        return tableBuilders;
    }

    /** Returns a builder of the test cluster object. */
    public static ClusterBuilder cluster() {
        return new ClusterBuilderImpl();
    }
}

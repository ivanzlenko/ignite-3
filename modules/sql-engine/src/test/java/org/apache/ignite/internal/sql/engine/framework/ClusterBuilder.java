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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.systemview.api.SystemView;

/**
 * A builder to create a test cluster object.
 *
 * @see TestCluster
 */
public interface ClusterBuilder {
    /**
     * Sets desired names for the cluster nodes.
     *
     * @param firstNodeName A name of the first node. There is no difference in what node should be first. This parameter was
     *         introduced to force user to provide at least one node name.
     * @param otherNodeNames An array of rest of the names to create cluster from.
     * @return {@code this} for chaining.
     */
    ClusterBuilder nodes(String firstNodeName, String... otherNodeNames);

    /**
     * A decorator to wrap {@link CatalogManager} instance which will be used in the test cluster.
     *
     * <p>May be used to slow down or ignore certain catalog commands.
     *
     * @param decorator A decorator function which accepts original manager and returns decorated one.
     * @return {@code this} for chaining.
     */
    ClusterBuilder catalogManagerDecorator(Function<CatalogManager, CatalogManager> decorator);

    /**
     * Sets desired handlers for operation cancellation.
     *
     * <p>Cannot be used to cancel operation on test cluster, but may serve as mock.
     *
     * @param handlers The handlers to set.
     * @return {@code this} for chaining.
     */
    ClusterBuilder operationKillHandlers(OperationKillHandler... handlers);

    /**
     * Creates a table builder to add to the cluster.
     *
     * @return An instance of table builder.
     */
    ClusterTableBuilder addTable();

    /**
     * Adds the given system view to the cluster.
     *
     * @param systemView System view.
     * @param <T> System view data type.
     * @return {@code this} for chaining.
     */
    <T> ClusterBuilder addSystemView(SystemView<T> systemView);

    /**
     * Builds the cluster object.
     *
     * @return Created cluster object.
     */
    TestCluster build();

    /**
     * Provides implementation of table with given name.
     *
     * @param defaultDataProvider Name of the table given instance represents.
     * @return {@code this} for chaining.
     */
    ClusterBuilder defaultDataProvider(DefaultDataProvider defaultDataProvider);

    /**
     * Provides implementation of table with given name.
     *
     * @param defaultAssignmentsProvider Name of the table given instance represents.
     * @return {@code this} for chaining.
     */
    ClusterBuilder defaultAssignmentsProvider(DefaultAssignmentsProvider defaultAssignmentsProvider);

    /**
     * Registers a previously added system view (see {@link #addSystemView(SystemView)}) on the specified node.
     *
     * @param nodeName Name of the node the view is going to be registered at.
     * @param systemViewName Name of previously registered system.
     * @return {@code this} for chaining.
     */
    ClusterBuilder registerSystemView(String nodeName, String systemViewName);

    /**
     * Sets a timeout for query optimization phase.
     *
     * @param value A planning timeout value.
     * @param timeUnit A time unit.
     * @return {@code this} for chaining.
     */
    ClusterBuilder planningTimeout(long value, TimeUnit timeUnit);
}

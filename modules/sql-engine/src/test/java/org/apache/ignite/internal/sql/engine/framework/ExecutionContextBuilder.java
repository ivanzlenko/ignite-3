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

import java.util.UUID;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.network.ClusterNode;

/**
 * A builder to create an execution context.
 *
 * @see ExecutionContext
 */
public interface ExecutionContextBuilder {
    /** Sets the identifier of the query. */
    ExecutionContextBuilder queryId(UUID queryId);

    /** Sets the description of the fragment this context will be created for. */
    ExecutionContextBuilder fragment(FragmentDescription fragmentDescription);

    /** Sets the query task executor. */
    ExecutionContextBuilder executor(QueryTaskExecutor executor);

    /** Sets the node this fragment will be executed on. */
    ExecutionContextBuilder localNode(ClusterNode node);

    /** Sets the dynamic parameters this fragment will be executed with. */
    ExecutionContextBuilder dynamicParameters(Object... params);

    /**
     * Builds the context object.
     *
     * @return Created context object.
     */
    ExecutionContext<Object[]> build();
}

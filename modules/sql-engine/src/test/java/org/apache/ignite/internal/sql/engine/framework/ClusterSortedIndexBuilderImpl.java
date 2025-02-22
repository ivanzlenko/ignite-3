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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;

public class ClusterSortedIndexBuilderImpl extends AbstractClusterTableIndexBuilderImpl<ClusterSortedIndexBuilder>
        implements ClusterSortedIndexBuilder {
    private final ClusterTableBuilderImpl parent;

    ClusterSortedIndexBuilderImpl(ClusterTableBuilderImpl parent) {
        this.parent = parent;
    }

    /** {@inheritDoc} */
    @Override
    ClusterSortedIndexBuilder self() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ClusterTableBuilder end() {
        parent.indexBuilders().add(this);

        return parent;
    }

    @Override
    CatalogCommand build(String schemaName, String tableName) {
        assert collations.size() == columns.size();

        List<CatalogColumnCollation> catalogCollations = collations.stream()
                .map(c -> CatalogColumnCollation.get(c.asc, c.nullsFirst))
                .collect(Collectors.toList());

        return CreateSortedIndexCommand.builder()
                .schemaName(schemaName)
                .tableName(tableName)
                .indexName(name)
                .columns(columns)
                .collations(catalogCollations)
                .build();
    }
}

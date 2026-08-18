/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.MaterializedTableInfo;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;

import java.util.Optional;

/**
 * A {@link MaterializedTableInfo} over one catalog read.
 *
 * <p>Holds the resolved table rather than re-reading per getter, so the values a caller sees belong
 * to the same moment.
 */
@Internal
class MaterializedTableInfoImpl implements MaterializedTableInfo {

    private final ResolvedCatalogMaterializedTable table;

    MaterializedTableInfoImpl(ResolvedCatalogMaterializedTable table) {
        this.table = table;
    }

    @Override
    public RefreshStatus getRefreshStatus() {
        return table.getRefreshStatus();
    }

    @Override
    public String getOriginalQuery() {
        return table.getOriginalQuery();
    }

    @Override
    public String getExpandedQuery() {
        return table.getExpandedQuery();
    }

    @Override
    public IntervalFreshness getFreshness() {
        return table.getDefinitionFreshness();
    }

    @Override
    public RefreshMode getRefreshMode() {
        return table.getRefreshMode();
    }

    @Override
    public Optional<String> getRefreshHandlerDescription() {
        return table.getRefreshHandlerDescription();
    }

    @Override
    public String toString() {
        return "MaterializedTableInfo{"
                + "refreshStatus="
                + getRefreshStatus()
                + ", refreshMode="
                + getRefreshMode()
                + ", freshness="
                + getFreshness()
                + '}';
    }
}

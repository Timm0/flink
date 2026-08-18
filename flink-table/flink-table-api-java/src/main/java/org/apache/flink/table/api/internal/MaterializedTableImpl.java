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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.MaterializedTableInfo;
import org.apache.flink.table.api.RefreshJob;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableRefreshOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableResumeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;
import org.apache.flink.table.operations.materializedtable.DropMaterializedTableOperation;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;

import java.util.Map;
import java.util.Optional;

/**
 * A {@link MaterializedTable} handle over an identifier.
 *
 * <p>Holds no cached table: every call resolves the catalog first, so the handle keeps working
 * across suspends, resumes and refreshes, and reports what is true now rather than what was true
 * when the handle was made. The verbs build operations directly and hand them to {@link
 * TableEnvironmentInternal#executeInternal}, which routes them to the materialized table lifecycle
 * — the same path SQL takes, with no statement text in between.
 */
@Internal
class MaterializedTableImpl implements MaterializedTable {

    private final TableEnvironmentInternal tableEnvironment;
    private final ObjectIdentifier identifier;

    MaterializedTableImpl(TableEnvironmentInternal tableEnvironment, ObjectIdentifier identifier) {
        this.tableEnvironment = tableEnvironment;
        this.identifier = identifier;
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public Table asTable() {
        return tableEnvironment.from(identifier.asSerializableString());
    }

    @Override
    public MaterializedTableInfo info() {
        return new MaterializedTableInfoImpl(resolveTable());
    }

    @Override
    public Optional<RefreshJob> getRefreshJob() {
        final ResolvedCatalogMaterializedTable table = resolveTable();
        if (table.getRefreshMode() != RefreshMode.CONTINUOUS
                || table.getRefreshStatus() != RefreshStatus.ACTIVATED) {
            return Optional.empty();
        }
        final byte[] serializedHandler = table.getSerializedRefreshHandler();
        if (serializedHandler == null || serializedHandler.length == 0) {
            return Optional.empty();
        }

        final ContinuousRefreshHandler handler = deserializeRefreshHandler(serializedHandler);
        final JobClient jobClient =
                tableEnvironment
                        .getMaterializedTableLifecycle()
                        .getRefreshJobClient(
                                tableEnvironment.getMaterializedTableLifecycleContext(), identifier)
                        .orElse(null);
        return Optional.of(RefreshJobImpl.ofContinuousRefresh(handler, jobClient));
    }

    @Override
    public void suspend() {
        tableEnvironment.executeInternal(new AlterMaterializedTableSuspendOperation(identifier));
    }

    @Override
    public void suspend(boolean ignoreIfSuspended) {
        if (ignoreIfSuspended && refreshStatus() == RefreshStatus.SUSPENDED) {
            return;
        }
        suspend();
    }

    @Override
    public void resume() {
        resume(Map.of());
    }

    @Override
    public void resume(boolean ignoreIfActive) {
        resume(Map.of(), ignoreIfActive);
    }

    @Override
    public void resume(Map<String, String> dynamicOptions) {
        tableEnvironment.executeInternal(
                new AlterMaterializedTableResumeOperation(identifier, Map.copyOf(dynamicOptions)));
    }

    @Override
    public void resume(Map<String, String> dynamicOptions, boolean ignoreIfActive) {
        if (ignoreIfActive && refreshStatus() == RefreshStatus.ACTIVATED) {
            return;
        }
        resume(dynamicOptions);
    }

    @Override
    public RefreshJob refresh() {
        return refresh(Map.of());
    }

    @Override
    public RefreshJob refresh(Map<String, String> partitionSpec) {
        final TableResultInternal result =
                tableEnvironment.executeInternal(
                        new AlterMaterializedTableRefreshOperation(
                                identifier, Map.copyOf(partitionSpec)));
        return RefreshJobImpl.ofRefreshResult(result);
    }

    @Override
    public void drop() {
        drop(false);
    }

    @Override
    public void drop(boolean ignoreIfNotExists) {
        tableEnvironment.executeInternal(
                new DropMaterializedTableOperation(identifier, ignoreIfNotExists));
    }

    @Override
    public String toString() {
        return "MaterializedTable[" + identifier.asSummaryString() + "]";
    }

    // ---------------------------------------------------------------------------------------------

    private RefreshStatus refreshStatus() {
        return resolveTable().getRefreshStatus();
    }

    private ResolvedCatalogMaterializedTable resolveTable() {
        final Optional<ContextResolvedTable> contextResolvedTable =
                tableEnvironment.getCatalogManager().getTable(identifier);
        if (contextResolvedTable.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Materialized table %s does not exist.", identifier.asSummaryString()));
        }
        final ResolvedCatalogBaseTable<?> resolved = contextResolvedTable.get().getResolvedTable();
        if (!(resolved instanceof ResolvedCatalogMaterializedTable)) {
            throw new ValidationException(
                    String.format(
                            "%s is a %s, not a materialized table.",
                            identifier.asSummaryString(), resolved.getTableKind()));
        }
        return (ResolvedCatalogMaterializedTable) resolved;
    }

    private ContinuousRefreshHandler deserializeRefreshHandler(byte[] serializedHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                    serializedHandler,
                    tableEnvironment
                            .getMaterializedTableLifecycleContext()
                            .getResourceManager()
                            .getUserClassLoader());
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to read the refresh handler of materialized table %s.",
                            identifier.asSummaryString()),
                    e);
        }
    }
}

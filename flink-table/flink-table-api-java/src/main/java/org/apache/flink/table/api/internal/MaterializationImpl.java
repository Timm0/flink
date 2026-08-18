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
import org.apache.flink.table.api.CreateMode;
import org.apache.flink.table.api.Materialization;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.materializedtable.MaterializedTableDefinition;
import org.apache.flink.table.materializedtable.MaterializedTableOperationBuilder;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;

/**
 * A {@link Materialization} that builds its operation directly from the query tree.
 *
 * <p>No SQL text is generated along the way: the query stays a {@link QueryOperation} and the
 * declaration becomes an operation through the same {@link MaterializedTableOperationBuilder} the
 * SQL converters use, exactly as {@code Table.insertInto} builds a sink operation. The query is
 * serialized to SQL only for the catalog, because a materialized table is a persisted query
 * definition.
 */
@Internal
class MaterializationImpl implements Materialization {

    private final TableEnvironmentInternal tableEnvironment;
    private final QueryOperation queryOperation;
    private final ObjectIdentifier identifier;
    private final MaterializedTableDescriptor descriptor;

    private MaterializationImpl(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation queryOperation,
            ObjectIdentifier identifier,
            MaterializedTableDescriptor descriptor) {
        this.tableEnvironment = tableEnvironment;
        this.queryOperation = queryOperation;
        this.identifier = identifier;
        this.descriptor = descriptor;
    }

    static Materialization of(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation queryOperation,
            String path,
            MaterializedTableDescriptor descriptor) {
        final UnresolvedIdentifier unresolvedIdentifier =
                tableEnvironment.getParser().parseIdentifier(path);
        final ObjectIdentifier identifier =
                tableEnvironment.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        return new MaterializationImpl(tableEnvironment, queryOperation, identifier, descriptor);
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public MaterializedTable execute() {
        return execute(CreateMode.CREATE_OR_UPDATE);
    }

    @Override
    public MaterializedTable execute(CreateMode mode) {
        validateDeclaration();

        final MaterializedTableOperationBuilder operationBuilder =
                new MaterializedTableOperationBuilder(
                        tableEnvironment.getCatalogManager(),
                        tableEnvironment.getConfig().getRootConfiguration());
        final MaterializedTableDefinition definition =
                MaterializedTableDefinition.fromDescriptor(
                        queryOperation,
                        descriptor,
                        tableEnvironment.getCatalogManager(),
                        tableEnvironment.getConfig());

        final Operation operation =
                mode == CreateMode.CREATE_OR_FAIL
                        ? operationBuilder.buildCreate(identifier, definition)
                        : operationBuilder.buildCreateOrAlter(identifier, definition);

        tableEnvironment.executeInternal(operation);
        return new MaterializedTableImpl(tableEnvironment, identifier);
    }

    @Override
    public String toString() {
        return "Materialization[" + identifier.asSummaryString() + "]";
    }

    /**
     * Checks only what can be checked without the engine.
     *
     * <p>Everything that depends on the query — schema, partitioning, whether the refresh mode
     * suits the freshness — is left to the engine, which is the authority on it. Duplicating those
     * checks here would mean two implementations to keep in agreement, and the client's would be
     * the one that goes stale.
     */
    private void validateDeclaration() {
        final IntervalFreshness freshness =
                descriptor
                        .getFreshness()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Materialized table %s requires a freshness. Set one on the "
                                                                + "descriptor, or use materializeAs(path, Duration).",
                                                        identifier.asSummaryString())));
        if (descriptor.getRefreshMode() == LogicalRefreshMode.FULL) {
            // A full-mode refresh runs on a cron schedule, which cannot express every interval.
            IntervalFreshness.validateFreshnessForCron(freshness);
        }
    }
}

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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.MaterializedPipeline;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory.Mode;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Implementation of {@link MaterializedPipeline}. It serializes the query when it is constructed
 * and builds the operation against the catalog state at execute time.
 */
@Internal
public final class MaterializedPipelineImpl implements MaterializedPipeline {

    private final TableEnvironmentInternal tableEnvironment;
    private final QueryOperation queryOperation;
    private final String serializedQuery;
    private final ObjectIdentifier identifier;
    private final @Nullable IntervalFreshness freshness;

    public MaterializedPipelineImpl(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation queryOperation,
            ObjectIdentifier identifier,
            @Nullable IntervalFreshness freshness) {
        this.tableEnvironment =
                Preconditions.checkNotNull(tableEnvironment, "Table environment must not be null.");
        this.queryOperation =
                Preconditions.checkNotNull(queryOperation, "Query operation must not be null.");
        this.identifier = Preconditions.checkNotNull(identifier, "Identifier must not be null.");
        this.freshness = freshness;
        this.serializedQuery =
                MaterializedTableQuerySerializer.serialize(
                        queryOperation, tableEnvironment.getCatalogManager());
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public MaterializedTable execute() {
        tableEnvironment.executeInternal(buildOperation());
        return new MaterializedTableImpl(identifier);
    }

    private Operation buildOperation() {
        final CatalogManager catalogManager = tableEnvironment.getCatalogManager();
        if (catalogManager.isTemporaryTable(identifier)) {
            throw new ValidationException(
                    String.format(
                            "Cannot create or alter materialized table %s, because a temporary table or view with the same identifier exists and would shadow it. Drop the temporary object first.",
                            identifier.asSummaryString()));
        }
        final Optional<ResolvedCatalogBaseTable<?>> existing =
                catalogManager.getCatalogBaseTable(identifier);
        if (existing.isPresent() && existing.get().getTableKind() == TableKind.TABLE) {
            throw new ValidationException(
                    String.format(
                            "Cannot create or alter materialized table %s, because a regular table with the same identifier exists. Table#materializeInto does not convert regular tables.",
                            identifier.asSummaryString()));
        }
        return new MaterializedTableOperationFactory(catalogManager, tableEnvironment.getConfig())
                .build(
                        identifier,
                        Mode.CREATE_OR_ALTER,
                        () ->
                                new QueryMaterializedTableMergeContext(
                                        queryOperation, serializedQuery, freshness));
    }

    @Override
    public String toString() {
        return "MaterializedPipeline into '" + identifier.asSummaryString() + "'";
    }
}

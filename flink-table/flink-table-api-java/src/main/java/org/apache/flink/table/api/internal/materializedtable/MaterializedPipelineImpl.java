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
import org.apache.flink.table.api.CreateMode;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.MaterializedPipeline;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory.Mode;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;

/**
 * Implementation of {@link MaterializedPipeline}. It serializes the query when it is constructed,
 * and builds the operation against the catalog state at {@link #explain} or {@code execute} time.
 */
@Internal
public final class MaterializedPipelineImpl implements MaterializedPipeline {

    private final TableEnvironmentInternal tableEnvironment;
    private final QueryOperation queryOperation;
    private final String serializedQuery;
    private final ObjectIdentifier identifier;
    private final MaterializedTableDescriptor descriptor;

    public MaterializedPipelineImpl(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation queryOperation,
            ObjectIdentifier identifier,
            MaterializedTableDescriptor descriptor) {
        this.tableEnvironment =
                Preconditions.checkNotNull(tableEnvironment, "Table environment must not be null.");
        this.queryOperation =
                Preconditions.checkNotNull(queryOperation, "Query operation must not be null.");
        this.identifier = Preconditions.checkNotNull(identifier, "Identifier must not be null.");
        this.descriptor = Preconditions.checkNotNull(descriptor, "Descriptor must not be null.");
        this.serializedQuery =
                DescriptorMaterializedTableMergeContext.serializeQuery(
                        queryOperation, tableEnvironment.getCatalogManager());
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public MaterializedTable execute() {
        return submit(CreateMode.createOrAlter(), null);
    }

    @Override
    public MaterializedTable execute(StartMode startMode) {
        Preconditions.checkNotNull(startMode, "Start mode must not be null.");
        return submit(CreateMode.createOrAlter(), startMode);
    }

    @Override
    public MaterializedTable execute(CreateMode mode) {
        Preconditions.checkNotNull(mode, "Create mode must not be null.");
        return submit(mode, null);
    }

    @Override
    public MaterializedTable execute(CreateMode mode, StartMode startMode) {
        Preconditions.checkNotNull(mode, "Create mode must not be null.");
        Preconditions.checkNotNull(startMode, "Start mode must not be null.");
        return submit(mode, startMode);
    }

    @Override
    public String explain(ExplainFormat format, ExplainDetail... extraDetails) {
        return tableEnvironment.explainInternal(
                Collections.singletonList(buildOperation(CreateMode.createOrAlter(), null)),
                format,
                extraDetails);
    }

    private Operation buildOperation(CreateMode mode, @Nullable StartMode startMode) {
        Preconditions.checkNotNull(mode, "Create mode must not be null.");
        final Mode factoryMode = toFactoryMode(mode);
        final CatalogManager catalogManager = tableEnvironment.getCatalogManager();
        if (catalogManager.isTemporaryTable(identifier)) {
            throw new ValidationException(
                    String.format(
                            "Cannot create or alter materialized table %s, because a temporary table or view with the same identifier exists and would shadow it. Drop the temporary object first.",
                            identifier.asSummaryString()));
        }
        return new MaterializedTableOperationFactory(catalogManager, tableEnvironment.getConfig())
                .build(
                        identifier,
                        factoryMode,
                        () ->
                                new DescriptorMaterializedTableMergeContext(
                                        descriptor,
                                        queryOperation,
                                        serializedQuery,
                                        startMode,
                                        catalogManager,
                                        statementName(factoryMode)));
    }

    private MaterializedTable submit(CreateMode mode, @Nullable StartMode startMode) {
        tableEnvironment.executeInternal(buildOperation(mode, startMode));
        return new MaterializedTableImpl(identifier);
    }

    @Override
    public String toString() {
        return "MaterializedPipeline into '" + identifier.asSummaryString() + "'";
    }

    private static Mode toFactoryMode(CreateMode mode) {
        switch (mode.getKind()) {
            case CREATE_OR_ALTER:
                return Mode.CREATE_OR_ALTER;
            case FAIL_IF_EXISTS:
                return Mode.CREATE;
            default:
                throw new TableException("Unsupported create mode: " + mode);
        }
    }

    private static String statementName(Mode mode) {
        return mode == Mode.CREATE_OR_ALTER
                ? "CREATE OR ALTER MATERIALIZED TABLE"
                : "CREATE MATERIALIZED TABLE";
    }
}

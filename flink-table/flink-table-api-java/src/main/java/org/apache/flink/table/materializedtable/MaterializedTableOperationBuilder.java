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

package org.apache.flink.table.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.MaterializedTableSchemaChanges;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.ConvertTableToMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableChangeHandler;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;

/**
 * Turns a {@link MaterializedTableDefinition} into the operation that declares the materialized
 * table.
 *
 * <p>Extracted from the SQL converters so that the Table API declares materialized tables the same
 * way SQL does. Everything here works on {@link QueryOperation}s and catalog types; the {@code
 * SqlNode} handling stayed behind in the planner.
 */
@Internal
public class MaterializedTableOperationBuilder {

    private final CatalogManager catalogManager;
    private final ReadableConfig rootConfiguration;

    public MaterializedTableOperationBuilder(
            CatalogManager catalogManager, ReadableConfig rootConfiguration) {
        this.catalogManager = catalogManager;
        this.rootConfiguration = rootConfiguration;
    }

    /**
     * Builds the operation for a declaration that adapts to whatever already sits at the path:
     * another materialized table is altered in place, a regular table is converted, and nothing at
     * all is created.
     */
    public Operation buildCreateOrAlter(
            final ObjectIdentifier identifier, final MaterializedTableDefinition definition) {
        final Optional<ResolvedCatalogBaseTable<?>> resolvedBaseTable =
                catalogManager.getCatalogBaseTable(identifier);
        if (resolvedBaseTable.isEmpty()) {
            return buildCreate(identifier, definition);
        }
        final ResolvedCatalogBaseTable<?> oldBaseTable = resolvedBaseTable.get();
        final TableKind oldBaseTableKind = oldBaseTable.getTableKind();
        switch (oldBaseTableKind) {
            case MATERIALIZED_TABLE:
                return buildAlter(
                        identifier, definition, (ResolvedCatalogMaterializedTable) oldBaseTable);
            case TABLE:
                return buildConvert(identifier, definition, (ResolvedCatalogTable) oldBaseTable);
            default:
                throw new ValidationException(
                        String.format(
                                "Catalog object %s of kind %s does not support the CREATE OR ALTER MATERIALIZED TABLE operation.",
                                identifier.asSummaryString(), oldBaseTableKind));
        }
    }

    /** Builds the operation for a declaration that requires the path to be free. */
    public Operation buildCreate(
            final ObjectIdentifier identifier, final MaterializedTableDefinition definition) {
        final ResolvedCatalogMaterializedTable resolvedTable =
                buildResolvedCatalogMaterializedTable(definition);
        return new CreateMaterializedTableOperation(
                identifier, resolvedTable, definition.adaptQueryToTable(resolvedTable));
    }

    public ResolvedCatalogMaterializedTable buildResolvedCatalogMaterializedTable(
            final MaterializedTableDefinition definition) {
        final List<String> partitionKeys = definition.getPartitionKeys();
        final Map<String, String> tableOptions = definition.getOptions();
        verifyPartitioningColumnsExist(definition.getQuerySchema(), partitionKeys, tableOptions);

        return catalogManager.resolveCatalogMaterializedTable(
                CatalogMaterializedTable.newBuilder()
                        .schema(definition.getSchema())
                        .comment(definition.getComment())
                        .distribution(definition.getDistribution().orElse(null))
                        .partitionKeys(partitionKeys)
                        .options(tableOptions)
                        .originalQuery(definition.getOriginalQuery())
                        .expandedQuery(definition.getExpandedQuery())
                        .freshness(definition.getFreshness())
                        .logicalRefreshMode(definition.getLogicalRefreshMode())
                        .refreshMode(definition.getRefreshMode())
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .startMode(definition.getStartMode())
                        .build());
    }

    private Operation buildAlter(
            final ObjectIdentifier identifier,
            final MaterializedTableDefinition definition,
            final ResolvedCatalogMaterializedTable oldTable) {
        final SchemaResolver schemaResolver = catalogManager.getSchemaResolver();
        return new FullAlterMaterializedTableOperation(
                identifier,
                currentTable -> buildTableChanges(currentTable, definition, schemaResolver),
                oldTable,
                currentTable -> buildNewTable(currentTable, definition, schemaResolver),
                definition.getQueryOperation());
    }

    private Operation buildConvert(
            final ObjectIdentifier identifier,
            final MaterializedTableDefinition definition,
            final ResolvedCatalogTable oldBaseTable) {
        final boolean conversionEnabled =
                rootConfiguration.get(
                        TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED);
        if (!conversionEnabled) {
            throw new ValidationException(
                    "Regular table does not support create or alter operation.");
        }

        final CatalogMaterializedTable newMaterializedTable =
                CatalogMaterializedTable.newBuilder()
                        .schema(definition.getSchema())
                        .comment(definition.getComment())
                        .partitionKeys(definition.getPartitionKeys())
                        .options(definition.getOptions())
                        .originalQuery(definition.getOriginalQuery())
                        .expandedQuery(definition.getExpandedQuery())
                        .distribution(definition.getDistribution().orElse(null))
                        .freshness(definition.getFreshness())
                        .logicalRefreshMode(definition.getLogicalRefreshMode())
                        .refreshMode(definition.getRefreshMode())
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .startMode(definition.getStartMode())
                        .build();
        final ResolvedCatalogMaterializedTable resolvedNewMaterializedTable =
                catalogManager.resolveCatalogMaterializedTable(newMaterializedTable);

        return new ConvertTableToMaterializedTableOperation(
                identifier,
                oldBaseTable,
                resolvedNewMaterializedTable,
                resolvedCatalogMaterializedTable ->
                        buildConversionTableChanges(
                                oldBaseTable,
                                resolvedCatalogMaterializedTable,
                                definition.isSchemaDeclared(),
                                definition.isConstraintDeclared()),
                definition.adaptQueryToTable(resolvedNewMaterializedTable));
    }

    private List<TableChange> buildConversionTableChanges(
            final ResolvedCatalogTable oldTable,
            final ResolvedCatalogMaterializedTable newTable,
            final boolean hasSchemaDefinition,
            final boolean hasConstraintDefinition) {
        final ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        final ResolvedSchema newSchema = newTable.getResolvedSchema();
        final List<TableChange> changes =
                new ArrayList<>(
                        MaterializedTableSchemaChanges.validateAndExtractColumnChanges(
                                oldSchema, newSchema, hasSchemaDefinition));

        getConstraintChange(oldSchema, newSchema, hasConstraintDefinition).ifPresent(changes::add);
        getWatermarkChange(oldSchema, newSchema, hasSchemaDefinition).ifPresent(changes::add);

        changes.addAll(
                getQueryTableChanges(
                        null, null, newTable.getOriginalQuery(), newTable.getExpandedQuery()));
        changes.addAll(getOptionsTableChanges(oldTable.getOptions(), newTable.getOptions()));
        changes.addAll(
                getDistributionTableChanges(
                        oldTable.getDistribution().orElse(null),
                        newTable.getDistribution().orElse(null)));

        newTable.getStartMode()
                .ifPresent(newStartMode -> changes.add(TableChange.modifyStartMode(newStartMode)));

        return changes;
    }

    private CatalogMaterializedTable buildNewTable(
            final ResolvedCatalogMaterializedTable currentTable,
            final MaterializedTableDefinition definition,
            final SchemaResolver schemaResolver) {
        return CatalogMaterializedTable.newBuilder()
                .schema(
                        MaterializedTableChangeHandler.getHandlerWithChanges(
                                        currentTable,
                                        getSchemaTableChanges(
                                                definition, schemaResolver, currentTable))
                                .retrieveSchema())
                .comment(definition.getComment())
                .partitionKeys(definition.getPartitionKeys())
                .options(definition.getOptions())
                .originalQuery(definition.getOriginalQuery())
                .expandedQuery(definition.getExpandedQuery())
                .distribution(definition.getDistribution().orElse(null))
                .freshness(definition.getFreshness())
                .logicalRefreshMode(definition.getLogicalRefreshMode())
                .refreshMode(definition.getRefreshMode())
                .refreshStatus(currentTable.getRefreshStatus())
                .refreshHandlerDescription(currentTable.getRefreshHandlerDescription().orElse(null))
                .serializedRefreshHandler(currentTable.getSerializedRefreshHandler())
                .startMode(definition.getStartMode())
                .build();
    }

    private List<TableChange> buildTableChanges(
            final ResolvedCatalogMaterializedTable oldTable,
            final MaterializedTableDefinition definition,
            final SchemaResolver schemaResolver) {
        final List<TableChange> changes =
                getSchemaTableChanges(definition, schemaResolver, oldTable);

        changes.addAll(
                getQueryTableChanges(
                        oldTable.getOriginalQuery(),
                        oldTable.getExpandedQuery(),
                        definition.getOriginalQuery(),
                        definition.getExpandedQuery()));
        changes.addAll(getOptionsTableChanges(oldTable.getOptions(), definition.getOptions()));
        changes.addAll(
                getDistributionTableChanges(
                        oldTable.getDistribution().orElse(null),
                        definition.getDistribution().orElse(null)));

        final RefreshMode oldRefreshMode = oldTable.getRefreshMode();
        final RefreshMode newRefreshMode = definition.getRefreshMode();
        if (oldRefreshMode != newRefreshMode && newRefreshMode != null) {
            throw new ValidationException("Changing of REFRESH MODE is unsupported");
        }

        final StartMode newStartMode = definition.getStartMode();
        if (newStartMode != null) {
            final StartMode oldStartMode =
                    oldTable.getStartMode()
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    "Start mode must be set on materialized table."));
            if (!Objects.equals(oldStartMode, newStartMode)) {
                changes.add(TableChange.modifyStartMode(newStartMode));
            }
        }

        return changes;
    }

    private List<TableChange> getSchemaTableChanges(
            final MaterializedTableDefinition definition,
            final SchemaResolver schemaResolver,
            final ResolvedCatalogMaterializedTable oldTable) {
        final ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        final ResolvedSchema newSchema = schemaResolver.resolve(definition.getSchema());
        final boolean hasSchemaDefinition = definition.isSchemaDeclared();
        final List<TableChange> changes =
                new ArrayList<>(
                        MaterializedTableSchemaChanges.validateAndExtractColumnChanges(
                                oldSchema, newSchema, hasSchemaDefinition));

        getConstraintChange(oldSchema, newSchema, definition.isConstraintDeclared())
                .ifPresent(changes::add);
        getWatermarkChange(oldSchema, newSchema, hasSchemaDefinition).ifPresent(changes::add);
        return changes;
    }

    private List<TableChange> getDistributionTableChanges(
            final TableDistribution oldDistribution, final TableDistribution newDistribution) {
        if (!Objects.equals(oldDistribution, newDistribution)) {
            if (oldDistribution == null) {
                return List.of(TableChange.add(newDistribution));
            } else if (newDistribution == null) {
                return List.of(TableChange.dropDistribution());
            } else {
                return List.of(TableChange.modify(newDistribution));
            }
        }
        return List.of();
    }

    private List<TableChange> getOptionsTableChanges(
            final Map<String, String> oldOptions, final Map<String, String> newOptions) {
        final List<TableChange> changes = new ArrayList<>();

        for (Map.Entry<String, String> newOptionEntry : newOptions.entrySet()) {
            if (!newOptionEntry.getValue().equals(oldOptions.get(newOptionEntry.getKey()))) {
                changes.add(TableChange.set(newOptionEntry.getKey(), newOptionEntry.getValue()));
            }
        }

        for (Map.Entry<String, String> oldOptionEntry : oldOptions.entrySet()) {
            if (newOptions.get(oldOptionEntry.getKey()) == null) {
                changes.add(TableChange.reset(oldOptionEntry.getKey()));
            }
        }
        return changes;
    }

    private List<TableChange> getQueryTableChanges(
            final String oldOriginalQuery,
            final String oldExpandedQuery,
            final String newOriginalQuery,
            final String newExpandedQuery) {
        if (!Objects.equals(oldOriginalQuery, newOriginalQuery)
                || !Objects.equals(oldExpandedQuery, newExpandedQuery)) {
            return List.of(TableChange.modifyDefinitionQuery(newOriginalQuery, newExpandedQuery));
        }
        return List.of();
    }

    private Optional<TableChange> getConstraintChange(
            final ResolvedSchema oldSchema,
            final ResolvedSchema newSchema,
            final boolean hasConstraintDefinition) {
        final UniqueConstraint oldConstraint = oldSchema.getPrimaryKey().orElse(null);
        final UniqueConstraint newConstraint = newSchema.getPrimaryKey().orElse(null);
        if (hasConstraintDefinition && !Objects.equals(oldConstraint, newConstraint)) {
            if (newConstraint == null) {
                return Optional.of(TableChange.dropConstraint(oldConstraint.getName()));
            } else if (oldConstraint == null) {
                return Optional.of(TableChange.add(newConstraint));
            } else {
                return Optional.of(TableChange.modify(newConstraint));
            }
        }
        return Optional.empty();
    }

    private Optional<TableChange> getWatermarkChange(
            final ResolvedSchema oldSchema,
            final ResolvedSchema newSchema,
            boolean hasSchemaDefinition) {
        final WatermarkSpec oldWatermarkSpec =
                oldSchema.getWatermarkSpecs().isEmpty()
                        ? null
                        : oldSchema.getWatermarkSpecs().get(0);
        final WatermarkSpec newWatermarkSpec =
                newSchema.getWatermarkSpecs().isEmpty()
                        ? null
                        : newSchema.getWatermarkSpecs().get(0);
        if (hasSchemaDefinition && !Objects.equals(oldWatermarkSpec, newWatermarkSpec)) {
            if (newWatermarkSpec == null) {
                return Optional.of(TableChange.dropWatermark());
            } else if (oldWatermarkSpec == null) {
                return Optional.of(TableChange.add(newWatermarkSpec));
            } else {
                return Optional.of(TableChange.modify(newWatermarkSpec));
            }
        }
        return Optional.empty();
    }

    private void verifyPartitioningColumnsExist(
            ResolvedSchema schema, List<String> partitionKeys, Map<String, String> tableOptions) {
        final Set<String> partitionFieldOptions =
                tableOptions.keySet().stream()
                        .filter(k -> k.startsWith(PARTITION_FIELDS))
                        .collect(Collectors.toSet());

        for (String partitionKey : partitionKeys) {
            if (schema.getColumn(partitionKey).isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the query's schema. Available columns: [%s].",
                                partitionKey,
                                schema.getColumnNames().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }

        // verify partition key used by materialized table partition option
        // partition.fields.#.date-formatter whether exist
        for (String partitionOption : partitionFieldOptions) {
            String partitionKey =
                    partitionOption.substring(
                            PARTITION_FIELDS.length() + 1,
                            partitionOption.length() - (DATE_FORMATTER.length() + 1));
            // partition key used in option partition.fields.#.date-formatter must be existed
            if (!partitionKeys.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Column '%s' referenced by materialized table option '%s' isn't a partition column. Available partition columns: [%s].",
                                partitionKey,
                                partitionOption,
                                partitionKeys.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            // partition key used in option partition.fields.#.date-formatter must be string type
            LogicalType partitionKeyType =
                    schema.getColumn(partitionKey).get().getDataType().getLogicalType();
            if (!partitionKeyType
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                throw new ValidationException(
                        String.format(
                                "Materialized table option '%s' only supports referring to char, varchar and string type partition column. Column `%s` type is %s.",
                                partitionOption, partitionKey, partitionKeyType.asSummaryString()));
            }
        }
    }
}

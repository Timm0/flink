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
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.MaterializedTableConfigOptions;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.QueryOperation;

import javax.annotation.Nullable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The fully resolved definition of a materialized table: everything the user declared, already
 * merged with everything derived from the query.
 *
 * <p>This is the seam between the two front ends. SQL populates it from a {@code SqlNode}; the
 * Table API populates it from a {@link QueryOperation} and a {@link MaterializedTableDescriptor}
 * via {@link #fromDescriptor}. Neither flavour is visible to {@link
 * MaterializedTableOperationBuilder}, which is what lets one implementation serve both.
 *
 * <p>Deliberately a value rather than an interface: both front ends differ only in where the values
 * come from, which is what a constructor is for. Populating it eagerly also forces the SQL side's
 * ordering to be explicit, which matters — reading the expanded query mutates the {@code SqlNode},
 * so the original has to be read first.
 *
 * <p>The one exception is {@link #getSchema()}, which stays deferred; see its javadoc.
 */
@Internal
public class MaterializedTableDefinition {

    private final boolean schemaDeclared;
    private final boolean constraintDeclared;
    private final Supplier<Schema> schema;
    private final ResolvedSchema querySchema;
    private final QueryOperation queryOperation;
    private final Map<String, String> options;
    private final List<String> partitionKeys;
    private final @Nullable TableDistribution distribution;
    private final String originalQuery;
    private final String expandedQuery;
    private final LogicalRefreshMode logicalRefreshMode;
    private final @Nullable StartMode startMode;
    private final @Nullable String comment;
    private final @Nullable IntervalFreshness freshness;
    private final QueryAdapter queryAdapter;

    private MaterializedTableDefinition(Builder builder) {
        this.schemaDeclared = builder.schemaDeclared;
        this.constraintDeclared = builder.constraintDeclared;
        this.schema = Objects.requireNonNull(builder.schema, "schema");
        this.querySchema = Objects.requireNonNull(builder.querySchema, "querySchema");
        this.queryOperation = Objects.requireNonNull(builder.queryOperation, "queryOperation");
        this.options = Objects.requireNonNull(builder.options, "options");
        this.partitionKeys = Objects.requireNonNull(builder.partitionKeys, "partitionKeys");
        this.distribution = builder.distribution;
        this.originalQuery = Objects.requireNonNull(builder.originalQuery, "originalQuery");
        this.expandedQuery = Objects.requireNonNull(builder.expandedQuery, "expandedQuery");
        this.logicalRefreshMode =
                Objects.requireNonNull(builder.logicalRefreshMode, "logicalRefreshMode");
        this.startMode = builder.startMode;
        this.comment = builder.comment;
        this.freshness = builder.freshness;
        this.queryAdapter = builder.queryAdapter;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Resolves a Table API declaration into a definition.
     *
     * <p>The query defines the physical columns; a declared schema may only add what the query
     * cannot express — watermarks, a primary key, and computed or metadata columns. Re-declaring a
     * column the query already produces is rejected rather than merged, because reconciling two
     * definitions of one column is what SQL's column list does and there is no Table API
     * equivalent.
     */
    public static MaterializedTableDefinition fromDescriptor(
            QueryOperation queryOperation,
            MaterializedTableDescriptor descriptor,
            CatalogManager catalogManager,
            ReadableConfig tableConfig) {
        final ResolvedSchema querySchema = queryOperation.getResolvedSchema();
        // A materialized table is a persisted query definition, so it has to survive the process
        // that declared it — a later resume has nothing but this text. The Table API has no
        // separate
        // "as written" form, so the original and expanded queries coincide.
        final String serializedQuery = serializeQuery(queryOperation, catalogManager);

        return newBuilder()
                .schemaDeclared(
                        declaredColumns(descriptor).stream()
                                .anyMatch(c -> c instanceof UnresolvedPhysicalColumn))
                .constraintDeclared(
                        descriptor.getSchema().flatMap(Schema::getPrimaryKey).isPresent()
                                || declaredColumns(descriptor).stream()
                                        .anyMatch(c -> c instanceof UnresolvedPhysicalColumn))
                .schema(mergeSchema(querySchema, descriptor))
                .querySchema(querySchema)
                .queryOperation(queryOperation)
                .options(descriptor.getOptions())
                .partitionKeys(descriptor.getPartitionKeys())
                .distribution(descriptor.getDistribution().orElse(null))
                .originalQuery(serializedQuery)
                .expandedQuery(serializedQuery)
                .logicalRefreshMode(descriptor.getRefreshMode())
                .startMode(
                        descriptor
                                .getStartMode()
                                .orElseGet(
                                        () ->
                                                StartMode.of(
                                                        tableConfig.get(
                                                                MaterializedTableConfigOptions
                                                                        .MATERIALIZED_TABLE_DEFAULT_START_MODE))))
                .comment(descriptor.getComment().orElse(null))
                .freshness(descriptor.getFreshness().orElse(null))
                .build();
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Whether the user spelled out a column list, as opposed to letting the query define the
     * schema. Drives whether a column absent from the new schema is a drop or merely a column the
     * query no longer projects.
     */
    public boolean isSchemaDeclared() {
        return schemaDeclared;
    }

    // Separate from the schema flag because SQL allows constraints to be declared on their own.
    public boolean isConstraintDeclared() {
        return constraintDeclared;
    }

    /**
     * The declared schema merged with the query's.
     *
     * <p>Resolved on each call rather than up front, because merging validates the declared columns
     * against the query and {@code FullAlterMaterializedTableOperation} promises that all such
     * validation happens when the operation executes, not when it is built.
     */
    public Schema getSchema() {
        return schema.get();
    }

    public ResolvedSchema getQuerySchema() {
        return querySchema;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Optional<TableDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public String getExpandedQuery() {
        return expandedQuery;
    }

    public LogicalRefreshMode getLogicalRefreshMode() {
        return logicalRefreshMode;
    }

    /** The physical refresh mode, or {@code null} when the choice is left to the engine. */
    public @Nullable RefreshMode getRefreshMode() {
        return toRefreshMode(logicalRefreshMode);
    }

    /**
     * Resolves a declared refresh mode into the physical one, or {@code null} for {@link
     * LogicalRefreshMode#AUTOMATIC}, which leaves the choice to the engine.
     */
    public static @Nullable RefreshMode toRefreshMode(LogicalRefreshMode logicalRefreshMode) {
        switch (logicalRefreshMode) {
            case AUTOMATIC:
                return null;
            case FULL:
                return RefreshMode.FULL;
            case CONTINUOUS:
                return RefreshMode.CONTINUOUS;
            default:
                throw new IllegalArgumentException(
                        "Unknown logical refresh mode: " + logicalRefreshMode);
        }
    }

    public @Nullable StartMode getStartMode() {
        return startMode;
    }

    public @Nullable String getComment() {
        return comment;
    }

    public @Nullable IntervalFreshness getFreshness() {
        return freshness;
    }

    /**
     * Adapts the query to the sink schema of a freshly built table — projecting, reordering and
     * null-padding columns so the insert lines up.
     *
     * <p>Only the create and convert paths need this; an alter keeps the query as-is, because its
     * sink schema is derived from that same query. Without an adapter this is a no-op, which is
     * correct whenever the schema was derived from the query rather than declared alongside it.
     */
    public QueryOperation adaptQueryToTable(ResolvedCatalogMaterializedTable resolvedTable) {
        return queryAdapter.adapt(queryOperation, resolvedTable);
    }

    // ---------------------------------------------------------------------------------------------

    /** Rewrites the query so it lines up with the materialized table it will be inserted into. */
    @FunctionalInterface
    @Internal
    public interface QueryAdapter {
        QueryOperation adapt(QueryOperation query, ResolvedCatalogMaterializedTable table);
    }

    private static List<UnresolvedColumn> declaredColumns(MaterializedTableDescriptor descriptor) {
        return descriptor.getSchema().map(Schema::getColumns).orElse(List.of());
    }

    private static Schema mergeSchema(
            ResolvedSchema querySchema, MaterializedTableDescriptor descriptor) {
        final Schema.Builder builder = Schema.newBuilder().fromResolvedSchema(querySchema);
        final Optional<Schema> declared = descriptor.getSchema();
        if (declared.isEmpty()) {
            return builder.build();
        }

        final Set<String> queryColumns = new LinkedHashSet<>(querySchema.getColumnNames());
        final List<UnresolvedColumn> collisions =
                declared.get().getColumns().stream()
                        .filter(column -> queryColumns.contains(column.getName()))
                        .collect(Collectors.toList());
        if (!collisions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Column%s [%s] %s already defined by the query and cannot be redeclared on the "
                                    + "materialized table descriptor. Declare only what the query cannot express: "
                                    + "watermarks, a primary key, and computed or metadata columns.",
                            collisions.size() == 1 ? "" : "s",
                            collisions.stream()
                                    .map(UnresolvedColumn::getName)
                                    .collect(Collectors.joining("', '", "'", "'")),
                            collisions.size() == 1 ? "is" : "are"));
        }

        builder.fromColumns(declared.get().getColumns());
        for (UnresolvedWatermarkSpec watermark : declared.get().getWatermarkSpecs()) {
            builder.watermark(watermark.getColumnName(), watermark.getWatermarkExpression());
        }
        declared.get()
                .getPrimaryKey()
                .ifPresent(
                        primaryKey ->
                                builder.primaryKeyNamed(
                                        primaryKey.getConstraintName(),
                                        primaryKey.getColumnNames()));
        return builder.build();
    }

    private static String serializeQuery(
            QueryOperation queryOperation, CatalogManager catalogManager) {
        try {
            return queryOperation.asSerializableString(catalogManager.getSqlFactory());
        } catch (Exception e) {
            throw new ValidationException(
                    "Cannot declare a materialized table on this query: it has no SQL representation, "
                            + "so it could not be persisted in the catalog. Queries reading from a "
                            + "DataStream, from an unregistered table, or calling an inline function cannot "
                            + "define a materialized table — register the source and the functions first.",
                    e);
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link MaterializedTableDefinition}. */
    @Internal
    public static class Builder {

        private boolean schemaDeclared;
        private boolean constraintDeclared;
        private Supplier<Schema> schema;
        private ResolvedSchema querySchema;
        private QueryOperation queryOperation;
        private Map<String, String> options = Map.of();
        private List<String> partitionKeys = List.of();
        private @Nullable TableDistribution distribution;
        private String originalQuery;
        private String expandedQuery;
        private LogicalRefreshMode logicalRefreshMode = LogicalRefreshMode.AUTOMATIC;
        private @Nullable StartMode startMode;
        private @Nullable String comment;
        private @Nullable IntervalFreshness freshness;
        private QueryAdapter queryAdapter = (query, table) -> query;

        private Builder() {}

        public Builder schemaDeclared(boolean schemaDeclared) {
            this.schemaDeclared = schemaDeclared;
            return this;
        }

        public Builder constraintDeclared(boolean constraintDeclared) {
            this.constraintDeclared = constraintDeclared;
            return this;
        }

        /** The schema, already known. */
        public Builder schema(Schema schema) {
            return schema(() -> schema);
        }

        /**
         * The schema, resolved on demand — for front ends whose merge validates, and which must
         * therefore defer that validation to operation execution.
         */
        public Builder schema(Supplier<Schema> schema) {
            this.schema = schema;
            return this;
        }

        public Builder querySchema(ResolvedSchema querySchema) {
            this.querySchema = querySchema;
            return this;
        }

        public Builder queryOperation(QueryOperation queryOperation) {
            this.queryOperation = queryOperation;
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public Builder partitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public Builder distribution(@Nullable TableDistribution distribution) {
            this.distribution = distribution;
            return this;
        }

        public Builder originalQuery(String originalQuery) {
            this.originalQuery = originalQuery;
            return this;
        }

        public Builder expandedQuery(String expandedQuery) {
            this.expandedQuery = expandedQuery;
            return this;
        }

        public Builder logicalRefreshMode(LogicalRefreshMode logicalRefreshMode) {
            this.logicalRefreshMode = logicalRefreshMode;
            return this;
        }

        public Builder startMode(@Nullable StartMode startMode) {
            this.startMode = startMode;
            return this;
        }

        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        public Builder freshness(@Nullable IntervalFreshness freshness) {
            this.freshness = freshness;
            return this;
        }

        public Builder queryAdapter(QueryAdapter queryAdapter) {
            this.queryAdapter = queryAdapter;
            return this;
        }

        public MaterializedTableDefinition build() {
            return new MaterializedTableDefinition(this);
        }
    }
}

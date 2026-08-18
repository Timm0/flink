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

package org.apache.flink.table.planner.operations.converters.materializedtable;

import org.apache.flink.sql.parser.ddl.materializedtable.SqlCreateMaterializedTable;
import org.apache.flink.table.api.config.MaterializedTableConfigOptions;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.materializedtable.MaterializedTableDefinition;
import org.apache.flink.table.materializedtable.MaterializedTableOperationBuilder;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConvertUtils;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for converting {@link SqlCreateMaterializedTable} and it's children to create
 * materialized table operations.
 */
public abstract class AbstractCreateMaterializedTableConverter<T extends SqlCreateMaterializedTable>
        implements SqlNodeConverter<T> {
    /**
     * Resolves the statement into the front-end-neutral definition that {@link
     * MaterializedTableOperationBuilder} builds operations from.
     */
    protected abstract MaterializedTableDefinition buildDefinition(
            T sqlCreateMaterializedTable, ConvertContext context);

    protected final Optional<TableDistribution> getDerivedTableDistribution(
            T sqlCreateMaterializedTable) {
        return Optional.ofNullable(sqlCreateMaterializedTable.getDistribution())
                .map(OperationConverterUtils::getDistributionFromSqlDistribution);
    }

    protected final List<String> getDerivedPartitionKeys(T sqlCreateMaterializedTable) {
        return sqlCreateMaterializedTable.getPartitionKeyList();
    }

    protected final IntervalFreshness getDerivedFreshness(T sqlCreateMaterializedTable) {
        return Optional.ofNullable(sqlCreateMaterializedTable.getFreshness())
                .map(MaterializedTableUtils::getMaterializedTableFreshness)
                .orElse(null);
    }

    protected final StartMode getStartMode(T sqlCreateMaterializedTable, ConvertContext context) {
        StartMode startMode =
                MaterializedTableUtils.getStartMode(sqlCreateMaterializedTable.getStartMode());
        if (startMode != null) {
            return startMode;
        }
        return StartMode.of(
                context.getTableConfig()
                        .get(MaterializedTableConfigOptions.MATERIALIZED_TABLE_DEFAULT_START_MODE));
    }

    protected final PlannerQueryOperation getAsQueryOperation(
            T sqlCreateMaterializedTable, ConvertContext context) {
        final SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        final SqlNode validateQuery = context.getSqlValidator().validate(selectQuery);
        return new PlannerQueryOperation(
                context.toRelRoot(validateQuery).project(),
                () -> context.toQuotedSqlString(validateQuery));
    }

    protected final LogicalRefreshMode getDerivedLogicalRefreshMode(T sqlCreateMaterializedTable) {
        return MaterializedTableUtils.deriveLogicalRefreshMode(
                sqlCreateMaterializedTable.getRefreshMode());
    }

    protected final RefreshMode getDerivedRefreshMode(LogicalRefreshMode logicalRefreshMode) {
        return MaterializedTableUtils.fromLogicalRefreshModeToRefreshMode(logicalRefreshMode);
    }

    /**
     * Returns the user's original {@code AS} query text, sliced verbatim from the statement so
     * formatting and identifier casing are preserved (e.g. {@code int} is not normalized to {@code
     * INTEGER}). A comment placed between {@code AS} and the query is kept; the {@code AS} keyword
     * itself is excluded.
     */
    protected final String getDerivedOriginalQuery(
            T sqlCreateMaterializedTable, ConvertContext context) {
        return SqlNodeConvertUtils.extractOriginalAsQueryText(
                        context, sqlCreateMaterializedTable.getAsQueryKeywordPos())
                .orElse(context.toQuotedSqlString(sqlCreateMaterializedTable.getAsQuery()));
    }

    protected final String getDerivedExpandedQuery(
            T sqlCreateMaterializedTable, ConvertContext context) {
        SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        SqlNode validatedQuery = context.getSqlValidator().validate(selectQuery);
        return context.expandSqlIdentifiers(context.toQuotedSqlString(validatedQuery));
    }

    protected final String getComment(T sqlCreateMaterializedTable) {
        return sqlCreateMaterializedTable.getComment();
    }

    protected final ObjectIdentifier getIdentifier(
            SqlCreateMaterializedTable node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.getFullName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    protected final MaterializedTableOperationBuilder getOperationBuilder(ConvertContext context) {
        return new MaterializedTableOperationBuilder(
                context.getCatalogManager(), context.getTableConfig().getRootConfiguration());
    }
}

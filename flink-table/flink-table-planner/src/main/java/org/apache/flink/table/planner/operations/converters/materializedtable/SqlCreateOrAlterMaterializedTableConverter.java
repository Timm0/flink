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

import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlCreateOrAlterMaterializedTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.materializedtable.MaterializedTableDefinition;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.MergeTableAsUtil;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashSet;
import java.util.Set;

/**
 * A converter for {@link SqlCreateOrAlterMaterializedTable}.
 *
 * <p>Only the {@code SqlNode} handling lives here. Once the statement has been reduced to a {@link
 * MaterializedTableDefinition}, the operation is built by the shared builder — the same one the
 * Table API uses, so both front ends produce identical operations.
 */
public class SqlCreateOrAlterMaterializedTableConverter
        extends AbstractCreateMaterializedTableConverter<SqlCreateOrAlterMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        final ObjectIdentifier identifier =
                getIdentifier(sqlCreateOrAlterMaterializedTable, context);
        final MaterializedTableDefinition definition =
                buildDefinition(sqlCreateOrAlterMaterializedTable, context);

        if (createOrAlterOperation(sqlCreateOrAlterMaterializedTable)) {
            return getOperationBuilder(context).buildCreateOrAlter(identifier, definition);
        }
        return getOperationBuilder(context).buildCreate(identifier, definition);
    }

    private static boolean createOrAlterOperation(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable) {
        return sqlCreateOrAlterMaterializedTable.getOperator()
                == SqlCreateOrAlterMaterializedTable.CREATE_OR_ALTER_OPERATOR;
    }

    @Override
    protected MaterializedTableDefinition buildDefinition(
            final SqlCreateOrAlterMaterializedTable sqlCreateMaterializedTable,
            final ConvertContext context) {
        final MergeTableAsUtil mergeTableAsUtil = new MergeTableAsUtil(context);

        // Read the original query text first: validating the query below rewrites the SqlNode in
        // place, after which getAsQuery() only ever yields the expanded form.
        final String originalQuery = getDerivedOriginalQuery(sqlCreateMaterializedTable, context);

        final PlannerQueryOperation asQueryOperation =
                getAsQueryOperation(sqlCreateMaterializedTable, context);
        final ResolvedSchema querySchema = asQueryOperation.getResolvedSchema();

        final SqlNodeList columnList = sqlCreateMaterializedTable.getColumnList();
        final boolean schemaDeclared =
                !columnList.getList().isEmpty()
                        && columnList.getList().get(0) instanceof SqlRegularColumn;

        return MaterializedTableDefinition.newBuilder()
                .schemaDeclared(schemaDeclared)
                // The syntax allows constraints to be declared without a full column list.
                .constraintDeclared(
                        !sqlCreateMaterializedTable.getTableConstraints().isEmpty()
                                || schemaDeclared)
                // Deferred: merging validates the declared columns against the query, and an
                // alter must fail when the operation executes rather than when it is built.
                .schema(
                        () ->
                                mergeSchema(
                                        sqlCreateMaterializedTable,
                                        mergeTableAsUtil,
                                        columnList,
                                        querySchema))
                .querySchema(querySchema)
                .queryOperation(asQueryOperation)
                .options(sqlCreateMaterializedTable.getProperties())
                .partitionKeys(sqlCreateMaterializedTable.getPartitionKeyList())
                .distribution(getDerivedTableDistribution(sqlCreateMaterializedTable).orElse(null))
                .originalQuery(originalQuery)
                .expandedQuery(getDerivedExpandedQuery(sqlCreateMaterializedTable, context))
                .logicalRefreshMode(getDerivedLogicalRefreshMode(sqlCreateMaterializedTable))
                .startMode(getStartMode(sqlCreateMaterializedTable, context))
                .comment(getComment(sqlCreateMaterializedTable))
                .freshness(getDerivedFreshness(sqlCreateMaterializedTable))
                .queryAdapter(
                        (query, table) ->
                                mergeTableAsUtil.maybeRewriteQuery(
                                        asQueryOperation,
                                        sqlCreateMaterializedTable.getAsQuery(),
                                        table))
                .build();
    }

    private Schema mergeSchema(
            final SqlCreateOrAlterMaterializedTable sqlCreateMaterializedTable,
            final MergeTableAsUtil mergeTableAsUtil,
            final SqlNodeList columnList,
            final ResolvedSchema querySchema) {
        if (createOrAlterOperation(sqlCreateMaterializedTable)) {
            MaterializedTableUtils.validatePersistedColumnsUsedByQuery(columnList, querySchema);
        } else {
            validatePhysicalColumnsUsedByQuery(columnList, querySchema);
        }
        if (sqlCreateMaterializedTable.isSchemaWithColumnsIdentifiersOnly()) {
            // If only column identifiers are provided, then these are used to
            // order the columns in the schema.
            return mergeTableAsUtil.reorderSchema(columnList, querySchema);
        }
        return mergeTableAsUtil.mergeSchemas(
                columnList,
                sqlCreateMaterializedTable.getWatermark().orElse(null),
                sqlCreateMaterializedTable.getFullConstraints(),
                querySchema);
    }

    private static void validatePhysicalColumnsUsedByQuery(
            SqlNodeList sqlNodeList, ResolvedSchema querySchema) {
        final Set<String> querySchemaColumnNames = new HashSet<>(querySchema.getColumnNames());
        for (SqlNode column : sqlNodeList) {
            if (!(column instanceof SqlRegularColumn)) {
                continue;
            }
            final SqlRegularColumn physicalColumn = (SqlRegularColumn) column;
            if (!querySchemaColumnNames.contains(physicalColumn.getName().getSimple())) {
                throw new ValidationException(
                        String.format(
                                "Invalid as physical column '%s' is defined in the DDL, but is not used in a query column.",
                                physicalColumn.getName().getSimple()));
            }
        }
    }
}

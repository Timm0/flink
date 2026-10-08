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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.CorrelatedFunctionQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.FunctionQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.PartitionQueryOperation;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.ValuesQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Turns the query of {@code Table#materializeInto} into the SQL text a materialized table stores.
 * The refresh job re-runs that text, also from other sessions, so the query must be expressible in
 * SQL and reference permanent catalog objects only. Rejected queries fail before anything is
 * written.
 */
final class MaterializedTableQuerySerializer {

    private static final String TEMPORARY_OBJECT_MESSAGE =
            "The query of a materialized table must only reference permanent catalog objects. The"
                    + " refresh job re-runs the stored query text, also from other sessions, which"
                    + " cannot see the temporary %s %s.";

    private MaterializedTableQuerySerializer() {}

    static String serialize(QueryOperation queryOperation, CatalogManager catalogManager) {
        rejectTemporaryObjects(queryOperation);
        try {
            return queryOperation.asSerializableString(catalogManager.getSqlFactory());
        } catch (UnsupportedOperationException | TableException | ValidationException e) {
            final String cause = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            throw new ValidationException(
                    "The query of a materialized table must be expressible in SQL. The refresh job"
                            + " re-runs the stored query text, which rules out, for example,"
                            + " functions that are not registered in a catalog, tables created from"
                            + " a TableDescriptor and tables backed by a DataStream. Cause: "
                            + cause,
                    e);
        }
    }

    /**
     * Rejects named temporary objects only. Anonymous objects, such as inline function instances
     * and tables created from a {@code TableDescriptor}, report themselves as temporary too, but
     * have no name another session could look up. They are left to the serialization, which rejects
     * them; a custom {@code SqlFactory} can make inline functions storable.
     */
    private static void rejectTemporaryObjects(QueryOperation queryOperation) {
        visitOperation(queryOperation);
    }

    private static void visitOperation(QueryOperation operation) {
        if (operation instanceof SourceQueryOperation) {
            final ContextResolvedTable table =
                    ((SourceQueryOperation) operation).getContextResolvedTable();
            if (table.isTemporary() && !table.isAnonymous()) {
                throw temporaryObject("table or view", table.getIdentifier().asSummaryString());
            }
        } else if (operation instanceof CorrelatedFunctionQueryOperation) {
            visitFunction(((CorrelatedFunctionQueryOperation) operation).getResolvedFunction());
        } else if (operation instanceof FunctionQueryOperation) {
            visitFunction(((FunctionQueryOperation) operation).getResolvedFunction());
        }
        expressionsOf(operation).forEach(MaterializedTableQuerySerializer::visitExpression);
        operation.getChildren().forEach(MaterializedTableQuerySerializer::visitOperation);
    }

    private static void visitFunction(ContextResolvedFunction function) {
        if (function.isTemporary() && function.getIdentifier().isPresent()) {
            throw temporaryObject("function", function.getIdentifier().get().asSummaryString());
        }
    }

    private static void visitExpression(ResolvedExpression expression) {
        if (expression instanceof CallExpression) {
            final CallExpression call = (CallExpression) expression;
            if (call.isTemporary() && call.getFunctionIdentifier().isPresent()) {
                throw temporaryObject(
                        "function", call.getFunctionIdentifier().get().asSummaryString());
            }
        } else if (expression instanceof TableReferenceExpression) {
            visitOperation(((TableReferenceExpression) expression).getQueryOperation());
        }
        expression.getResolvedChildren().forEach(MaterializedTableQuerySerializer::visitExpression);
    }

    private static List<ResolvedExpression> expressionsOf(QueryOperation operation) {
        if (operation instanceof CorrelatedFunctionQueryOperation) {
            return ((CorrelatedFunctionQueryOperation) operation).getArguments();
        } else if (operation instanceof FunctionQueryOperation) {
            return ((FunctionQueryOperation) operation).getArguments();
        } else if (operation instanceof ProjectQueryOperation) {
            return ((ProjectQueryOperation) operation).getProjectList();
        } else if (operation instanceof FilterQueryOperation) {
            return List.of(((FilterQueryOperation) operation).getCondition());
        } else if (operation instanceof AggregateQueryOperation) {
            final AggregateQueryOperation aggregate = (AggregateQueryOperation) operation;
            return concat(aggregate.getGroupingExpressions(), aggregate.getAggregateExpressions());
        } else if (operation instanceof WindowAggregateQueryOperation) {
            final WindowAggregateQueryOperation aggregate =
                    (WindowAggregateQueryOperation) operation;
            return concat(
                    aggregate.getGroupingExpressions(),
                    aggregate.getAggregateExpressions(),
                    aggregate.getWindowPropertiesExpressions());
        } else if (operation instanceof JoinQueryOperation) {
            return List.of(((JoinQueryOperation) operation).getCondition());
        } else if (operation instanceof SortQueryOperation) {
            return ((SortQueryOperation) operation).getOrder();
        } else if (operation instanceof ValuesQueryOperation) {
            return ((ValuesQueryOperation) operation)
                    .getValues().stream().flatMap(List::stream).collect(Collectors.toList());
        } else if (operation instanceof PartitionQueryOperation) {
            return ((PartitionQueryOperation) operation).getOrderExpressions();
        }
        return List.of();
    }

    @SafeVarargs
    private static List<ResolvedExpression> concat(List<ResolvedExpression>... expressionLists) {
        return Stream.of(expressionLists).flatMap(List::stream).collect(Collectors.toList());
    }

    private static ValidationException temporaryObject(String kind, String name) {
        return new ValidationException(String.format(TEMPORARY_OBJECT_MESSAGE, kind, name));
    }
}

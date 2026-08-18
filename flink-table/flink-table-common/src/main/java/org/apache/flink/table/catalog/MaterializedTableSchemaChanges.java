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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Diffs two materialized table schemas into {@link TableChange}s.
 *
 * <p>Lifted out of the planner unchanged so that both the SQL converters and the Table API can
 * reach it: the logic never touched Calcite, and everything it works on ({@link ResolvedSchema},
 * {@link TableChange}, {@link Column}) already lives here.
 */
@Internal
public class MaterializedTableSchemaChanges {

    private MaterializedTableSchemaChanges() {}

    public static List<TableChange> validateAndExtractColumnChanges(
            ResolvedSchema oldSchema, ResolvedSchema newSchema, boolean schemaDefinedInQuery) {
        final List<Column> oldColumns = oldSchema.getColumns();
        final Map<String, Tuple2<Column, Integer>> oldByName = new HashMap<>();
        for (int i = 0; i < oldColumns.size(); i++) {
            oldByName.put(oldColumns.get(i).getName(), Tuple2.of(oldColumns.get(i), i));
        }
        final Set<String> seen = new HashSet<>();
        final List<Column> newColumns = newSchema.getColumns();
        final List<TableChange> changes = new ArrayList<>();
        for (int newIndex = 0; newIndex < newColumns.size(); newIndex++) {
            final Column newColumn = newColumns.get(newIndex);
            seen.add(newColumn.getName());
            final Tuple2<Column, Integer> oldEntry = oldByName.get(newColumn.getName());
            if (oldEntry == null) {
                changes.add(addChange(newColumn, schemaDefinedInQuery));
                continue;
            }
            final Column oldColumn = oldEntry.f0;
            // No position diff: DDL order is arbitrary; query-driven reorders are caught by
            // buildSchemaTableChanges on the ALTER MT AS path.
            if (oldColumn.isPhysical()
                    && newColumn.isPhysical()
                    && typeChanged(oldColumn, newColumn, schemaDefinedInQuery)) {
                final DataType newType =
                        schemaDefinedInQuery
                                ? newColumn.getDataType()
                                : newColumn.getDataType().nullable();
                changes.add(TableChange.modifyPhysicalColumnType(oldColumn, newType));
                // Type changed; still check whether the comment also changed.
                final String oldComment = oldColumn.getComment().orElse(null);
                final String newComment = newColumn.getComment().orElse(null);
                if (!Objects.equals(oldComment, newComment)) {
                    changes.add(TableChange.modifyColumnComment(oldColumn, newComment));
                }
                continue;
            }
            if (oldColumn.getClass() != newColumn.getClass()
                    || !definitionEquals(oldColumn, newColumn)) {
                changes.add(
                        new TableChange.ModifyColumn(
                                oldColumn,
                                normalizedColumn(newColumn, schemaDefinedInQuery),
                                null));
                continue;
            }
            final String oldComment = oldColumn.getComment().orElse(null);
            final String newComment = newColumn.getComment().orElse(null);
            if (!Objects.equals(oldComment, newComment)) {
                changes.add(TableChange.modifyColumnComment(oldColumn, newComment));
            }
        }

        for (Map.Entry<String, Tuple2<Column, Integer>> entry : oldByName.entrySet()) {
            if (seen.contains(entry.getKey())) {
                continue;
            }
            // Without an explicit DDL column list the new schema only reflects the query
            // projection, so old non-persisted columns are retained, not dropped.
            if (!schemaDefinedInQuery && !entry.getValue().f0.isPersisted()) {
                continue;
            }
            changes.add(TableChange.dropColumn(entry.getKey()));
        }
        return changes;
    }

    private static TableChange.AddColumn addChange(Column column, boolean schemaDefinedInQuery) {
        return TableChange.add(normalizedColumn(column, schemaDefinedInQuery));
    }

    private static Column normalizedColumn(Column column, boolean schemaDefinedInQuery) {
        return schemaDefinedInQuery ? column : column.copy(column.getDataType().nullable());
    }

    private static boolean definitionEquals(Column oldColumn, Column newColumn) {
        if (oldColumn instanceof MetadataColumn && newColumn instanceof MetadataColumn) {
            final MetadataColumn oldMeta = (MetadataColumn) oldColumn;
            final MetadataColumn newMeta = (MetadataColumn) newColumn;
            return oldMeta.isVirtual() == newMeta.isVirtual()
                    && Objects.equals(
                            oldMeta.getMetadataKey().orElse(null),
                            newMeta.getMetadataKey().orElse(null))
                    && oldMeta.getDataType().equals(newMeta.getDataType());
        }
        if (oldColumn instanceof ComputedColumn && newColumn instanceof ComputedColumn) {
            return Objects.equals(
                    ((ComputedColumn) oldColumn).getExpression(),
                    ((ComputedColumn) newColumn).getExpression());
        }
        return true;
    }

    private static boolean typeChanged(
            Column oldColumn, Column newColumn, boolean schemaDefinedInQuery) {
        final DataType oldType = oldColumn.getDataType();
        final DataType newType = newColumn.getDataType();
        // schemaDefinedInQuery=false: schema is inferred from the query, which may flip
        // nullability without intent — only the base type difference is a real change.
        return schemaDefinedInQuery
                ? !oldType.equals(newType)
                : !oldType.nullable().equals(newType.nullable());
    }
}

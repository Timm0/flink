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

package org.apache.flink.table.operations.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsQuerySchemaMergerTest {

    private static final DataTypeFactory DATA_TYPE_FACTORY = new DataTypeFactoryMock();

    @Test
    void withoutDeclaredColumnsTheSourceColumnsAreKept() {
        AsQuerySchemaMerger merger = merger();

        assertThat(names(merger.getMergedColumns())).containsExactly("a", "b");
    }

    @Test
    void declaredOnlyColumnsComeFirstInDeclarationOrder() {
        AsQuerySchemaMerger merger = merger();
        merger.addDeclaredColumn(computed("c"), 0);
        merger.addDeclaredColumn(
                new UnresolvedMetadataColumn("m", DataTypes.STRING(), null, true), 1);

        assertThat(names(merger.getMergedColumns())).containsExactly("c", "m", "a", "b");
    }

    @Test
    void sameNameDeclaredColumnOverridesTheSourceColumnInPlace() {
        AsQuerySchemaMerger merger = merger();
        merger.addDeclaredColumn(
                new UnresolvedPhysicalColumn("b", DataTypes.STRING(), "declared"), 0);
        merger.addDeclaredColumn(new UnresolvedPhysicalColumn("a", DataTypes.BIGINT()), 1);

        List<UnresolvedColumn> merged = merger.getMergedColumns();
        assertThat(names(merged)).containsExactly("a", "b");
        assertThat(((UnresolvedPhysicalColumn) merged.get(0)).getDataType())
                .isEqualTo(DataTypes.BIGINT());
        assertThat(merged.get(1).getComment()).contains("declared");
    }

    @Test
    void persistedMetadataColumnMayOverrideASourceColumn() {
        AsQuerySchemaMerger merger = merger();
        merger.addDeclaredColumn(
                new UnresolvedMetadataColumn("b", DataTypes.STRING(), null, false), 0);

        assertThat(merger.getMergedColumns().get(1)).isInstanceOf(UnresolvedMetadataColumn.class);
    }

    @Test
    void incompatibleTypeOverrideIsRejected() {
        assertThatThrownBy(
                        () ->
                                merger().addDeclaredColumn(
                                                new UnresolvedPhysicalColumn(
                                                        "a", DataTypes.BOOLEAN()),
                                                0))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Incompatible types for sink column 'a' at position 1. "
                                + "The source column has type 'INT', "
                                + "while the target column has type 'BOOLEAN'.");
    }

    @Test
    void computedColumnCannotOverwriteASourceColumn() {
        assertThatThrownBy(() -> merger().addDeclaredColumn(computed("a"), 0))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A column named 'a' already exists in the source schema. "
                                + "Computed columns cannot overwrite columns from source.");
    }

    @Test
    void virtualMetadataColumnCannotOverwriteASourceColumn() {
        assertThatThrownBy(
                        () ->
                                merger().addDeclaredColumn(
                                                new UnresolvedMetadataColumn(
                                                        "a", DataTypes.INT(), null, true),
                                                0))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "A column named 'a' already exists in the source schema. "
                                + "Virtual metadata columns cannot overwrite "
                                + "columns from source.");
    }

    @Test
    void duplicateDeclaredOnlyColumnIsRejected() {
        AsQuerySchemaMerger merger = merger();
        merger.addDeclaredColumn(computed("c"), 0);

        assertThatThrownBy(() -> merger.checkNotDeclared("c"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("A column named 'c' already exists in the schema. ");
        assertThatThrownBy(() -> merger.addDeclaredColumn(computed("c"), 1))
                .isInstanceOf(ValidationException.class)
                .hasMessage("A column named 'c' already exists in the schema. ");
    }

    @Test
    void repeatedOverrideOfASourceColumnIsNotReportedAsDuplicate() {
        AsQuerySchemaMerger merger = merger();
        merger.addDeclaredColumn(new UnresolvedPhysicalColumn("a", DataTypes.BIGINT()), 0);
        merger.addDeclaredColumn(new UnresolvedPhysicalColumn("a", DataTypes.BIGINT()), 1);

        assertThat(names(merger.getMergedColumns())).containsExactly("a", "b");
    }

    @Test
    void nonPhysicalSourceColumnIsRejected() {
        assertThatThrownBy(() -> new AsQuerySchemaMerger(List.of(computed("a")), DATA_TYPE_FACTORY))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Computed columns and metadata columns are not expected "
                                + "in the source schema.");
    }

    @Test
    void duplicateSourceColumnIsRejected() {
        assertThatThrownBy(
                        () ->
                                new AsQuerySchemaMerger(
                                        List.of(
                                                new UnresolvedPhysicalColumn("a", DataTypes.INT()),
                                                new UnresolvedPhysicalColumn("a", DataTypes.INT())),
                                        DATA_TYPE_FACTORY))
                .isInstanceOf(ValidationException.class)
                .hasMessage("A column named 'a' already exists in the schema. ");
    }

    private static AsQuerySchemaMerger merger() {
        return new AsQuerySchemaMerger(
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build()
                        .getColumns(),
                DATA_TYPE_FACTORY);
    }

    private static UnresolvedComputedColumn computed(String name) {
        return new UnresolvedComputedColumn(name, new SqlCallExpression("1 + 1"));
    }

    private static List<String> names(List<UnresolvedColumn> columns) {
        return columns.stream().map(UnresolvedColumn::getName).collect(Collectors.toList());
    }
}

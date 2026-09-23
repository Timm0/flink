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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AlterMaterializedTableRefreshOperation}. */
class AlterMaterializedTableRefreshOperationTest {

    private static final ObjectIdentifier TABLE_IDENTIFIER = ObjectIdentifier.of("cat", "db", "mt");

    @Test
    void testTwoArgConstructorDefaults() {
        Map<String, String> partitionSpec = Map.of("region", "eu");

        AlterMaterializedTableRefreshOperation operation =
                new AlterMaterializedTableRefreshOperation(TABLE_IDENTIFIER, partitionSpec);

        assertThat(operation.getPartitionSpec()).isEqualTo(partitionSpec);
        assertThat(operation.isPeriodic()).isFalse();
        assertThat(operation.getScheduleTime()).isNull();
        assertThat(operation.getDynamicOptions()).isEmpty();
    }

    @Test
    void testOneTimeRefresh() {
        Map<String, String> partitionSpec = Map.of("region", "eu");
        Map<String, String> dynamicOptions = Map.of("key", "value");

        AlterMaterializedTableRefreshOperation operation =
                AlterMaterializedTableRefreshOperation.oneTime(
                        TABLE_IDENTIFIER, partitionSpec, dynamicOptions);

        assertThat(operation.getTableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
        assertThat(operation.getPartitionSpec()).isEqualTo(partitionSpec);
        assertThat(operation.isPeriodic()).isFalse();
        assertThat(operation.getScheduleTime()).isNull();
        assertThat(operation.getDynamicOptions()).isEqualTo(dynamicOptions);
    }

    @Test
    void testPeriodicRefresh() {
        Map<String, String> dynamicOptions = Map.of("key", "value");

        AlterMaterializedTableRefreshOperation operation =
                AlterMaterializedTableRefreshOperation.periodic(
                        TABLE_IDENTIFIER, "2026-09-17 00:00:00", dynamicOptions);

        assertThat(operation.getTableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
        assertThat(operation.getPartitionSpec()).isEmpty();
        assertThat(operation.isPeriodic()).isTrue();
        assertThat(operation.getScheduleTime()).isEqualTo("2026-09-17 00:00:00");
        assertThat(operation.getDynamicOptions()).isEqualTo(dynamicOptions);
    }

    @Test
    void testPeriodicRefreshRequiresScheduleTime() {
        assertThatThrownBy(
                        () ->
                                AlterMaterializedTableRefreshOperation.periodic(
                                        TABLE_IDENTIFIER, null, Map.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "The scheduler time must not be null during the periodic refresh of the materialized table `cat`.`db`.`mt`.");
    }

    @Test
    void testAsSummaryStringRendersPartition() {
        AlterMaterializedTableRefreshOperation operation =
                new AlterMaterializedTableRefreshOperation(
                        TABLE_IDENTIFIER, Map.of("region", "eu"));

        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER MATERIALIZED TABLE `cat`.`db`.`mt` REFRESH PARTITION (region=eu)");
    }

    @Test
    void testAsSummaryStringWithoutPartition() {
        AlterMaterializedTableRefreshOperation operation =
                new AlterMaterializedTableRefreshOperation(TABLE_IDENTIFIER, Map.of());

        assertThat(operation.asSummaryString())
                .isEqualTo("ALTER MATERIALIZED TABLE `cat`.`db`.`mt` REFRESH");
    }
}

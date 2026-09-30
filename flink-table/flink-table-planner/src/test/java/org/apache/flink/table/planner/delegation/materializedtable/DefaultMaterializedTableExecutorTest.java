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

package org.apache.flink.table.planner.delegation.materializedtable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.FailingCatalog;
import org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.TestingJobSubmitter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.getMaterializedTable;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.getRefreshHandler;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.hasRefreshStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link DefaultMaterializedTableExecutor}. */
class DefaultMaterializedTableExecutorTest {

    private static final String CATALOG = "mt_cat";
    private static final String DATABASE = "mt_db";
    private static final ObjectPath MATERIALIZED_TABLE = new ObjectPath(DATABASE, "my_mt");
    private static final String SAVEPOINT_DIRECTORY = "file:///savepoints";

    private static final String CREATE_MATERIALIZED_TABLE =
            "CREATE MATERIALIZED TABLE my_mt"
                    + "\n FRESHNESS = INTERVAL '30' SECOND"
                    + "\n REFRESH_MODE = CONTINUOUS"
                    + "\n AS SELECT k, v FROM datagen_source";

    private static final String ALTER_AS_QUERY =
            "ALTER MATERIALIZED TABLE my_mt"
                    + " AS SELECT k, v, v * 2 AS doubled_v FROM datagen_source";

    private final FailingCatalog catalog = new FailingCatalog(CATALOG, DATABASE);

    @Test
    void testGetManuallyRefreshStatement() {
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of("catalog", "database", "my_materialized_table");
        String query = "SELECT * FROM my_source_table";
        assertThat(
                        DefaultMaterializedTableExecutor.getRefreshStatement(
                                tableIdentifier, query, Map.of(), Map.of()))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table`\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)");

        Map<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("k1", "v1");
        partitionSpec.put("k2", "v2");
        assertThat(
                        DefaultMaterializedTableExecutor.getRefreshStatement(
                                tableIdentifier, query, partitionSpec, Map.of()))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table`\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)\n"
                                + "  WHERE k1 = 'v1' AND k2 = 'v2'");
    }

    @Test
    void testGetRefreshStatementWithDynamicOptions() {
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of("catalog", "database", "my_materialized_table");
        String query = "SELECT * FROM my_source_table";

        // Dynamic options render as an OPTIONS hint on the target table.
        Map<String, String> dynamicOptions = new LinkedHashMap<>();
        dynamicOptions.put("option1", "value1");
        dynamicOptions.put("option2", "value2");
        assertThat(
                        DefaultMaterializedTableExecutor.getRefreshStatement(
                                tableIdentifier, query, Map.of(), dynamicOptions))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table` "
                                + "/*+ OPTIONS('option1'='value1', 'option2'='value2') */\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)");
    }

    @Test
    void testGetRefreshStatementWithPartitionSpecAndDynamicOptions() {
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of("catalog", "database", "my_materialized_table");
        String query = "SELECT * FROM my_source_table";

        Map<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("k1", "v1");
        partitionSpec.put("k2", "v2");
        Map<String, String> dynamicOptions = new LinkedHashMap<>();
        dynamicOptions.put("option1", "value1");

        // The OPTIONS hint and the partition WHERE clause combine.
        assertThat(
                        DefaultMaterializedTableExecutor.getRefreshStatement(
                                tableIdentifier, query, partitionSpec, dynamicOptions))
                .isEqualTo(
                        "INSERT OVERWRITE `catalog`.`database`.`my_materialized_table` "
                                + "/*+ OPTIONS('option1'='value1') */\n"
                                + "  SELECT * FROM (SELECT * FROM my_source_table)\n"
                                + "  WHERE k1 = 'v1' AND k2 = 'v2'");
    }

    @Test
    void testGenerateInsertStatement() {
        // Test generate insert crate statement
        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of("catalog", "database", "table");
        String definitionQuery = "SELECT * FROM source_table";
        String expectedStatement =
                "INSERT INTO `catalog`.`database`.`table`\n" + "SELECT * FROM source_table";

        String actualStatement =
                DefaultMaterializedTableExecutor.getInsertStatement(
                        materializedTableIdentifier, definitionQuery, Map.of());

        assertThat(actualStatement).isEqualTo(expectedStatement);
    }

    @Test
    void testGenerateInsertStatementWithDynamicOptions() {
        ObjectIdentifier materializedTableIdentifier =
                ObjectIdentifier.of("catalog", "database", "table");
        String definitionQuery = "SELECT * FROM source_table";
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("option1", "value1");
        dynamicOptions.put("option2", "value2");

        String expectedStatement =
                "INSERT INTO `catalog`.`database`.`table` "
                        + "/*+ OPTIONS('option1'='value1', 'option2'='value2') */\n"
                        + "SELECT * FROM source_table";

        String actualStatement =
                DefaultMaterializedTableExecutor.getInsertStatement(
                        materializedTableIdentifier, definitionQuery, dynamicOptions);
        assertThat(actualStatement).isEqualTo(expectedStatement);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testGetPeriodRefreshPartition(TestSpec testSpec) {
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of("catalog", "database", "table");

        if (testSpec.errorMessage == null) {
            Map<String, String> actualRefreshPartition =
                    DefaultMaterializedTableExecutor.getPeriodRefreshPartition(
                            testSpec.schedulerTime,
                            testSpec.freshness,
                            objectIdentifier,
                            testSpec.tableOptions,
                            ZoneId.systemDefault());

            assertThat(actualRefreshPartition).isEqualTo(testSpec.expectedRefreshPartition);
        } else {
            assertThatThrownBy(
                            () ->
                                    DefaultMaterializedTableExecutor.getPeriodRefreshPartition(
                                            testSpec.schedulerTime,
                                            testSpec.freshness,
                                            objectIdentifier,
                                            testSpec.tableOptions,
                                            ZoneId.systemDefault()))
                    .hasMessage(testSpec.errorMessage);
        }
    }

    static Stream<TestSpec> testData() {
        return Stream.of(
                // The interval of freshness match the partition specified by the 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 00:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-02 01:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-02")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "22"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "20"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(8))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "16"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(12))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "12"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 12:00:00")
                        .freshness(IntervalFreshness.ofHour(12))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "59"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "58"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "56"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(5))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "55"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(6))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "54"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(10))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "50"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(12))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "48"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(15))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "45"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(30))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "30"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:30:00")
                        .freshness(IntervalFreshness.ofMinute(30))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),

                // The interval of freshness is larger than the partition specified by the
                // 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23")
                        .expectedRefreshPartition("minute", "00"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 01:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .tableOptions("partition.fields.minute.date-formatter", "mm")
                        .expectedRefreshPartition("day", "2024-01-01")
                        .expectedRefreshPartition("hour", "00")
                        .expectedRefreshPartition("minute", "00"),
                // The interval of freshness is less than the partition specified by the
                // 'date-formatter'.
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 01:00:00")
                        .freshness(IntervalFreshness.ofHour(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 02:00:00")
                        .freshness(IntervalFreshness.ofHour(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofHour(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 04:00:00")
                        .freshness(IntervalFreshness.ofHour(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(2))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(4))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(15))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .expectedRefreshPartition("day", "2023-12-31")
                        .expectedRefreshPartition("hour", "23"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:00:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2023-12-31"),
                TestSpec.create()
                        .schedulerTime("2024-01-01 00:01:00")
                        .freshness(IntervalFreshness.ofMinute(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .expectedRefreshPartition("day", "2024-01-01"),

                // Invalid test case.
                TestSpec.create()
                        .schedulerTime(null)
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .errorMessage(
                                "The scheduler time must not be null during the periodic refresh of the materialized table `catalog`.`database`.`table`."),
                TestSpec.create()
                        .schedulerTime("2024-01-01")
                        .freshness(IntervalFreshness.ofDay(1))
                        .tableOptions("partition.fields.day.date-formatter", "yyyy-MM-dd")
                        .tableOptions("partition.fields.hour.date-formatter", "HH")
                        .errorMessage(
                                "Failed to parse a valid partition value for the field 'day' in materialized table `catalog`.`database`.`table` using the scheduler time '2024-01-01' based on the date format 'yyyy-MM-dd HH:mm:ss'."));
    }

    @Test
    void resumeDoesNotRestartARefreshJobThatIsOnlyLocallySuspended() {
        final TestingJobClient suspendedJob =
                new TestingJobClient(new JobID(), JobStatus.SUSPENDED);
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.scripted(suspendedJob);
        final TableEnvironment tableEnv = createTableEnvironment(jobSubmitter);
        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);

        assertThatThrownBy(() -> tableEnv.executeSql("ALTER MATERIALIZED TABLE my_mt RESUME"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "continuous refresh job has been resumed, jobId is "
                                + suspendedJob.getJobID());
        assertThat(jobSubmitter.targets).hasSize(1);
    }

    @Test
    void resumeReportsARunningRefreshJobBeforeResolvingTheRestartTarget() {
        final TestingJobClient runningJob = new TestingJobClient(new JobID(), JobStatus.RUNNING);
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.scripted(runningJob);
        final TableEnvironment tableEnv = createTableEnvironment(jobSubmitter);
        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);
        tableEnv.getConfig().set(DeploymentOptions.TARGET, "local");

        assertThatThrownBy(() -> tableEnv.executeSql("ALTER MATERIALIZED TABLE my_mt RESUME"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "continuous refresh job has been resumed, jobId is "
                                + runningJob.getJobID());
        assertThat(jobSubmitter.targets).hasSize(1);
    }

    @Test
    void dropDoesNotCancelASuspendedRefreshJob() throws Exception {
        final TestingJobClient suspendedJob =
                new TestingJobClient(new JobID(), JobStatus.SUSPENDED);
        final TableEnvironment tableEnv =
                createTableEnvironment(TestingJobSubmitter.scripted(suspendedJob));
        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);

        tableEnv.executeSql("DROP MATERIALIZED TABLE my_mt");

        assertThat(suspendedJob.isCancelRequested()).isFalse();
        assertThat(catalog.tableExists(MATERIALIZED_TABLE)).isFalse();
    }

    @Test
    void alterDoesNotRestartThePreviousDefinitionWhileTheNewRefreshJobMayStillRun()
            throws Exception {
        final TestingJobClient previousJob = new TestingJobClient(new JobID(), JobStatus.RUNNING);
        final JobID newJobId = new JobID();
        final TestingJobClient uncancellableNewJob =
                new TestingJobClient(newJobId, JobStatus.RUNNING).withFailingCancel();
        final TestingJobSubmitter jobSubmitter =
                TestingJobSubmitter.scripted(previousJob, uncancellableNewJob);
        final TableEnvironment tableEnv = createTableEnvironment(jobSubmitter);
        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);
        final String previousQuery =
                getMaterializedTable(catalog, MATERIALIZED_TABLE).getExpandedQuery();
        catalog.failNextAlterTable(hasRefreshStatus(RefreshStatus.ACTIVATED));

        assertThatThrownBy(() -> tableEnv.executeSql(ALTER_AS_QUERY))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("the new refresh job could not be cancelled")
                .hasMessageContaining("must be cancelled manually")
                .hasStackTraceContaining(newJobId.toHexString());

        final CatalogMaterializedTable materializedTable =
                getMaterializedTable(catalog, MATERIALIZED_TABLE);
        assertThat(materializedTable.getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        assertThat(materializedTable.getExpandedQuery()).isEqualTo(previousQuery);
        assertThat(getRefreshHandler(catalog, MATERIALIZED_TABLE).getRestorePath())
                .contains(SAVEPOINT_DIRECTORY + "/savepoint-1");
        assertThat(jobSubmitter.targets).hasSize(2);
    }

    private TableEnvironment createTableEnvironment(MaterializedTableJobSubmitter jobSubmitter) {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(DeploymentOptions.TARGET, "remote");
        rootConfiguration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, SAVEPOINT_DIRECTORY);
        final TableEnvironment tableEnv =
                MaterializedTableTestUtils.createTableEnvironment(
                        rootConfiguration, CATALOG, catalog, jobSubmitter);
        tableEnv.executeSql(
                "CREATE TABLE datagen_source (k INT, v BIGINT) WITH ('connector' = 'datagen')");
        return tableEnv;
    }

    private static class TestSpec {
        private String schedulerTime;
        private IntervalFreshness freshness;
        private final Map<String, String> tableOptions;
        private final Map<String, String> expectedRefreshPartition;

        private @Nullable String errorMessage;

        private TestSpec() {
            this.tableOptions = new HashMap<>();
            this.expectedRefreshPartition = new HashMap<>();
        }

        public static TestSpec create() {
            return new TestSpec();
        }

        public TestSpec schedulerTime(String schedulerTime) {
            this.schedulerTime = schedulerTime;
            return this;
        }

        public TestSpec freshness(IntervalFreshness freshness) {
            this.freshness = freshness;
            return this;
        }

        public TestSpec tableOptions(String key, String value) {
            this.tableOptions.put(key, value);
            return this;
        }

        public TestSpec expectedRefreshPartition(String key, String value) {
            this.expectedRefreshPartition.put(key, value);
            return this;
        }

        public TestSpec errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "schedulerTime="
                    + schedulerTime
                    + ", freshness="
                    + freshness
                    + ", tableOptions="
                    + tableOptions
                    + ", expectedRefreshPartition="
                    + expectedRefreshPartition
                    + '}';
        }
    }
}

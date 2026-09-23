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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * IT case for materialized table related statement via {@link
 * org.apache.flink.table.api.TableEnvironment}.
 */
class MaterializedTableTableApiITCase {

    private static final String DATABASE = "mt_db";
    private static final String MT_NAME = "my_mt";

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration PAUSE = Duration.ofMillis(200);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @TempDir private Path savepointDir;

    private StreamTableEnvironment tEnv;
    private GenericInMemoryCatalog catalog;
    private RestClusterClient<?> clusterClient;

    @BeforeEach
    void before(@InjectClusterClient RestClusterClient<?> injectedClusterClient) {
        TestStreamEnvironment.unsetAsContext();

        this.clusterClient = injectedClusterClient;

        Configuration configuration = new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(DeploymentOptions.TARGET, "remote");
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toUri().toString());
        configuration.set(
                TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set(DeploymentOptions.TARGET, "remote");

        catalog = new GenericInMemoryCatalog("mt_cat", DATABASE);
        tEnv.registerCatalog("mt_cat", catalog);
        tEnv.useCatalog("mt_cat");
        tEnv.useDatabase(DATABASE);

        tEnv.executeSql(
                "CREATE TABLE datagen_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");
    }

    @AfterEach
    void after() throws Exception {
        // Cancel every job this test may have left running so nothing leaks into sibling tests.
        if (clusterClient != null) {
            Collection<JobStatusMessage> jobs = clusterClient.listJobs().get();
            for (JobStatusMessage job : jobs) {
                if (!job.getJobState().isTerminalState()) {
                    clusterClient.cancel(job.getJobId()).get();
                }
            }
        }
        TestStreamEnvironment.unsetAsContext();
    }

    @Test
    void createAndDropContinuous() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());

        CatalogMaterializedTable materializedTable = getMaterializedTable();
        assertThat(materializedTable.getRefreshMode()).isEqualTo(RefreshMode.CONTINUOUS);
        assertThat(materializedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);

        JobID refreshJobId = getRefreshJobId();
        tEnv.executeSql("DROP MATERIALIZED TABLE " + MT_NAME);

        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
        awaitJobStatus(refreshJobId, JobStatus.CANCELED);
    }

    @Test
    void suspendAndResumeContinuous() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);

        awaitAllVerticesRunning(getRefreshJobId());

        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND");
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);

        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME");
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void oneTimeRefreshRunsBatchJob() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE bounded_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '20',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS SELECT k, COUNT(v) AS cnt FROM bounded_source GROUP BY k");
        final String batchJobId;
        try (CloseableIterator<Row> it =
                tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " REFRESH").collect()) {
            assertThat(it.hasNext()).isTrue();
            Row row = it.next();
            batchJobId = (String) row.getField(0);
            assertThat(batchJobId).isNotEmpty();
            assertThat(row.getField(1))
                    .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
                    .containsEntry("execution.target", "remote");
            assertThat(it.hasNext()).isFalse();
        }
        awaitJobStatus(JobID.fromHexString(batchJobId), JobStatus.FINISHED);
    }

    @Test
    void fullModeThrows() {
        String ddl =
                "CREATE MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = FULL\n"
                        + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k";

        assertThatThrownBy(() -> tEnv.executeSql(ddl))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "CREATE MATERIALIZED TABLE on a full-mode materialized table requires a workflow scheduler, which is not configured in this environment");
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
    }

    @Test
    void convertRegularTableToFullModeThrowsAndKeepsTheTable() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + MT_NAME
                        + " (\n"
                        + "  k INT,\n"
                        + "  cnt BIGINT\n"
                        + ") WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE OR ALTER MATERIALIZED TABLE "
                                                + MT_NAME
                                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                                                + " FRESHNESS = INTERVAL '30' SECOND\n"
                                                + " REFRESH_MODE = FULL\n"
                                                + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "CREATE OR ALTER MATERIALIZED TABLE on a full-mode materialized table requires a workflow scheduler");
        assertThat(catalog.getTable(new ObjectPath(DATABASE, MT_NAME)).getTableKind())
                .isEqualTo(TableKind.TABLE);
    }

    @Test
    void applicationModeThrows() {
        tEnv.getConfig().set(DeploymentOptions.TARGET, "kubernetes-application");

        assertThatThrownBy(() -> tEnv.executeSql(continuousMaterializedTableDdl()))
                .isInstanceOf(TableException.class)
                .rootCause()
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Application-mode materialized table refresh is not supported in this environment");
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
    }

    @Test
    void localTargetThrows() {
        tEnv.getConfig().set(DeploymentOptions.TARGET, "local");

        assertThatThrownBy(() -> tEnv.executeSql(continuousMaterializedTableDdl()))
                .isInstanceOf(TableException.class)
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'local' is not supported");
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
    }

    @Test
    void dropMissingWithIfExistsSucceeds() {
        tEnv.executeSql("DROP MATERIALIZED TABLE IF EXISTS " + MT_NAME);

        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
    }

    @Test
    void dropMissingWithoutIfExistsThrows() {
        assertThatThrownBy(() -> tEnv.executeSql("DROP MATERIALIZED TABLE " + MT_NAME))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void alterAsQueryContinuous() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);

        awaitAllVerticesRunning(getRefreshJobId());

        tEnv.executeSql(
                "ALTER MATERIALIZED TABLE "
                        + MT_NAME
                        + " AS SELECT k, COUNT(v) AS cnt, MAX(v) AS max_v FROM datagen_source"
                        + " GROUP BY k");

        CatalogMaterializedTable materializedTable = getMaterializedTable();
        assertThat(materializedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(materializedTable.getExpandedQuery()).contains("max_v");
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void alterAsQueryRollsBackWhenTheNewRefreshJobCannotStart() throws Exception {
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'true')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS SELECT k, v FROM datagen_source");
        CatalogMaterializedTable originalTable = getMaterializedTable();
        awaitAllVerticesRunning(getRefreshJobId());

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE "
                                                + MT_NAME
                                                + " AS SELECT k, v, COUNT(*) AS cnt FROM datagen_source"
                                                + " GROUP BY k, v"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Failed to start the continuous refresh job using new query");

        CatalogMaterializedTable rolledBackTable = getMaterializedTable();
        assertThat(rolledBackTable.getExpandedQuery()).isEqualTo(originalTable.getExpandedQuery());
        assertThat(rolledBackTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("k", "v");
        assertThat(rolledBackTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);

        JobID restoredJobId = getRefreshJobId();
        awaitAllVerticesRunning(restoredJobId);
        tEnv.executeSql("DROP MATERIALIZED TABLE " + MT_NAME);
        awaitJobStatus(restoredJobId, JobStatus.CANCELED);
    }

    @Test
    void convertRegularTableToContinuousMaterializedTable() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + MT_NAME
                        + " (\n"
                        + "  k INT,\n"
                        + "  cnt BIGINT\n"
                        + ") WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        tEnv.executeSql(
                "CREATE OR ALTER MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k");

        CatalogMaterializedTable materializedTable = getMaterializedTable();
        assertThat(materializedTable.getRefreshMode()).isEqualTo(RefreshMode.CONTINUOUS);
        assertThat(materializedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);

        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void controllingJobFromAnotherSessionThrows() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);

        StreamTableEnvironment otherEnv =
                createTableEnvOnSharedCatalog(EnvironmentSettings.inStreamingMode());

        assertThatThrownBy(
                        () ->
                                otherEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND"))
                .isInstanceOf(TableException.class)
                .rootCause()
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Controlling a refresh job started by another session is not supported");
    }

    @Test
    void createContinuousFromBatchModeEnvironment() throws Exception {
        StreamTableEnvironment batchEnv =
                createTableEnvOnSharedCatalog(EnvironmentSettings.inBatchMode());
        batchEnv.executeSql(
                "CREATE TABLE bounded_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '20',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");

        batchEnv.executeSql(
                "CREATE MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS SELECT k, COUNT(v) AS cnt FROM bounded_source GROUP BY k");

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(clusterClient.getJobDetails(getRefreshJobId()).get().getJobVertexInfos())
                .extracting(JobDetailsInfo.JobVertexDetailsInfo::getName)
                .anySatisfy(name -> assertThat(name).contains("GroupAggregate"));
    }

    @Test
    void laterJobsOfTheSessionDoNotInheritRefreshJobConfiguration() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        awaitAllVerticesRunning(getRefreshJobId());

        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND");
        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME");

        tEnv.executeSql(
                "CREATE TABLE blackhole_sink (k INT, v BIGINT) WITH ('connector' = 'blackhole')");
        JobID insertJobId =
                tEnv.executeSql("INSERT INTO blackhole_sink SELECT k, v FROM datagen_source")
                        .getJobClient()
                        .orElseThrow()
                        .getJobID();

        assertThat(clusterClient.getJobDetails(insertJobId).get().getName())
                .doesNotStartWith("Materialized_table_");
        awaitAllVerticesRunning(insertJobId);
    }

    @Test
    void checkpointIntervalFromEnvironmentIsNotOverriddenByFreshness() throws Exception {
        Configuration envConfiguration = new Configuration();
        envConfiguration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(7));
        StreamTableEnvironment envWithCheckpointing =
                createTableEnvOnSharedCatalog(
                        EnvironmentSettings.inStreamingMode(), envConfiguration);

        envWithCheckpointing.executeSql(continuousMaterializedTableDdl());

        assertThat(getCheckpointIntervalMillis(getRefreshJobId()))
                .isEqualTo(Duration.ofSeconds(7).toMillis());
    }

    @Test
    void oneTimeRefreshAppliesTableOptionsFromEnvironment() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE bounded_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '20',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS SELECT k, COUNT(v) AS cnt FROM bounded_source GROUP BY k");

        Configuration envConfiguration = new Configuration();
        envConfiguration.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        StreamTableEnvironment envWithTableOption =
                createTableEnvOnSharedCatalog(
                        EnvironmentSettings.inStreamingMode(), envConfiguration);

        final JobID batchJobId;
        try (CloseableIterator<Row> it =
                envWithTableOption
                        .executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " REFRESH")
                        .collect()) {
            batchJobId = JobID.fromHexString((String) it.next().getField(0));
        }

        assertThat(clusterClient.getJobDetails(batchJobId).get().getJobVertexInfos())
                .extracting(JobDetailsInfo.JobVertexDetailsInfo::getParallelism)
                .contains(2);
    }

    @Test
    void suspendWithoutSavepointDirThrows() {
        Configuration configuration = new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(DeploymentOptions.TARGET, "remote");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        StreamTableEnvironment noSavepointEnv = StreamTableEnvironment.create(env);
        noSavepointEnv.getConfig().set(DeploymentOptions.TARGET, "remote");

        GenericInMemoryCatalog noSavepointCatalog =
                new GenericInMemoryCatalog("mt_cat_no_sp", DATABASE);
        noSavepointEnv.registerCatalog("mt_cat_no_sp", noSavepointCatalog);
        noSavepointEnv.useCatalog("mt_cat_no_sp");
        noSavepointEnv.useDatabase(DATABASE);

        noSavepointEnv.executeSql(
                "CREATE TABLE datagen_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");

        noSavepointEnv.executeSql(continuousMaterializedTableDdl());

        assertThatThrownBy(
                        () ->
                                noSavepointEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND"))
                .isInstanceOf(TableException.class)
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Savepoint directory is not configured");
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalog(EnvironmentSettings settings) {
        return createTableEnvOnSharedCatalog(settings, new Configuration());
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalog(
            EnvironmentSettings settings, Configuration envConfiguration) {
        Configuration configuration = new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.addAll(envConfiguration);
        configuration.set(DeploymentOptions.TARGET, "remote");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        StreamTableEnvironment otherEnv = StreamTableEnvironment.create(env, settings);
        otherEnv.getConfig().set(DeploymentOptions.TARGET, "remote");
        otherEnv.registerCatalog("mt_cat", catalog);
        otherEnv.useCatalog("mt_cat");
        otherEnv.useDatabase(DATABASE);
        return otherEnv;
    }

    private static String continuousMaterializedTableDdl() {
        return "CREATE MATERIALIZED TABLE "
                + MT_NAME
                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                + " FRESHNESS = INTERVAL '30' SECOND\n"
                + " REFRESH_MODE = CONTINUOUS\n"
                + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k";
    }

    private CatalogMaterializedTable getMaterializedTable() throws Exception {
        return (CatalogMaterializedTable) catalog.getTable(new ObjectPath(DATABASE, MT_NAME));
    }

    private JobID getRefreshJobId() throws Exception {
        ContinuousRefreshHandler handler =
                ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                        getMaterializedTable().getSerializedRefreshHandler(),
                        getClass().getClassLoader());
        return JobID.fromHexString(handler.getJobId());
    }

    private void awaitJobStatus(JobID jobId, JobStatus expectedStatus) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return clusterClient.getJobStatus(jobId).get() == expectedStatus;
                    } catch (Exception e) {
                        return false;
                    }
                },
                TIMEOUT,
                PAUSE,
                String.format("Job %s did not reach %s in time.", jobId, expectedStatus));
    }

    private long getCheckpointIntervalMillis(JobID jobId) throws Exception {
        JobMessageParameters parameters =
                CheckpointConfigHeaders.getInstance().getUnresolvedMessageParameters();
        parameters.jobPathParameter.resolve(jobId);
        CheckpointConfigInfo checkpointConfig =
                clusterClient
                        .sendRequest(
                                CheckpointConfigHeaders.getInstance(),
                                parameters,
                                EmptyRequestBody.getInstance())
                        .get();
        return RestMapperUtils.getStrictObjectMapper()
                .valueToTree(checkpointConfig)
                .get(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_INTERVAL)
                .asLong();
    }

    private void awaitAllVerticesRunning(JobID jobId) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        JobDetailsInfo details = clusterClient.getJobDetails(jobId).get();
                        return !details.getJobVertexInfos().isEmpty()
                                && details.getJobVertexInfos().stream()
                                        .allMatch(
                                                v ->
                                                        v.getExecutionState()
                                                                == ExecutionState.RUNNING);
                    } catch (Exception e) {
                        return false;
                    }
                },
                TIMEOUT,
                PAUSE,
                "Refresh job did not reach a fully RUNNING state in time.");
    }
}

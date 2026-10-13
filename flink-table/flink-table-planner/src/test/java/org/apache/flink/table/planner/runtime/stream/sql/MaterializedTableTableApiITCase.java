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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.MaterializedPipeline;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.delegation.materializedtable.DefaultMaterializedTableExecutor;
import org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils;
import org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.FailingCatalog;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.NetUtils;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.hasRefreshStatus;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.recordRefreshHandler;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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

    @RegisterExtension
    private final LoggerAuditingExtension executorLog =
            new LoggerAuditingExtension(DefaultMaterializedTableExecutor.class, Level.WARN);

    @TempDir private Path savepointDir;

    private StreamExecutionEnvironment executionEnvironment;
    private StreamTableEnvironment tEnv;
    private FailingCatalog catalog;
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

        executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        executionEnvironment.setParallelism(1);
        tEnv = StreamTableEnvironment.create(executionEnvironment);
        targetMiniClusterFromTableConfig(tEnv);

        catalog = new FailingCatalog("mt_cat", DATABASE);
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
            Collection<JobStatusMessage> jobs =
                    clusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            for (JobStatusMessage job : jobs) {
                if (!job.getJobState().isTerminalState()) {
                    clusterClient
                            .cancel(job.getJobId())
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Application-mode materialized table refresh is not supported in this environment");
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
    }

    @Test
    void localTargetThrows() {
        tEnv.getConfig().set(DeploymentOptions.TARGET, "local");

        assertThatThrownBy(() -> tEnv.executeSql(continuousMaterializedTableDdl()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'local' is not supported");
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
    }

    @Test
    void refreshJobsAndTheirControlIgnoreTheEnvironmentsClusterWhenTheTableConfigSetsTheTarget()
            throws Exception {
        createTableEnvOnSharedCatalogWithAnUnreachableClusterInTheRoot()
                .executeSql(continuousMaterializedTableDdl());
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);

        createTableEnvOnSharedCatalogWithAnUnreachableClusterInTheRoot()
                .executeSql("DROP MATERIALIZED TABLE " + MT_NAME);

        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
        awaitJobStatus(refreshJobId, JobStatus.CANCELED);
    }

    @Test
    void refreshJobsBypassTheProgramsEnvironmentAndItsFixedJobIdAndSavepointPath()
            throws Exception {
        final Configuration envConfiguration = new Configuration();
        envConfiguration.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, new JobID().toHexString());
        envConfiguration.set(StateRecoveryOptions.SAVEPOINT_PATH, "file:///no-such-savepoint");
        final String secondTable = MT_NAME + "_2";

        ProgramContextEnvironment.setAsContext();
        try {
            final StreamTableEnvironment applicationEnv =
                    createTableEnvOnSharedCatalog(
                            EnvironmentSettings.inStreamingMode(), envConfiguration);
            applicationEnv.executeSql(continuousMaterializedTableDdl());
            applicationEnv.executeSql(
                    continuousMaterializedTableDdl().replaceFirst(MT_NAME, secondTable));
        } finally {
            ProgramContextEnvironment.unsetAsContext();
        }

        final JobID firstJobId = getRefreshJobId();
        final JobID secondJobId =
                JobID.fromHexString(
                        MaterializedTableTestUtils.getRefreshHandler(
                                        catalog, new ObjectPath(DATABASE, secondTable))
                                .getJobId());
        assertThat(secondJobId).isNotEqualTo(firstJobId);
        awaitAllVerticesRunning(firstJobId);
        awaitAllVerticesRunning(secondJobId);
    }

    @Test
    void miniclusterTargetRunsTheRefreshJobThroughTheTestEnvironment(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        TestStreamEnvironment.setAsContext(miniCluster, 1);
        try {
            tEnv.getConfig().set(DeploymentOptions.TARGET, "minicluster");
            tEnv.executeSql(continuousMaterializedTableDdl());
        } finally {
            TestStreamEnvironment.unsetAsContext();
        }

        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void alterWithoutSavepointDirectoryKeepsTheRefreshJobRunning() throws Exception {
        StreamTableEnvironment noSavepointEnv =
                createTableEnvOnSharedCatalog(EnvironmentSettings.inStreamingMode());
        noSavepointEnv.executeSql(continuousMaterializedTableDdl());
        CatalogMaterializedTable originalTable = getMaterializedTable();
        JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);

        assertThatThrownBy(() -> noSavepointEnv.executeSql(alterAsQueryAddingMaxV()))
                .isInstanceOf(TableException.class)
                .hasStackTraceContaining(
                        "Savepoint directory is not configured ('execution.checkpointing.savepoint-dir'), can't stop job with savepoint.");

        CatalogMaterializedTable unchangedTable = getMaterializedTable();
        assertThat(unchangedTable.getExpandedQuery()).isEqualTo(originalTable.getExpandedQuery());
        assertThat(unchangedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(
                        clusterClient
                                .getJobStatus(refreshJobId)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void alterAsQueryThatFailsValidationKeepsTheRefreshJobRunning() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        final CatalogMaterializedTable originalTable = getMaterializedTable();
        final ContinuousRefreshHandler originalHandler = getRefreshHandler();
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE "
                                                + MT_NAME
                                                + " AS SELECT k FROM datagen_source GROUP BY k"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Dropping of persisted column `cnt` is not supported.");

        final CatalogMaterializedTable unchangedTable = getMaterializedTable();
        assertThat(unchangedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(unchangedTable.getExpandedQuery()).isEqualTo(originalTable.getExpandedQuery());
        assertThat(getRefreshHandler()).usingRecursiveComparison().isEqualTo(originalHandler);
        assertThat(
                        clusterClient
                                .getJobStatus(refreshJobId)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void convertWithAnUnresolvableTargetLeavesTheTableSuspended() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + MT_NAME
                        + " (\n"
                        + "  k INT,\n"
                        + "  cnt BIGINT\n"
                        + ") WITH ('connector' = 'values', 'sink-insert-only' = 'false')");
        tEnv.getConfig().set(DeploymentOptions.TARGET, "kubernetes-session");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE OR ALTER MATERIALIZED TABLE "
                                                + MT_NAME
                                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                                                + " FRESHNESS = INTERVAL '30' SECOND\n"
                                                + " REFRESH_MODE = CONTINUOUS\n"
                                                + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("left in SUSPENDED status")
                .hasStackTraceContaining("'kubernetes.cluster-id'");

        CatalogMaterializedTable convertedTable = getMaterializedTable();
        assertThat(convertedTable.getTableKind()).isEqualTo(TableKind.MATERIALIZED_TABLE);
        assertThat(convertedTable.getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);

        assertThatThrownBy(
                        () -> tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND"))
                .isInstanceOf(TableException.class)
                .hasMessageEndingWith("continuous refresh job has been suspended.");

        tEnv.getConfig().set(DeploymentOptions.TARGET, "remote");
        tEnv.executeSql(alterAsQueryAddingMaxV());
        CatalogMaterializedTable alteredTable = getMaterializedTable();
        assertThat(alteredTable.getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        assertThat(alteredTable.getExpandedQuery()).contains("max_v");

        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME");
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void alterWithAnUnresolvableTargetKeepsTheRefreshJobRunning() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        final CatalogMaterializedTable originalTable = getMaterializedTable();
        final ContinuousRefreshHandler originalHandler = getRefreshHandler();
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);

        tEnv.getConfig().set(DeploymentOptions.TARGET, "kubernetes-session");

        assertThatThrownBy(() -> tEnv.executeSql(alterAsQueryAddingMaxV()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'kubernetes.cluster-id'");

        final CatalogMaterializedTable unchangedTable = getMaterializedTable();
        assertThat(unchangedTable.getExpandedQuery()).isEqualTo(originalTable.getExpandedQuery());
        assertThat(unchangedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(getRefreshHandler()).usingRecursiveComparison().isEqualTo(originalHandler);
        awaitJobStatus(refreshJobId, JobStatus.RUNNING);
    }

    @Test
    void resumeWithAnUnresolvableTargetLeavesTheTableSuspended() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);
        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND");
        ContinuousRefreshHandler suspendedHandler = getRefreshHandler();

        tEnv.getConfig().set(DeploymentOptions.TARGET, "kubernetes-session");

        assertThatThrownBy(() -> tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'kubernetes.cluster-id'");

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        assertThat(getRefreshHandler()).usingRecursiveComparison().isEqualTo(suspendedHandler);
    }

    @Test
    void refreshWithAnUnresolvableTargetSubmitsNoJob() throws Exception {
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
        Set<JobID> jobsBeforeRefresh = listJobIds();

        tEnv.getConfig().set(DeploymentOptions.TARGET, "kubernetes-session");

        assertThatThrownBy(
                        () -> tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " REFRESH"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'kubernetes.cluster-id'");

        assertThat(listJobIds()).isEqualTo(jobsBeforeRefresh);
    }

    @Test
    void resumeRestartsARefreshJobThatWasCancelledOutsideTheTable() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        JobID cancelledJobId = getRefreshJobId();
        awaitAllVerticesRunning(cancelledJobId);

        clusterClient.cancel(cancelledJobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        awaitJobStatus(cancelledJobId, JobStatus.CANCELED);

        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME");

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        JobID resumedJobId = getRefreshJobId();
        assertThat(resumedJobId).isNotEqualTo(cancelledJobId);
        awaitAllVerticesRunning(resumedJobId);
        assertThat(executorLog.getMessages())
                .anySatisfy(
                        message ->
                                assertThat(message)
                                        .contains("without restoring state")
                                        .contains("and unset it afterwards (RESET in SQL)"));
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

        tEnv.executeSql(alterAsQueryAddingMaxV());

        CatalogMaterializedTable materializedTable = getMaterializedTable();
        assertThat(materializedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(materializedTable.getExpandedQuery()).contains("max_v");
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void alterAsQueryRollsBackWhenTheNewRefreshJobCannotStart() throws Exception {
        tEnv.executeSql(insertOnlyMaterializedTableDdl());
        CatalogMaterializedTable originalTable = getMaterializedTable();
        awaitAllVerticesRunning(getRefreshJobId());

        assertThatThrownBy(() -> tEnv.executeSql(alterAsQueryToAnUpdatingAggregate()))
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
    void alterReportsTheSavepointWhenTheChangeCannotBeApplied() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        final CatalogMaterializedTable originalTable = getMaterializedTable();
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);
        catalog.failNextAlterTable(
                hasRefreshStatus(RefreshStatus.SUSPENDED)
                        .and(hasExpandedQuery(query -> query.contains("max_v"))));

        final Throwable alterFailure =
                catchThrowable(() -> tEnv.executeSql(alterAsQueryAddingMaxV()));

        final CatalogMaterializedTable suspendedTable = getMaterializedTable();
        assertThat(suspendedTable.getExpandedQuery()).isEqualTo(originalTable.getExpandedQuery());
        assertThat(suspendedTable.getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        final ContinuousRefreshHandler suspendedHandler = getRefreshHandler();
        assertThat(suspendedHandler.getRestorePath()).isPresent();
        assertThat(alterFailure)
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "continuous refresh job "
                                + refreshJobId
                                + " was stopped with savepoint "
                                + suspendedHandler.getRestorePath().get())
                .hasMessageContaining("SUSPENDED status with its previous definition")
                .hasRootCauseMessage("injected alter failure");
        awaitJobStatus(refreshJobId, JobStatus.FINISHED);
    }

    @Test
    void alterAsQueryLeavesNoStaleSavepointWhenTheRollbackFails() throws Exception {
        tEnv.executeSql(insertOnlyMaterializedTableDdl());
        final CatalogMaterializedTable originalTable = getMaterializedTable();
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);
        catalog.failNextAlterTable(
                firstMatchAfter(
                        hasExpandedQuery(query -> query.contains("cnt")),
                        hasExpandedQuery(originalTable.getExpandedQuery()::equals)));

        final Throwable alterFailure =
                catchThrowable(() -> tEnv.executeSql(alterAsQueryToAnUpdatingAggregate()));

        final List<Path> savepoints;
        try (Stream<Path> savepointDirEntries = Files.list(savepointDir)) {
            savepoints = savepointDirEntries.collect(Collectors.toList());
        }
        assertThat(savepoints).hasSize(1);
        assertThat(alterFailure)
                .isInstanceOf(TableException.class)
                .hasMessageContaining("rolling back the table's definition failed")
                .hasMessageContaining("The previous definition's savepoint is ")
                .hasMessageContaining(
                        savepointDir.getFileName() + "/" + savepoints.get(0).getFileName())
                .hasRootCauseMessage("injected alter failure");
        assertThat(alterFailure.getSuppressed())
                .singleElement()
                .satisfies(
                        startFailure ->
                                assertThat(startFailure)
                                        .hasStackTraceContaining("doesn't support consuming"));
        final CatalogMaterializedTable leftTable = getMaterializedTable();
        assertThat(leftTable.getExpandedQuery()).contains("cnt");
        assertThat(leftTable.getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        assertThat(getRefreshHandler().getRestorePath()).isEmpty();
        awaitJobStatus(refreshJobId, JobStatus.FINISHED);
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
    void suspendUsesTheJobClientFromSubmission() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);

        try (NetUtils.Port unusedPort = NetUtils.getAvailablePort()) {
            tEnv.getConfig().set(RestOptions.PORT, unusedPort.getPort());
            tEnv.getConfig().set(RestOptions.RETRY_MAX_ATTEMPTS, 0);

            tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND");
        }

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        awaitJobStatus(refreshJobId, JobStatus.FINISHED);
    }

    @Test
    void createCancelsTheRefreshJobWhenTheCatalogCannotRecordIt() throws Exception {
        final Set<JobID> jobsBeforeCreate = listJobIds();
        catalog.failNextAlterTable(hasRefreshStatus(RefreshStatus.ACTIVATED));

        assertThatThrownBy(() -> tEnv.executeSql(continuousMaterializedTableDdl()))
                .isInstanceOf(TableException.class)
                .hasStackTraceContaining("The job was cancelled")
                .hasRootCauseMessage("injected alter failure");

        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
        final List<JobStatusMessage> jobsSubmittedByCreate =
                clusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).stream()
                        .filter(job -> !jobsBeforeCreate.contains(job.getJobId()))
                        .collect(Collectors.toList());
        assertThat(jobsSubmittedByCreate)
                .singleElement()
                .extracting(JobStatusMessage::getJobName)
                .isEqualTo(
                        String.format(
                                "Materialized_table_%s_continuous_refresh_job",
                                ObjectIdentifier.of("mt_cat", DATABASE, MT_NAME)
                                        .asSerializableString()));
        awaitJobStatus(jobsSubmittedByCreate.get(0).getJobId(), JobStatus.CANCELED);
    }

    @Test
    void createKeepsTheOriginalFailureWhenDroppingTheTableFails() throws Exception {
        catalog.failNextAlterTable(hasRefreshStatus(RefreshStatus.ACTIVATED));
        catalog.failNextDropTable();

        final Throwable createFailure =
                catchThrowable(() -> tEnv.executeSql(continuousMaterializedTableDdl()));

        assertThat(createFailure)
                .isInstanceOf(TableException.class)
                .hasMessageContaining("dropping the table failed as well")
                .hasMessageContaining("INITIALIZING")
                .hasMessageContaining(
                        "No materialized table statement can drop a table in this status or move it out of it; remove it from the catalog directly.")
                .hasRootCauseMessage("injected drop failure");
        assertThat(createFailure.getSuppressed())
                .singleElement()
                .satisfies(
                        submitFailure ->
                                assertThat(submitFailure)
                                        .hasMessageContaining("The job was cancelled")
                                        .hasRootCauseMessage("injected alter failure"));
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.INITIALIZING);
    }

    @Test
    void suspendReportsTheSavepointWhenTheCatalogCannotRecordIt() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);
        catalog.failNextAlterTable(hasRefreshStatus(RefreshStatus.SUSPENDED));

        final Throwable suspendFailure =
                catchThrowable(
                        () -> tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND"));

        final List<Path> savepoints;
        try (Stream<Path> savepointDirEntries = Files.list(savepointDir)) {
            savepoints = savepointDirEntries.collect(Collectors.toList());
        }
        assertThat(savepoints).hasSize(1);
        assertThat(suspendFailure)
                .isInstanceOf(TableException.class)
                .hasMessageContaining("with savepoint")
                .hasMessageContaining(
                        savepointDir.getFileName() + "/" + savepoints.get(0).getFileName())
                .hasMessageContaining("still reports the table as ACTIVATED")
                .hasMessageContaining("'execution.state-recovery.path'")
                .hasMessageContaining("and unset it afterwards (RESET in SQL)");
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        awaitJobStatus(refreshJobId, JobStatus.FINISHED);
    }

    @Test
    void suspendResumeAndDropFromAnotherSession() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        JobID originalJobId = getRefreshJobId();
        awaitAllVerticesRunning(originalJobId);

        StreamTableEnvironment otherEnv = createTableEnvOnSharedCatalogWithSavepointDirectory();

        otherEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND");
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        awaitJobStatus(originalJobId, JobStatus.FINISHED);

        otherEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME");
        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        JobID resumedJobId = getRefreshJobId();
        awaitAllVerticesRunning(resumedJobId);

        createTableEnvOnSharedCatalogWithSavepointDirectory()
                .executeSql("DROP MATERIALIZED TABLE " + MT_NAME);
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
        awaitJobStatus(resumedJobId, JobStatus.CANCELED);
    }

    @Test
    void resumeFromAProgramThatNamesTheCatalogDifferentlyFailsAndKeepsTheTableSuspended()
            throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        awaitAllVerticesRunning(getRefreshJobId());
        tEnv.executeSql("ALTER MATERIALIZED TABLE " + MT_NAME + " SUSPEND");
        final ContinuousRefreshHandler suspendedHandler = getRefreshHandler();

        final StreamTableEnvironment renamingEnv =
                createTableEnvRegisteringTheCatalogAs("renamed_cat");

        assertThatThrownBy(
                        () ->
                                renamingEnv.executeSql(
                                        "ALTER MATERIALIZED TABLE " + MT_NAME + " RESUME"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Failed to resume the continuous refresh job")
                .hasStackTraceContaining("Object 'mt_cat' not found");

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        assertThat(getRefreshHandler()).usingRecursiveComparison().isEqualTo(suspendedHandler);
    }

    @Test
    void alterAsQueryFromAnotherSession() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        JobID originalJobId = getRefreshJobId();
        awaitAllVerticesRunning(originalJobId);

        createTableEnvOnSharedCatalogWithSavepointDirectory().executeSql(alterAsQueryAddingMaxV());

        CatalogMaterializedTable materializedTable = getMaterializedTable();
        assertThat(materializedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(materializedTable.getExpandedQuery()).contains("max_v");
        awaitJobStatus(originalJobId, JobStatus.FINISHED);
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void redeployFromAFreshEnvironmentRestartsTheRefreshJob() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        JobID originalJobId = getRefreshJobId();
        awaitAllVerticesRunning(originalJobId);

        createTableEnvOnSharedCatalogWithSavepointDirectory()
                .executeSql(
                        continuousMaterializedTableDdl()
                                .replaceFirst(
                                        "CREATE MATERIALIZED TABLE",
                                        "CREATE OR ALTER MATERIALIZED TABLE"));

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        JobID redeployedJobId = getRefreshJobId();
        assertThat(redeployedJobId).isNotEqualTo(originalJobId);
        awaitJobStatus(originalJobId, JobStatus.FINISHED);
        awaitAllVerticesRunning(redeployedJobId);
    }

    @Test
    void changedRedeployFromAFreshEnvironmentAppliesTheNewQuery() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        final JobID originalJobId = getRefreshJobId();
        awaitAllVerticesRunning(originalJobId);

        createTableEnvOnSharedCatalogWithSavepointDirectory()
                .executeSql(
                        continuousMaterializedTableDdl()
                                .replaceFirst(
                                        "CREATE MATERIALIZED TABLE",
                                        "CREATE OR ALTER MATERIALIZED TABLE")
                                .replace(
                                        "COUNT(v) AS cnt FROM",
                                        "COUNT(v) AS cnt, MAX(v) AS max_v FROM"));

        final CatalogMaterializedTable redeployedTable = getMaterializedTable();
        assertThat(redeployedTable.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(redeployedTable.getExpandedQuery()).contains("max_v");
        final JobID redeployedJobId = getRefreshJobId();
        assertThat(redeployedJobId).isNotEqualTo(originalJobId);
        awaitJobStatus(originalJobId, JobStatus.FINISHED);
        awaitAllVerticesRunning(redeployedJobId);
    }

    @Test
    void dropFailsClosedWhenTheClusterDoesNotKnowTheRefreshJob() throws Exception {
        tEnv.executeSql(continuousMaterializedTableDdl());
        awaitAllVerticesRunning(getRefreshJobId());

        String unknownJobId = new JobID().toHexString();
        ContinuousRefreshHandler unknownHandler =
                new ContinuousRefreshHandler("remote", "", unknownJobId);
        recordRefreshHandler(
                catalog,
                new ObjectPath(DATABASE, MT_NAME),
                RefreshStatus.ACTIVATED,
                unknownHandler);

        assertThatThrownBy(
                        () ->
                                createTableEnvOnSharedCatalog(EnvironmentSettings.inStreamingMode())
                                        .executeSql("DROP MATERIALIZED TABLE " + MT_NAME))
                .isInstanceOf(TableException.class)
                .hasStackTraceContaining(unknownJobId)
                .hasStackTraceContaining("'rest.address'");
        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isTrue();
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
        assertThat(
                        clusterClient
                                .getJobDetails(getRefreshJobId())
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                .getJobVertexInfos())
                .extracting(JobDetailsInfo.JobVertexDetailsInfo::getName)
                .anySatisfy(name -> assertThat(name).contains("GroupAggregate"));
    }

    @ParameterizedTest(name = "streaming mode: {0}")
    @ValueSource(booleans = {true, false})
    void createAndDropWithTheClusterOnlyInTheEnvironmentsConfiguration(boolean streamingMode)
            throws Exception {
        final StreamTableEnvironment rootTargetEnv =
                createTableEnvOnSharedCatalogWithTheClusterOnlyInTheRoot(
                        streamingMode
                                ? EnvironmentSettings.inStreamingMode()
                                : EnvironmentSettings.inBatchMode());
        rootTargetEnv.executeSql(
                "CREATE TABLE slow_bounded_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '1000000',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");

        aggregate(rootTargetEnv, "slow_bounded_source")
                .materializeInto(MT_NAME, continuousDescriptor())
                .execute();
        final JobID refreshJobId = getRefreshJobId();
        awaitAllVerticesRunning(refreshJobId);

        rootTargetEnv.executeSql("DROP MATERIALIZED TABLE " + MT_NAME);

        assertThat(catalog.tableExists(new ObjectPath(DATABASE, MT_NAME))).isFalse();
        awaitJobStatus(refreshJobId, JobStatus.CANCELED);
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

        assertThat(
                        clusterClient
                                .getJobDetails(insertJobId)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                .getName())
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

        assertThat(
                        clusterClient
                                .getJobDetails(batchJobId)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                .getJobVertexInfos())
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
        targetMiniClusterFromTableConfig(noSavepointEnv);

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

    @Test
    void materializeIntoCreatesAContinuousMaterializedTable() throws Exception {
        final MaterializedTable materializedTable =
                aggregate().materializeInto(MT_NAME, Duration.ofSeconds(30)).execute();

        assertThat(materializedTable.getIdentifier())
                .isEqualTo(ObjectIdentifier.of("mt_cat", DATABASE, MT_NAME));
        final CatalogMaterializedTable table = getMaterializedTable();
        assertThat(table.getRefreshMode()).isEqualTo(RefreshMode.CONTINUOUS);
        assertThat(table.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(table.getOptions()).isEmpty();
        assertThat(table.getDefinitionFreshness()).isEqualTo(IntervalFreshness.ofSecond(30));
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void executeRecordsTheGivenStartMode() throws Exception {
        final StartMode fromNow = StartMode.fromNow();

        final MaterializedTable materializedTable =
                aggregate().materializeInto(MT_NAME, continuousDescriptor()).execute(fromNow);

        assertThat(materializedTable.getIdentifier())
                .isEqualTo(ObjectIdentifier.of("mt_cat", DATABASE, MT_NAME));
        final CatalogMaterializedTable table = getMaterializedTable();
        assertThat(table.getRefreshMode()).isEqualTo(RefreshMode.CONTINUOUS);
        assertThat(table.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(table.getStartMode()).contains(fromNow);
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void executeRejectsAStartModeWithoutItsRequiredTimestamp() {
        final MaterializedPipeline pipeline =
                aggregate().materializeInto(MT_NAME, continuousDescriptor());
        final StartMode withoutTimestamp = StartMode.of(StartMode.StartModeKind.FROM_TIMESTAMP);

        assertThatThrownBy(() -> pipeline.execute(withoutTimestamp))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Start mode FROM_TIMESTAMP requires a timestamp. Create it with StartMode.fromTimestamp(Instant) or StartMode.resumeOrFromTimestamp(Instant).");
        assertThat(tableExists()).isFalse();
    }

    @Test
    void invalidPathFailsAtMaterializeInto() {
        assertThatThrownBy(() -> aggregate().materializeInto("a.b.c.d"))
                .isInstanceOf(SqlParserException.class)
                .hasMessage("Invalid SQL identifier a.b.c.d.");
    }

    @Test
    void invalidFreshnessFailsAtMaterializeInto() {
        final Table query = aggregate();
        final Duration aboveIntegerSeconds = Duration.ofSeconds(Integer.MAX_VALUE + 1L);

        assertThatThrownBy(() -> query.materializeInto(MT_NAME, Duration.ZERO))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Freshness must be positive, but was: PT0S");
        assertThatThrownBy(() -> query.materializeInto(MT_NAME, Duration.ofSeconds(-5)))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Freshness must be positive, but was: PT-5S");
        assertThatThrownBy(() -> query.materializeInto(MT_NAME, Duration.ofMillis(1500)))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Freshness must be a whole number of seconds, but was: PT1.5S");
        assertThatThrownBy(() -> query.materializeInto(MT_NAME, aboveIntegerSeconds))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Freshness must be at most 2147483647 seconds, but was: "
                                + aboveIntegerSeconds);
        assertThat(tableExists()).isFalse();
    }

    @Test
    void sqlQueryLeafIsStoredFullyQualified() throws Exception {
        tEnv.sqlQuery("SELECT k, v FROM datagen_source").materializeInto(MT_NAME).execute();

        assertThat(getMaterializedTable().getExpandedQuery())
                .contains("`mt_cat`.`" + DATABASE + "`.`datagen_source`");
    }

    @Test
    void reExecutingAnUnchangedDeclarationRestartsTheRefreshJobWithoutState() throws Exception {
        final MaterializedPipeline pipeline =
                aggregate().materializeInto(MT_NAME, Duration.ofSeconds(30));
        pipeline.execute();
        final JobID firstJobId = getRefreshJobId();
        awaitAllVerticesRunning(firstJobId);

        pipeline.execute();

        assertThat(getMaterializedTable().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        final JobID secondJobId = getRefreshJobId();
        assertThat(secondJobId).isNotEqualTo(firstJobId);
        assertThat(getRefreshHandler().getRestorePath()).isEmpty();
        awaitJobStatus(firstJobId, JobStatus.FINISHED);
        awaitAllVerticesRunning(secondJobId);
    }

    @Test
    void reExecutingAChangedDeclarationAppendsANullableColumn() throws Exception {
        aggregate().materializeInto(MT_NAME, Duration.ofSeconds(30)).execute();
        final JobID firstJobId = getRefreshJobId();
        awaitAllVerticesRunning(firstJobId);

        final Table changedQuery =
                tEnv.from("datagen_source")
                        .groupBy($("k"))
                        .select($("k"), $("v").count().as("cnt"), lit(1).count().as("rows"));
        assertThat(changedQuery.getResolvedSchema().getColumnDataTypes().get(2).getLogicalType())
                .isEqualTo(new BigIntType(false));

        changedQuery.materializeInto(MT_NAME, Duration.ofSeconds(30)).execute();

        final ResolvedSchema storedSchema = tEnv.from(MT_NAME).getResolvedSchema();
        assertThat(storedSchema.getColumnNames()).containsExactly("k", "cnt", "rows");
        assertThat(storedSchema.getColumnDataTypes().get(2).getLogicalType())
                .isEqualTo(new BigIntType(true));
        final JobID secondJobId = getRefreshJobId();
        assertThat(secondJobId).isNotEqualTo(firstJobId);
        assertThat(getRefreshHandler().getRestorePath()).isEmpty();
        awaitJobStatus(firstJobId, JobStatus.FINISHED);
        awaitAllVerticesRunning(secondJobId);
    }

    @Test
    void regularTableAtThePathIsRejectedAndKept() throws Exception {
        createRegularTable();
        final Set<JobID> jobsBefore = listJobIds();

        assertThatThrownBy(
                        () ->
                                aggregate()
                                        .materializeInto(MT_NAME, Duration.ofSeconds(30))
                                        .execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("a regular table with the same identifier exists");

        assertThat(catalog.getTable(new ObjectPath(DATABASE, MT_NAME)).getTableKind())
                .isEqualTo(TableKind.TABLE);
        assertThat(listJobIds()).isEqualTo(jobsBefore);
    }

    @Test
    void permanentViewAtThePathIsRejected() throws Exception {
        tEnv.executeSql("CREATE VIEW " + MT_NAME + " AS SELECT k, v FROM datagen_source");
        final Set<JobID> jobsBefore = listJobIds();

        assertThatThrownBy(() -> aggregate().materializeInto(MT_NAME).execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "does not support the CREATE OR ALTER MATERIALIZED TABLE operation");

        assertThat(catalog.getTable(new ObjectPath(DATABASE, MT_NAME)).getTableKind())
                .isEqualTo(TableKind.VIEW);
        assertThat(listJobIds()).isEqualTo(jobsBefore);
    }

    @Test
    void temporaryTableAtThePathIsRejected() {
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE " + MT_NAME + " (k INT) WITH ('connector' = 'values')");

        assertThatThrownBy(
                        () ->
                                aggregate()
                                        .materializeInto(MT_NAME, Duration.ofSeconds(30))
                                        .execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("would shadow it");
        assertThat(tableExists()).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unstorableQueries")
    void queriesOverTemporaryObjectsOrUnstorableSourcesFailAtMaterializeInto(
            String description,
            Function<MaterializedTableTableApiITCase, Table> query,
            String expectedMessage) {
        final Table table = query.apply(this);

        assertThatThrownBy(() -> table.materializeInto(MT_NAME))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        assertThat(tableExists()).isFalse();
    }

    private static Stream<Arguments> unstorableQueries() {
        final String notExpressibleInSql =
                "The query of a materialized table must be expressible in SQL.";
        final String temporaryObject =
                "The query of a materialized table must only reference permanent catalog objects."
                        + " The refresh job re-runs the stored query text, also from other"
                        + " sessions, which cannot see the temporary ";
        return Stream.of(
                unstorableQuery(
                        "inline function",
                        test -> test.aggregate().select(call(new PlusOne(), $("k"))),
                        notExpressibleInSql),
                unstorableQuery(
                        "temporary system function",
                        test -> {
                            test.tEnv.createTemporarySystemFunction("plus_one_sys", PlusOne.class);
                            return test.aggregate().select(call("plus_one_sys", $("k")));
                        },
                        temporaryObject + "function plus_one_sys."),
                unstorableQuery(
                        "temporary catalog function",
                        test -> {
                            test.tEnv.createTemporaryFunction("plus_one_tmp", PlusOne.class);
                            return test.aggregate().select(call("plus_one_tmp", $("k")));
                        },
                        temporaryObject + "function mt_cat.mt_db.plus_one_tmp."),
                unstorableQuery(
                        "temporary table",
                        test -> {
                            test.createTemporarySource();
                            return test.tEnv.from("tmp_source");
                        },
                        temporaryObject + "table or view mt_cat.mt_db.tmp_source."),
                unstorableQuery(
                        "temporary view over a permanent table",
                        test -> {
                            test.tEnv.createTemporaryView(
                                    "tmp_view", test.tEnv.from("datagen_source"));
                            return test.tEnv.from("tmp_view");
                        },
                        temporaryObject + "table or view mt_cat.mt_db.tmp_view."),
                unstorableQuery(
                        "temporary function in a subquery",
                        test -> {
                            test.tEnv.createTemporarySystemFunction("plus_one_sys", PlusOne.class);
                            final Table subquery =
                                    test.tEnv
                                            .from("datagen_source")
                                            .select(call("plus_one_sys", $("k")));
                            return test.aggregate().filter($("k").in(subquery));
                        },
                        temporaryObject + "function plus_one_sys."),
                unstorableQuery(
                        "DataStream",
                        test -> test.tEnv.fromDataStream(test.executionEnvironment.fromData(1, 2)),
                        notExpressibleInSql),
                unstorableQuery(
                        "changelog DataStream",
                        test ->
                                test.tEnv.fromChangelogStream(
                                        test.executionEnvironment.fromData(
                                                Row.ofKind(RowKind.INSERT, 1))),
                        notExpressibleInSql),
                unstorableQuery(
                        "TableDescriptor",
                        test ->
                                test.tEnv.from(
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column("k", INT())
                                                                .build())
                                                .build()),
                        notExpressibleInSql));
    }

    @Test
    void sqlDdlOverTemporaryObjectsIsUnaffected() throws Exception {
        createTemporarySource();
        tEnv.createTemporaryView("tmp_view", tEnv.from("datagen_source"));
        tEnv.createTemporarySystemFunction("plus_one_sys", PlusOne.class);
        final List<String> tableNames =
                List.of("mt_over_tmp_source", "mt_over_tmp_view", "mt_with_plus_one_sys");

        createContinuousMaterializedTable(
                tableNames.get(0), "SELECT k, COUNT(v) AS cnt FROM tmp_source GROUP BY k");
        createContinuousMaterializedTable(
                tableNames.get(1), "SELECT k, COUNT(v) AS cnt FROM tmp_view GROUP BY k");
        createContinuousMaterializedTable(
                tableNames.get(2), "SELECT k, plus_one_sys(k) AS k_plus_one FROM datagen_source");

        for (String tableName : tableNames) {
            assertThat(
                            MaterializedTableTestUtils.getMaterializedTable(
                                            catalog, new ObjectPath(DATABASE, tableName))
                                    .getRefreshStatus())
                    .isEqualTo(RefreshStatus.ACTIVATED);
        }
        for (String tableName : tableNames) {
            tEnv.executeSql("DROP MATERIALIZED TABLE " + tableName);
        }
    }

    @Test
    void freshnessAtOrAboveTheThresholdResolvesToFullAndIsRejected() {
        assertThatThrownBy(
                        () -> aggregate().materializeInto(MT_NAME, Duration.ofHours(1)).execute())
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "CREATE MATERIALIZED TABLE on a full-mode materialized table requires a workflow scheduler, which is not configured in this environment."
                                + " Only the SQL Gateway provides one, when 'workflow-scheduler.type' is set."
                                + " The refresh mode was resolved to FULL because the freshness INTERVAL '1' HOUR is not below 'materialized-table.refresh-mode.freshness-threshold'."
                                + " Declare a continuous refresh mode (REFRESH_MODE = CONTINUOUS in SQL, continuousRefresh() in the Table API) or use a smaller freshness.");
        assertThat(tableExists()).isFalse();
    }

    @Test
    void sqlFreshnessResolvingToFullIsRejectedWithTheThresholdHint() {
        final String ddl =
                "CREATE MATERIALIZED TABLE "
                        + MT_NAME
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '1' HOUR\n"
                        + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k";

        assertThatThrownBy(() -> tEnv.executeSql(ddl))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "CREATE MATERIALIZED TABLE on a full-mode materialized table requires a workflow scheduler, which is not configured in this environment")
                .hasMessageContaining(
                        "The refresh mode was resolved to FULL because the freshness INTERVAL '1' HOUR is not below 'materialized-table.refresh-mode.freshness-threshold'.");
        assertThat(tableExists()).isFalse();
    }

    @Test
    void explicitFullRefreshIsRejectedWithoutTheThresholdHint() {
        final MaterializedTableDescriptor full = valuesDescriptor().fullRefresh().build();

        assertThatThrownBy(() -> aggregate().materializeInto(MT_NAME, full).execute())
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "CREATE MATERIALIZED TABLE on a full-mode materialized table requires a workflow scheduler, which is not configured in this environment")
                .hasMessageNotContaining("resolved to FULL");
        assertThat(tableExists()).isFalse();
    }

    @Test
    void rejectedReExecutionKeepsTheJobRunning() throws Exception {
        aggregate().materializeInto(MT_NAME, continuousDescriptor()).execute();
        final JobID jobId = getRefreshJobId();
        awaitAllVerticesRunning(jobId);

        final MaterializedTableDescriptor full = valuesDescriptor().fullRefresh().build();
        assertThatThrownBy(() -> aggregate().materializeInto(MT_NAME, full).execute())
                .isInstanceOf(ValidationException.class)
                .hasMessage("Changing of REFRESH MODE is unsupported");

        final CatalogMaterializedTable table = getMaterializedTable();
        assertThat(table.getRefreshMode()).isEqualTo(RefreshMode.CONTINUOUS);
        assertThat(table.getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(getRefreshJobId()).isEqualTo(jobId);
        awaitAllVerticesRunning(jobId);
    }

    @Test
    void declaredSchemaIsStoredInTheCatalog() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE keyed_source (k INT NOT NULL, v BIGINT, ts TIMESTAMP(3)) WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");
        final MaterializedTableDescriptor descriptor =
                valuesDescriptor()
                        .schema(
                                Schema.newBuilder()
                                        .columnByExpression("k_plus_one", "k + 1")
                                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                                        .primaryKey("k")
                                        .build())
                        .continuousRefresh()
                        .build();

        tEnv.from("keyed_source")
                .groupBy($("k"))
                .select($("k"), $("v").sum().as("v"), $("ts").max().as("ts"))
                .materializeInto(MT_NAME, descriptor)
                .execute();

        final Schema stored = getMaterializedTable().getUnresolvedSchema();
        assertThat(stored.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("k_plus_one", "k", "v", "ts");
        assertThat(stored.getPrimaryKey())
                .map(Schema.UnresolvedPrimaryKey::getColumnNames)
                .contains(List.of("k"));
        assertThat(stored.getWatermarkSpecs())
                .extracting(Schema.UnresolvedWatermarkSpec::getColumnName)
                .containsExactly("ts");
        awaitAllVerticesRunning(getRefreshJobId());
    }

    @Test
    void fullDeclarationMatchesTheEquivalentSql() throws Exception {
        // The primary key needs ds to be NOT NULL in the query.
        tEnv.executeSql(
                "CREATE TABLE keyed_source ("
                        + "k INT NOT NULL, v BIGINT, ts TIMESTAMP(3), ds STRING NOT NULL) WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3',\n"
                        + "  'fields.ds.length' = '1'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE sql_mt (\n"
                        + "  k_plus_one AS k + 1,\n"
                        + "  PRIMARY KEY (k, ds) NOT ENFORCED,\n"
                        + "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                        + ")\n"
                        + "COMMENT 'keyed totals'\n"
                        + "DISTRIBUTED BY HASH(k) INTO 2 BUCKETS\n"
                        + "PARTITIONED BY (ds)\n"
                        + "WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT k, ds, SUM(v) AS v, MAX(ts) AS ts FROM keyed_source GROUP BY k, ds");

        tEnv.from("keyed_source")
                .groupBy($("k"), $("ds"))
                .select($("k"), $("ds"), $("v").sum().as("v"), $("ts").max().as("ts"))
                .materializeInto(
                        "api_mt",
                        valuesDescriptor()
                                .schema(
                                        Schema.newBuilder()
                                                .columnByExpression("k_plus_one", "k + 1")
                                                .watermark("ts", "ts - INTERVAL '5' SECOND")
                                                .primaryKey("k", "ds")
                                                .build())
                                .comment("keyed totals")
                                .distributedByHash(2, "k")
                                .partitionedBy("ds")
                                .continuousRefresh()
                                .build())
                .execute();

        final ResolvedCatalogMaterializedTable sqlTable = getResolvedMaterializedTable("sql_mt");
        final ResolvedCatalogMaterializedTable apiTable = getResolvedMaterializedTable("api_mt");
        assertThat(apiTable.getResolvedSchema()).isEqualTo(sqlTable.getResolvedSchema());
        assertThat(apiTable.getOptions()).isEqualTo(sqlTable.getOptions());
        assertThat(apiTable.getPartitionKeys()).isEqualTo(sqlTable.getPartitionKeys());
        assertThat(apiTable.getDistribution()).isEqualTo(sqlTable.getDistribution());
        assertThat(apiTable.getComment()).isEqualTo(sqlTable.getComment());
        assertThat(apiTable.getDefinitionFreshness()).isEqualTo(sqlTable.getDefinitionFreshness());
        assertThat(apiTable.getLogicalRefreshMode()).isEqualTo(sqlTable.getLogicalRefreshMode());
        assertThat(apiTable.getRefreshMode()).isEqualTo(sqlTable.getRefreshMode());
        assertThat(apiTable.getStartMode()).isEqualTo(sqlTable.getStartMode());
        for (String tableName : List.of("sql_mt", "api_mt")) {
            awaitAllVerticesRunning(
                    JobID.fromHexString(
                            MaterializedTableTestUtils.getRefreshHandler(
                                            catalog, new ObjectPath(DATABASE, tableName))
                                    .getJobId()));
        }
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalog(EnvironmentSettings settings) {
        return createTableEnvOnSharedCatalog(settings, new Configuration());
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalogWithSavepointDirectory() {
        Configuration envConfiguration = new Configuration();
        envConfiguration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toUri().toString());
        return createTableEnvOnSharedCatalog(
                EnvironmentSettings.inStreamingMode(), envConfiguration);
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalog(
            EnvironmentSettings settings, Configuration envConfiguration) {
        return createTableEnvOnSharedCatalog(
                settings,
                envConfiguration,
                MaterializedTableTableApiITCase::targetMiniClusterFromTableConfig);
    }

    /**
     * The root names a cluster that can't be reached, by REST address and by high availability; the
     * TableConfig reaches the MiniCluster only through 'jobmanager.rpc.address'.
     */
    private StreamTableEnvironment
            createTableEnvOnSharedCatalogWithAnUnreachableClusterInTheRoot() {
        final Configuration envConfiguration = unreachableHighAvailability();
        envConfiguration.set(RestOptions.ADDRESS, "unreachable.invalid");
        return createTableEnvOnSharedCatalog(
                EnvironmentSettings.inStreamingMode(),
                envConfiguration,
                MaterializedTableTableApiITCase::targetMiniClusterByRpcAddressFromTableConfig);
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalog(
            EnvironmentSettings settings,
            Configuration envConfiguration,
            Consumer<StreamTableEnvironment> targetMiniCluster) {
        Configuration configuration = new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.addAll(envConfiguration);
        configuration.set(DeploymentOptions.TARGET, "remote");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        StreamTableEnvironment otherEnv = StreamTableEnvironment.create(env, settings);
        targetMiniCluster.accept(otherEnv);
        otherEnv.registerCatalog("mt_cat", catalog);
        otherEnv.useCatalog("mt_cat");
        otherEnv.useDatabase(DATABASE);
        return otherEnv;
    }

    private StreamTableEnvironment createTableEnvOnSharedCatalogWithTheClusterOnlyInTheRoot(
            EnvironmentSettings settings) {
        final Configuration configuration =
                new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(DeploymentOptions.TARGET, "remote");

        final StreamExecutionEnvironment env = new StreamExecutionEnvironment(configuration);
        env.setParallelism(1);
        final StreamTableEnvironment rootTargetEnv = StreamTableEnvironment.create(env, settings);
        rootTargetEnv.registerCatalog("mt_cat", catalog);
        rootTargetEnv.useCatalog("mt_cat");
        rootTargetEnv.useDatabase(DATABASE);
        return rootTargetEnv;
    }

    private StreamTableEnvironment createTableEnvRegisteringTheCatalogAs(String catalogName) {
        final Configuration configuration =
                new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(DeploymentOptions.TARGET, "remote");
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toUri().toString());

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        final StreamTableEnvironment renamingEnv = StreamTableEnvironment.create(env);
        targetMiniClusterFromTableConfig(renamingEnv);
        renamingEnv.registerCatalog(catalogName, catalog);
        renamingEnv.useCatalog(catalogName);
        renamingEnv.useDatabase(DATABASE);
        return renamingEnv;
    }

    private static void targetMiniClusterFromTableConfig(StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().set(DeploymentOptions.TARGET, "remote");
        tableEnv.getConfig()
                .set(
                        RestOptions.ADDRESS,
                        MINI_CLUSTER.getClientConfiguration().get(RestOptions.ADDRESS));
    }

    private static void targetMiniClusterByRpcAddressFromTableConfig(
            StreamTableEnvironment tableEnv) {
        tableEnv.getConfig().set(DeploymentOptions.TARGET, "remote");
        tableEnv.getConfig()
                .set(
                        JobManagerOptions.ADDRESS,
                        MINI_CLUSTER.getClientConfiguration().get(JobManagerOptions.ADDRESS));
    }

    private static Configuration unreachableHighAvailability() {
        final Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_MODE,
                "org.apache.flink.table.planner.NonExistentHighAvailabilityServicesFactory");
        return configuration;
    }

    private static String continuousMaterializedTableDdl() {
        return "CREATE MATERIALIZED TABLE "
                + MT_NAME
                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                + " FRESHNESS = INTERVAL '30' SECOND\n"
                + " REFRESH_MODE = CONTINUOUS\n"
                + " AS SELECT k, COUNT(v) AS cnt FROM datagen_source GROUP BY k";
    }

    private static String alterAsQueryAddingMaxV() {
        return "ALTER MATERIALIZED TABLE "
                + MT_NAME
                + " AS SELECT k, COUNT(v) AS cnt, MAX(v) AS max_v FROM datagen_source GROUP BY k";
    }

    private static String insertOnlyMaterializedTableDdl() {
        return "CREATE MATERIALIZED TABLE "
                + MT_NAME
                + " WITH ('connector' = 'values', 'sink-insert-only' = 'true')\n"
                + " FRESHNESS = INTERVAL '30' SECOND\n"
                + " REFRESH_MODE = CONTINUOUS\n"
                + " AS SELECT k, v FROM datagen_source";
    }

    private static String alterAsQueryToAnUpdatingAggregate() {
        return "ALTER MATERIALIZED TABLE "
                + MT_NAME
                + " AS SELECT k, v, COUNT(*) AS cnt FROM datagen_source GROUP BY k, v";
    }

    private static Predicate<CatalogBaseTable> hasExpandedQuery(
            Predicate<String> expandedQueryMatches) {
        return table ->
                table instanceof CatalogMaterializedTable
                        && expandedQueryMatches.test(
                                ((CatalogMaterializedTable) table).getExpandedQuery());
    }

    private static Predicate<CatalogBaseTable> firstMatchAfter(
            Predicate<CatalogBaseTable> earlierWrite, Predicate<CatalogBaseTable> laterWrite) {
        final AtomicBoolean earlierWriteSeen = new AtomicBoolean();
        return table -> {
            if (earlierWriteSeen.get()) {
                return laterWrite.test(table);
            }
            if (earlierWrite.test(table)) {
                earlierWriteSeen.set(true);
            }
            return false;
        };
    }

    private static Arguments unstorableQuery(
            String description,
            Function<MaterializedTableTableApiITCase, Table> query,
            String expectedMessage) {
        return Arguments.of(description, query, expectedMessage);
    }

    private void createTemporarySource() {
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE tmp_source (\n"
                        + "  k INT,\n"
                        + "  v BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '5',\n"
                        + "  'fields.k.min' = '1',\n"
                        + "  'fields.k.max' = '3'\n"
                        + ")");
    }

    private void createContinuousMaterializedTable(String tableName, String query) {
        tEnv.executeSql(
                "CREATE MATERIALIZED TABLE "
                        + tableName
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')\n"
                        + " FRESHNESS = INTERVAL '30' SECOND\n"
                        + " REFRESH_MODE = CONTINUOUS\n"
                        + " AS "
                        + query);
    }

    private void createRegularTable() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + MT_NAME
                        + " (k INT, cnt BIGINT)"
                        + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");
    }

    private Table aggregate() {
        return aggregate(tEnv, "datagen_source");
    }

    private static Table aggregate(TableEnvironment tableEnv, String sourceTable) {
        return tableEnv.from(sourceTable).groupBy($("k")).select($("k"), $("v").count().as("cnt"));
    }

    private static MaterializedTableDescriptor continuousDescriptor() {
        return valuesDescriptor().continuousRefresh().build();
    }

    private static MaterializedTableDescriptor.Builder valuesDescriptor() {
        return MaterializedTableDescriptor.newBuilder()
                .option("connector", "values")
                .option("sink-insert-only", "false")
                .freshness(Duration.ofSeconds(30));
    }

    private boolean tableExists() {
        return catalog.tableExists(new ObjectPath(DATABASE, MT_NAME));
    }

    private CatalogMaterializedTable getMaterializedTable() throws Exception {
        return MaterializedTableTestUtils.getMaterializedTable(
                catalog, new ObjectPath(DATABASE, MT_NAME));
    }

    private ResolvedCatalogMaterializedTable getResolvedMaterializedTable(String tableName)
            throws Exception {
        final CatalogManager catalogManager = ((TableEnvironmentInternal) tEnv).getCatalogManager();
        return catalogManager.resolveCatalogMaterializedTable(
                MaterializedTableTestUtils.getMaterializedTable(
                        catalog, new ObjectPath(DATABASE, tableName)));
    }

    private JobID getRefreshJobId() throws Exception {
        return JobID.fromHexString(getRefreshHandler().getJobId());
    }

    private ContinuousRefreshHandler getRefreshHandler() throws Exception {
        return MaterializedTableTestUtils.getRefreshHandler(
                catalog, new ObjectPath(DATABASE, MT_NAME));
    }

    private Set<JobID> listJobIds() throws Exception {
        return clusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).stream()
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toSet());
    }

    private void awaitJobStatus(JobID jobId, JobStatus expectedStatus) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return clusterClient
                                        .getJobStatus(jobId)
                                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                == expectedStatus;
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
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return RestMapperUtils.getStrictObjectMapper()
                .valueToTree(checkpointConfig)
                .get(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_INTERVAL)
                .asLong();
    }

    private void awaitAllVerticesRunning(JobID jobId) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        JobDetailsInfo details =
                                clusterClient
                                        .getJobDetails(jobId)
                                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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

    /** A user-defined function for the temporary and inline function cases. */
    public static class PlusOne extends ScalarFunction {
        public Integer eval(Integer value) {
            return value == null ? null : value + 1;
        }
    }

    private static final class ProgramContextEnvironment extends StreamExecutionEnvironment {

        private ProgramContextEnvironment() {}

        static void setAsContext() {
            initializeContextEnvironment(
                    configuration ->
                            new StreamExecutionEnvironment(configuration) {
                                @Override
                                public JobClient executeAsync(StreamGraph streamGraph) {
                                    throw new IllegalStateException(
                                            "Submitted through the program's own pipeline executor.");
                                }
                            });
        }

        static void unsetAsContext() {
            resetContextEnvironment();
        }
    }
}

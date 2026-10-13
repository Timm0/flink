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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.TestingJobSubmitter;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.createTableEnvironment;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.getMaterializedTable;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.getRefreshHandler;
import static org.apache.flink.table.planner.delegation.materializedtable.MaterializedTableTestUtils.recordRefreshHandler;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests where refresh jobs are submitted and recorded, with which coordinates, and which restarts
 * are refused, using a job submitter that records submissions instead of running them.
 */
class RefreshJobTargetPinningTest {

    private static final String CATALOG = "mt_cat";
    private static final String DATABASE = "mt_db";
    private static final String MATERIALIZED_TABLE = "my_mt";

    private static final String SESSION_TARGET = "kubernetes-session";
    private static final String CLUSTER_ID_KEY = "kubernetes.cluster-id";
    private static final String TARGET_KEY = DeploymentOptions.TARGET.key();
    private static final String CREATE_MATERIALIZED_TABLE =
            "CREATE MATERIALIZED TABLE "
                    + MATERIALIZED_TABLE
                    + "\n FRESHNESS = INTERVAL '30' SECOND"
                    + "\n REFRESH_MODE = CONTINUOUS"
                    + "\n AS SELECT k, v FROM datagen_source";

    private static final ObjectPath TABLE_PATH = new ObjectPath(DATABASE, MATERIALIZED_TABLE);
    private static final String OTHER_CLUSTER_ID = "B";
    private static final String SAVEPOINT_DIRECTORY = "file:///savepoints";
    private static final String RESTORE_PATH = "file:///savepoints/savepoint-1";

    private static final String ALTER_AS_QUERY =
            "ALTER MATERIALIZED TABLE "
                    + MATERIALIZED_TABLE
                    + " AS SELECT k, v, k AS k2 FROM datagen_source";
    private static final String REDEPLOY =
            "CREATE OR ALTER MATERIALIZED TABLE "
                    + MATERIALIZED_TABLE
                    + "\n FRESHNESS = INTERVAL '30' SECOND"
                    + "\n REFRESH_MODE = CONTINUOUS"
                    + "\n AS SELECT k, v FROM datagen_source";
    private static final String RESUME =
            "ALTER MATERIALIZED TABLE " + MATERIALIZED_TABLE + " RESUME";
    private static final String SUSPEND =
            "ALTER MATERIALIZED TABLE " + MATERIALIZED_TABLE + " SUSPEND";
    private static final String DROP = "DROP MATERIALIZED TABLE " + MATERIALIZED_TABLE;
    private static final String NO_WORKFLOW_SCHEDULER =
            "requires a workflow scheduler, which is not configured in this environment. "
                    + "Only the SQL Gateway provides one";

    private final GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(CATALOG, DATABASE);

    @ParameterizedTest(name = "{0}")
    @MethodSource("unusableTargets")
    void createWithAnUnusableTargetIsRejectedBeforeAnyCatalogChange(
            String description,
            Map<String, String> rootOptions,
            Map<String, String> tableConfigOptions,
            List<String> messageParts) {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnv(Configuration.fromMap(rootOptions), jobSubmitter);
        tableConfigOptions.forEach(tableEnv.getConfig()::set);
        createSourceTable(tableEnv);

        assertThatThrownBy(() -> tableEnv.executeSql(CREATE_MATERIALIZED_TABLE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContainingAll(messageParts.toArray(new String[0]));
        assertThat(catalog.tableExists(TABLE_PATH)).isFalse();
        assertThat(jobSubmitter.targets).isEmpty();
    }

    private static Stream<Arguments> unusableTargets() {
        return Stream.of(
                Arguments.of(
                        "unset target",
                        Map.of(),
                        Map.of(),
                        List.of(
                                "Unsupported execution target detected: null",
                                "(read from 'execution.target' in the "
                                        + RefreshJobTargetResolver.ROOT_LAYER
                                        + ")")),
                Arguments.of(
                        "embedded target",
                        Map.of(),
                        Map.of(TARGET_KEY, "embedded"),
                        List.of("Execution target 'embedded' is not supported")),
                Arguments.of(
                        "target in the root without a cluster id",
                        Map.of(TARGET_KEY, SESSION_TARGET),
                        Map.of(),
                        List.of(
                                "requires 'kubernetes.cluster-id' to be configured",
                                "It must be set on TableConfig or the "
                                        + RefreshJobTargetResolver.ROOT_LAYER)),
                Arguments.of(
                        "target on the TableConfig, cluster id only in the root",
                        Map.of(CLUSTER_ID_KEY, "mt-refresh"),
                        Map.of(TARGET_KEY, SESSION_TARGET),
                        List.of(
                                "requires 'kubernetes.cluster-id' to be configured",
                                "where 'execution.target' is set")),
                Arguments.of(
                        "blank cluster id on the TableConfig",
                        Map.of(),
                        Map.of(TARGET_KEY, SESSION_TARGET, CLUSTER_ID_KEY, " "),
                        List.of("requires 'kubernetes.cluster-id' to be configured")),
                Arguments.of(
                        "blank address for remote on the TableConfig",
                        Map.of(),
                        Map.of(TARGET_KEY, "remote", "rest.address", " "),
                        List.of(
                                "Execution target 'remote' requires 'rest.address' to be configured")),
                Arguments.of(
                        "remote on the TableConfig, address and high availability only in the root",
                        Map.of(
                                "rest.address",
                                "program-cluster",
                                "high-availability.type",
                                "zookeeper"),
                        Map.of(TARGET_KEY, "remote"),
                        List.of(
                                "Execution target 'remote' requires 'rest.address' to be configured",
                                "where 'execution.target' is set")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("resolvedTargets")
    void createSubmitsToAndRecordsTheResolvedTarget(
            String description,
            Map<String, String> rootOptions,
            Map<String, String> tableConfigOptions,
            RefreshJobTarget expectedTarget)
            throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnv(Configuration.fromMap(rootOptions), jobSubmitter);
        tableConfigOptions.forEach(tableEnv.getConfig()::set);

        createMaterializedTable(tableEnv);

        assertThat(jobSubmitter.targets).containsExactly(expectedTarget);
        final ContinuousRefreshHandler refreshHandler = getRefreshHandler(catalog, TABLE_PATH);
        assertThat(RefreshJobTarget.recordedIn(refreshHandler)).isEqualTo(expectedTarget);
        assertThat(refreshHandler.getJobId()).isEqualTo(jobSubmitter.jobId);
    }

    private static Stream<Arguments> resolvedTargets() {
        return Stream.of(
                Arguments.of(
                        "target and cluster id on the TableConfig",
                        Map.of(),
                        Map.of(TARGET_KEY, SESSION_TARGET, CLUSTER_ID_KEY, "mt-refresh"),
                        new RefreshJobTarget(SESSION_TARGET, "mt-refresh")),
                Arguments.of(
                        "target and cluster id in the root",
                        Map.of(
                                TARGET_KEY,
                                "yarn-session",
                                "yarn.application.id",
                                "application_1_0001"),
                        Map.of(),
                        new RefreshJobTarget("yarn-session", "application_1_0001")),
                Arguments.of(
                        "target in the root, cluster id only on the TableConfig",
                        Map.of(TARGET_KEY, SESSION_TARGET),
                        Map.of(CLUSTER_ID_KEY, "mt-refresh"),
                        new RefreshJobTarget(SESSION_TARGET, "mt-refresh")),
                Arguments.of(
                        "target in the root, the TableConfig's cluster id wins",
                        Map.of(TARGET_KEY, SESSION_TARGET, CLUSTER_ID_KEY, "A"),
                        Map.of(CLUSTER_ID_KEY, "B"),
                        new RefreshJobTarget(SESSION_TARGET, "B")),
                Arguments.of(
                        "remote on the TableConfig, reached through jobmanager.rpc.address",
                        Map.of(),
                        Map.of(TARGET_KEY, "remote", "jobmanager.rpc.address", "refresh-cluster"),
                        new RefreshJobTarget("remote", "")),
                Arguments.of(
                        "remote on the TableConfig with high availability and no address",
                        Map.of(),
                        Map.of(TARGET_KEY, "remote", "high-availability.type", "zookeeper"),
                        new RefreshJobTarget("remote", "")),
                Arguments.of(
                        "remote in the root without an address",
                        Map.of(TARGET_KEY, "remote"),
                        Map.of(),
                        new RefreshJobTarget("remote", "")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("restartsOnAnotherCluster")
    void restartOnAnotherClusterIsRejectedBeforeTheJobIsStopped(
            String description,
            String statement,
            RefreshStatus refreshStatus,
            Map<String, String> programOptions,
            String programCluster)
            throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                refreshStatus,
                refreshStatus == RefreshStatus.SUSPENDED
                        ? new ContinuousRefreshHandler(
                                SESSION_TARGET, "A", jobSubmitter.jobId, RESTORE_PATH)
                        : new ContinuousRefreshHandler(SESSION_TARGET, "A", jobSubmitter.jobId));
        final ContinuousRefreshHandler recordedHandler = getRefreshHandler(catalog, TABLE_PATH);
        programOptions.forEach(tableEnv.getConfig()::set);
        tableEnv.getConfig().set(CheckpointingOptions.SAVEPOINT_DIRECTORY, SAVEPOINT_DIRECTORY);

        assertThatThrownBy(() -> tableEnv.executeSql(statement))
                .isInstanceOf(ValidationException.class)
                .hasMessageContainingAll(
                        "runs on execution.target 'kubernetes-session', kubernetes.cluster-id 'A'",
                        "this program submits refresh jobs to " + programCluster);

        assertThat(jobSubmitter.targets).hasSize(1);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(refreshStatus);
        assertThat(getRefreshHandler(catalog, TABLE_PATH))
                .usingRecursiveComparison()
                .isEqualTo(recordedHandler);
    }

    private static Stream<Arguments> restartsOnAnotherCluster() {
        final Map<String, String> otherSessionCluster =
                Map.of(TARGET_KEY, SESSION_TARGET, CLUSTER_ID_KEY, OTHER_CLUSTER_ID);
        final String otherSessionClusterDescription =
                "execution.target 'kubernetes-session', kubernetes.cluster-id 'B'";
        return Stream.of(
                Arguments.of(
                        "alter as query",
                        ALTER_AS_QUERY,
                        RefreshStatus.ACTIVATED,
                        otherSessionCluster,
                        otherSessionClusterDescription),
                Arguments.of(
                        "redeploy",
                        REDEPLOY,
                        RefreshStatus.ACTIVATED,
                        otherSessionCluster,
                        otherSessionClusterDescription),
                Arguments.of(
                        "resume",
                        RESUME,
                        RefreshStatus.SUSPENDED,
                        otherSessionCluster,
                        otherSessionClusterDescription),
                Arguments.of(
                        "resume with another target",
                        RESUME,
                        RefreshStatus.SUSPENDED,
                        Map.of(TARGET_KEY, "remote", "rest.address", "another-standalone-cluster"),
                        "execution.target 'remote'"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("restartsOnTheProgramsCluster")
    void resumeRestartsOnTheProgramsClusterWhenItMatchesTheRecordedOne(
            String description,
            Map<String, String> rootOptions,
            String recordedTarget,
            @Nullable String recordedClusterId,
            Map<String, String> programOptions,
            RefreshJobTarget expectedTarget)
            throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(Configuration.fromMap(rootOptions), jobSubmitter);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.SUSPENDED,
                new ContinuousRefreshHandler(
                        recordedTarget, recordedClusterId, jobSubmitter.jobId, RESTORE_PATH));
        programOptions.forEach(tableEnv.getConfig()::set);

        tableEnv.executeSql(RESUME);

        assertThat(jobSubmitter.targets).hasSize(2).last().isEqualTo(expectedTarget);
        assertThat(RefreshJobTarget.recordedIn(getRefreshHandler(catalog, TABLE_PATH)))
                .isEqualTo(expectedTarget);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);
    }

    private static Stream<Arguments> restartsOnTheProgramsCluster() {
        final Map<String, String> sessionClusterA =
                Map.of(TARGET_KEY, SESSION_TARGET, CLUSTER_ID_KEY, "A");
        return Stream.of(
                Arguments.of(
                        "same cluster id",
                        sessionClusterA,
                        SESSION_TARGET,
                        "A",
                        Map.of(),
                        new RefreshJobTarget(SESSION_TARGET, "A")),
                Arguments.of(
                        "recorded without a cluster id",
                        sessionClusterA,
                        SESSION_TARGET,
                        null,
                        Map.of(TARGET_KEY, SESSION_TARGET, CLUSTER_ID_KEY, OTHER_CLUSTER_ID),
                        new RefreshJobTarget(SESSION_TARGET, OTHER_CLUSTER_ID)),
                Arguments.of(
                        "old StandaloneClusterId of a remote job",
                        Map.of(TARGET_KEY, "remote"),
                        "remote",
                        "StandaloneClusterId",
                        Map.of(),
                        new RefreshJobTarget("remote", "")),
                Arguments.of(
                        "remote job, program at another REST address",
                        Map.of(TARGET_KEY, "remote", "rest.address", "program-cluster"),
                        "remote",
                        "",
                        Map.of("rest.address", "another-standalone-cluster"),
                        new RefreshJobTarget("remote", "")));
    }

    @Test
    void theSqlGatewayRestartsOnItsSessionsTarget() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(true);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.SUSPENDED,
                new ContinuousRefreshHandler(
                        SESSION_TARGET, "A", jobSubmitter.jobId, RESTORE_PATH));
        targetOtherSessionCluster(tableEnv);

        tableEnv.executeSql(RESUME);

        assertThat(jobSubmitter.targets)
                .last()
                .isEqualTo(new RefreshJobTarget(SESSION_TARGET, OTHER_CLUSTER_ID));
    }

    @Test
    void theSqlGatewayStopsAnAlteredTablesJobWithoutCheckingTheRecordedCluster() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(true);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        targetOtherSessionCluster(tableEnv);

        assertThatThrownBy(() -> tableEnv.executeSql(ALTER_AS_QUERY))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Failed to suspend");

        assertThat(jobSubmitter.targets).hasSize(1);
    }

    @Test
    void theSqlGatewayReportsAnUnreadableHandlerOfAnAlteredTableAsAFailedStop() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(true);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        recordUnreadableRefreshHandler(RefreshStatus.ACTIVATED);

        assertThatThrownBy(() -> tableEnv.executeSql(ALTER_AS_QUERY))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Failed to suspend the continuous refresh job")
                .hasStackTraceContaining("Failed to deserialize ContinuousRefreshHandler.");
    }

    @Test
    void theSqlGatewayResolvesItsTargetBeforeItReadsTheHandlerOnResume() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(true);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        recordUnreadableRefreshHandler(RefreshStatus.SUSPENDED);
        tableEnv.getConfig().set(DeploymentOptions.TARGET, "local");

        assertThatThrownBy(() -> tableEnv.executeSql(RESUME))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'local' is not supported");
    }

    @Test
    void fullModeTableOperationsThatNeedTheSchedulerNameTheSqlGatewayAndChangeNothing()
            throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        rewriteAsActivatedFullModeTable();

        assertThatThrownBy(() -> tableEnv.executeSql(SUSPEND))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(NO_WORKFLOW_SCHEDULER);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);

        assertThatThrownBy(() -> tableEnv.executeSql(DROP))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(NO_WORKFLOW_SCHEDULER);
        assertThat(catalog.tableExists(TABLE_PATH)).isTrue();

        tableEnv.executeSql(ALTER_AS_QUERY);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getExpandedQuery()).contains("k2");

        tableEnv.executeSql("ALTER MATERIALIZED TABLE " + MATERIALIZED_TABLE + " REFRESH");
        assertThat(jobSubmitter.targets)
                .hasSize(2)
                .last()
                .isEqualTo(new RefreshJobTarget(SESSION_TARGET, "A"));
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);

        catalog.alterTable(
                TABLE_PATH,
                getMaterializedTable(catalog, TABLE_PATH)
                        .copy(RefreshStatus.SUSPENDED, "suspended workflow", new byte[0]),
                false);

        assertThatThrownBy(() -> tableEnv.executeSql(RESUME))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(NO_WORKFLOW_SCHEDULER);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.SUSPENDED);

        assertThat(jobSubmitter.targets).hasSize(2);
    }

    private TableEnvironment tableEnv(
            Configuration rootConfiguration, TestingJobSubmitter jobSubmitter) {
        return createTableEnvironment(rootConfiguration, CATALOG, catalog, jobSubmitter);
    }

    private TableEnvironment tableEnvWithMaterializedTable(
            Configuration rootConfiguration, TestingJobSubmitter jobSubmitter) {
        final TableEnvironment tableEnv = tableEnv(rootConfiguration, jobSubmitter);
        createMaterializedTable(tableEnv);
        return tableEnv;
    }

    private static void createMaterializedTable(TableEnvironment tableEnv) {
        createSourceTable(tableEnv);
        tableEnv.executeSql(CREATE_MATERIALIZED_TABLE);
    }

    private static void createSourceTable(TableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE datagen_source (k INT, v BIGINT) WITH ('connector' = 'datagen')");
    }

    private static Configuration sessionClusterA() {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(DeploymentOptions.TARGET, SESSION_TARGET);
        rootConfiguration.setString(CLUSTER_ID_KEY, "A");
        return rootConfiguration;
    }

    private static void targetOtherSessionCluster(TableEnvironment tableEnv) {
        tableEnv.getConfig().set(DeploymentOptions.TARGET, SESSION_TARGET);
        tableEnv.getConfig().set(CLUSTER_ID_KEY, OTHER_CLUSTER_ID);
        tableEnv.getConfig().set(CheckpointingOptions.SAVEPOINT_DIRECTORY, SAVEPOINT_DIRECTORY);
    }

    private void recordUnreadableRefreshHandler(RefreshStatus refreshStatus) throws Exception {
        catalog.alterTable(
                TABLE_PATH,
                getMaterializedTable(catalog, TABLE_PATH)
                        .copy(refreshStatus, "unreadable handler", new byte[] {1, 2, 3}),
                false);
    }

    private void rewriteAsActivatedFullModeTable() throws Exception {
        final CatalogMaterializedTable continuousTable = getMaterializedTable(catalog, TABLE_PATH);
        final CatalogMaterializedTable.Builder builder =
                CatalogMaterializedTable.newBuilder()
                        .schema(continuousTable.getUnresolvedSchema())
                        .comment(continuousTable.getComment())
                        .partitionKeys(continuousTable.getPartitionKeys())
                        .options(continuousTable.getOptions())
                        .originalQuery(continuousTable.getOriginalQuery())
                        .expandedQuery(continuousTable.getExpandedQuery())
                        .freshness(continuousTable.getDefinitionFreshness())
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.FULL)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.FULL)
                        .refreshStatus(RefreshStatus.ACTIVATED)
                        .refreshHandlerDescription("workflow of the SQL Gateway's scheduler")
                        .serializedRefreshHandler(new byte[0]);
        continuousTable.getStartMode().ifPresent(builder::startMode);
        catalog.dropTable(TABLE_PATH, false);
        catalog.createTable(TABLE_PATH, builder.build(), false);
    }
}

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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
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
import org.junit.jupiter.params.provider.ValueSource;

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

    private static final ObjectPath TABLE_PATH = new ObjectPath(DATABASE, MATERIALIZED_TABLE);
    private static final String OTHER_CLUSTER_ID = "B";
    private static final String APPLICATION_TARGET = "kubernetes-application";
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

    @Test
    void testRefreshJobRunsOnAndIsRecordedWithTheResolvedCluster() throws Exception {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(DeploymentOptions.TARGET, SESSION_TARGET);
        rootConfiguration.setString(CLUSTER_ID_KEY, "A");

        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv = tableEnv(rootConfiguration, jobSubmitter);
        tableEnv.getConfig().set(CLUSTER_ID_KEY, "B");

        createMaterializedTable(tableEnv);

        assertThat(jobSubmitter.targets).containsExactly(new RefreshJobTarget(SESSION_TARGET, "A"));

        final ContinuousRefreshHandler refreshHandler = getRefreshHandler(catalog, TABLE_PATH);
        assertThat(refreshHandler.getExecutionTarget()).isEqualTo(SESSION_TARGET);
        assertThat(refreshHandler.getClusterId()).isEqualTo("A");
        assertThat(refreshHandler.getJobId()).isEqualTo(jobSubmitter.jobId);
    }

    @Test
    void testRestAddressOfARemoteTargetIsPinnedFromTheTableConfig() throws Exception {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(RestOptions.ADDRESS, "program-cluster");
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv = tableEnv(rootConfiguration, jobSubmitter);
        tableEnv.getConfig().set(DeploymentOptions.TARGET, "remote");
        tableEnv.getConfig().set(JobManagerOptions.ADDRESS, "refresh-cluster");

        createMaterializedTable(tableEnv);

        assertThat(jobSubmitter.executionConfig.toMap())
                .containsEntry(RestOptions.ADDRESS.key(), "refresh-cluster");

        tableEnv.executeSql("ALTER MATERIALIZED TABLE " + MATERIALIZED_TABLE + " REFRESH");

        assertThat(jobSubmitter.targets).hasSize(2);
        assertThat(jobSubmitter.executionConfig.toMap())
                .containsEntry(RestOptions.ADDRESS.key(), "refresh-cluster");
    }

    @Test
    void testHighAvailabilityOfTheRootIsReplacedWhenTheTableConfigSetsTheTarget() throws Exception {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        rootConfiguration.set(HighAvailabilityOptions.HA_CLUSTER_ID, "/program-cluster");
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv = tableEnv(rootConfiguration, jobSubmitter);
        tableEnv.getConfig().set(DeploymentOptions.TARGET, SESSION_TARGET);
        tableEnv.getConfig().set(CLUSTER_ID_KEY, "refresh-cluster");

        createMaterializedTable(tableEnv);

        assertThat(jobSubmitter.executionConfig.toMap())
                .containsEntry(HighAvailabilityOptions.HA_MODE.key(), "NONE")
                .doesNotContainKey(RestOptions.ADDRESS.key());
    }

    @Test
    void testDeprecatedHighAvailabilityKeysOnTheTableConfigArePinned() throws Exception {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(HighAvailabilityOptions.HA_MODE, "kubernetes");
        rootConfiguration.set(HighAvailabilityOptions.HA_CLUSTER_ID, "/program-cluster");
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv = tableEnv(rootConfiguration, jobSubmitter);
        tableEnv.getConfig().set(DeploymentOptions.TARGET, SESSION_TARGET);
        tableEnv.getConfig().set(CLUSTER_ID_KEY, "refresh-cluster");
        tableEnv.getConfig().set("high-availability", "zookeeper");
        tableEnv.getConfig().set("high-availability.zookeeper.path.namespace", "/refresh-cluster");

        createMaterializedTable(tableEnv);

        assertThat(jobSubmitter.executionConfig.toMap())
                .containsEntry(HighAvailabilityOptions.HA_MODE.key(), "zookeeper")
                .containsEntry(HighAvailabilityOptions.HA_CLUSTER_ID.key(), "/refresh-cluster");
    }

    @Test
    void testTargetFromTheRootKeepsTheMergedCoordinates() throws Exception {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(DeploymentOptions.TARGET, "remote");
        rootConfiguration.set(RestOptions.ADDRESS, "session-cluster");
        rootConfiguration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv = tableEnv(rootConfiguration, jobSubmitter);

        createMaterializedTable(tableEnv);

        assertThat(jobSubmitter.executionConfig.toMap())
                .doesNotContainKeys(
                        RestOptions.ADDRESS.key(), HighAvailabilityOptions.HA_MODE.key());
    }

    @ParameterizedTest
    @ValueSource(strings = {ALTER_AS_QUERY, REDEPLOY})
    void alterAndRedeployOnAnotherClusterAreRejectedBeforeTheStop(String statement)
            throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        final ContinuousRefreshHandler recordedHandler = getRefreshHandler(catalog, TABLE_PATH);
        targetOtherSessionCluster(tableEnv);

        assertThatThrownBy(() -> tableEnv.executeSql(statement))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "runs on execution.target 'kubernetes-session', kubernetes.cluster-id 'A'")
                .hasMessageContaining("kubernetes.cluster-id 'B'");

        assertThat(jobSubmitter.targets).hasSize(1);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(getRefreshHandler(catalog, TABLE_PATH))
                .usingRecursiveComparison()
                .isEqualTo(recordedHandler);
    }

    @Test
    void resumeOnAnotherClusterIsRejected() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.SUSPENDED,
                new ContinuousRefreshHandler(
                        SESSION_TARGET, "A", jobSubmitter.jobId, RESTORE_PATH));
        targetOtherSessionCluster(tableEnv);

        assertThatThrownBy(() -> tableEnv.executeSql(RESUME))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("kubernetes.cluster-id 'A'");

        assertThat(jobSubmitter.targets).hasSize(1);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.SUSPENDED);
    }

    @Test
    void resumeOfAHandlerWithoutClusterIdRestartsOnTheProgramsCluster() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.SUSPENDED,
                new ContinuousRefreshHandler(SESSION_TARGET, null, jobSubmitter.jobId));
        targetOtherSessionCluster(tableEnv);

        tableEnv.executeSql(RESUME);

        assertThat(jobSubmitter.targets)
                .last()
                .isEqualTo(new RefreshJobTarget(SESSION_TARGET, OTHER_CLUSTER_ID));
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);
    }

    @Test
    void resumeOfALegacyRemoteHandlerRestartsOnTheProgramsRemoteTarget() throws Exception {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(DeploymentOptions.TARGET, "remote");
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(rootConfiguration, jobSubmitter);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.SUSPENDED,
                new ContinuousRefreshHandler(
                        "remote", "StandaloneClusterId", jobSubmitter.jobId, RESTORE_PATH));

        tableEnv.executeSql(RESUME);

        assertThat(jobSubmitter.targets)
                .hasSize(2)
                .last()
                .isEqualTo(new RefreshJobTarget("remote", ""));
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);
    }

    @Test
    void restartsOfAJobInItsOwnApplicationClusterAreRejected() throws Exception {
        final TestingJobSubmitter jobSubmitter = TestingJobSubmitter.recording(false);
        final TableEnvironment tableEnv =
                tableEnvWithMaterializedTable(sessionClusterA(), jobSubmitter);
        tableEnv.getConfig().set(CheckpointingOptions.SAVEPOINT_DIRECTORY, SAVEPOINT_DIRECTORY);
        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.ACTIVATED,
                new ContinuousRefreshHandler(APPLICATION_TARGET, "app-1", jobSubmitter.jobId));

        assertThatThrownBy(() -> tableEnv.executeSql(ALTER_AS_QUERY))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("own application cluster")
                .hasMessageContaining("SQL Gateway");
        assertThatThrownBy(() -> tableEnv.executeSql(REDEPLOY))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("own application cluster");
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.ACTIVATED);

        recordRefreshHandler(
                catalog,
                TABLE_PATH,
                RefreshStatus.SUSPENDED,
                new ContinuousRefreshHandler(
                        APPLICATION_TARGET, "app-1", jobSubmitter.jobId, RESTORE_PATH));

        assertThatThrownBy(() -> tableEnv.executeSql(RESUME))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("SQL Gateway");

        assertThat(jobSubmitter.targets).hasSize(1);
        assertThat(getMaterializedTable(catalog, TABLE_PATH).getRefreshStatus())
                .isEqualTo(RefreshStatus.SUSPENDED);
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
        tableEnv.executeSql(
                "CREATE TABLE datagen_source (k INT, v BIGINT) WITH ('connector' = 'datagen')");
        tableEnv.executeSql(
                "CREATE MATERIALIZED TABLE "
                        + MATERIALIZED_TABLE
                        + "\n FRESHNESS = INTERVAL '30' SECOND"
                        + "\n REFRESH_MODE = CONTINUOUS"
                        + "\n AS SELECT k, v FROM datagen_source");
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

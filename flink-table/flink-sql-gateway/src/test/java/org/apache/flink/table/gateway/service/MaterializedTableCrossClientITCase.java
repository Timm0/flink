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

package org.apache.flink.table.gateway.service;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.gateway.AbstractMaterializedTableStatementITCase;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.DeployScriptITCase;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.table.gateway.service.utils.SqlGatewayServiceTestUtil.awaitOperationTermination;
import static org.apache.flink.test.util.TestUtils.waitUntilAllTasksAreRunning;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for materialized tables declared through the SQL Gateway or a plain {@link
 * TableEnvironment} and operated through the other, or through a second plain {@link
 * TableEnvironment}.
 */
class MaterializedTableCrossClientITCase extends AbstractMaterializedTableStatementITCase {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration PAUSE = Duration.ofMillis(200);

    private static final String USERS_SHOPS = "users_shops";
    private static final String APPLICATION_MT = "app_mode_mt";
    private static final String APPLICATION_TARGET = "kubernetes-application";

    private static final String CONTINUOUS_QUERY =
            "SELECT user_id, shop_id, COUNT(*) AS cnt FROM datagenSource GROUP BY user_id, shop_id";
    private static final String CHANGED_CONTINUOUS_QUERY =
            "SELECT user_id, shop_id, COUNT(*) AS cnt, MAX(payment_amount_cents) AS max_fee"
                    + " FROM datagenSource GROUP BY user_id, shop_id";
    private static final String APPLICATION_QUERY =
            "SELECT user_id, shop_id, COUNT(*) AS cnt FROM my_source GROUP BY user_id, shop_id";
    private static final String CHANGED_APPLICATION_QUERY =
            "SELECT user_id, shop_id, COUNT(*) AS cnt, MAX(order_id) AS max_order_id"
                    + " FROM my_source GROUP BY user_id, shop_id";
    private static final String CHANGED_FULL_QUERY =
            "SELECT user_id, shop_id, ds, COUNT(order_id) AS order_cnt,"
                    + " SUM(order_amount) AS order_amount_sum"
                    + " FROM (SELECT user_id, shop_id, order_created_at AS ds, order_id,"
                    + " 1 AS order_amount FROM my_source) AS tmp"
                    + " GROUP BY (user_id, shop_id, ds)";
    private static final String NO_WORKFLOW_SCHEDULER = "Only the SQL Gateway provides one";

    @TempDir private Path savepointDirectory;

    @BeforeEach
    void unsetTestContextEnvironment() {
        TestStreamEnvironment.unsetAsContext();
    }

    @AfterEach
    void cleanUp() throws Exception {
        try {
            dropMaterializedTable(identifier(USERS_SHOPS));
        } finally {
            DeployScriptITCase.TestApplicationClusterClientFactory.id = null;
            for (final JobStatusMessage job :
                    restClusterClient.listJobs().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                if (!job.getJobState().isTerminalState()) {
                    cancelUnlessTerminated(job.getJobId());
                }
            }
        }
    }

    @Test
    void gatewayTableIsOperatedFromAProgram() throws Exception {
        executeInGateway(ddl("CREATE", USERS_SHOPS, CONTINUOUS_QUERY));
        final JobID gatewayJobId = refreshJobId(USERS_SHOPS);
        waitUntilAllTasksAreRunning(restClusterClient, gatewayJobId);
        final TableEnvironment program = createProgram();

        final CatalogMaterializedTable inspected =
                (CatalogMaterializedTable)
                        program.getCatalog(fileSystemCatalogName)
                                .orElseThrow()
                                .getTable(new ObjectPath(TEST_DEFAULT_DATABASE, USERS_SHOPS));
        assertThat(inspected.getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(
                        ContinuousRefreshHandlerSerializer.INSTANCE
                                .deserialize(
                                        inspected.getSerializedRefreshHandler(),
                                        getClass().getClassLoader())
                                .getJobId())
                .isEqualTo(gatewayJobId.toHexString());

        program.executeSql("ALTER MATERIALIZED TABLE users_shops AS " + CHANGED_CONTINUOUS_QUERY);
        assertThat(getTable(identifier(USERS_SHOPS)).getExpandedQuery()).contains("max_fee");
        final JobID alteredJobId = refreshJobId(USERS_SHOPS);
        awaitJobStatus(gatewayJobId, JobStatus.FINISHED);
        waitUntilAllTasksAreRunning(restClusterClient, alteredJobId);

        program.executeSql(ddl("CREATE OR ALTER", USERS_SHOPS, CHANGED_CONTINUOUS_QUERY));
        final JobID redeployedJobId = refreshJobId(USERS_SHOPS);
        assertThat(redeployedJobId).isNotEqualTo(alteredJobId);
        awaitJobStatus(alteredJobId, JobStatus.FINISHED);
        waitUntilAllTasksAreRunning(restClusterClient, redeployedJobId);

        program.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND");
        assertThat(getTable(identifier(USERS_SHOPS)).getRefreshStatus())
                .isSameAs(RefreshStatus.SUSPENDED);
        assertThat(refreshHandler(USERS_SHOPS).getRestorePath()).isPresent();
        awaitJobStatus(redeployedJobId, JobStatus.FINISHED);

        program.executeSql("ALTER MATERIALIZED TABLE users_shops RESUME");
        assertThat(getTable(identifier(USERS_SHOPS)).getRefreshStatus())
                .isSameAs(RefreshStatus.ACTIVATED);
        final JobID resumedJobId = refreshJobId(USERS_SHOPS);
        waitUntilAllTasksAreRunning(restClusterClient, resumedJobId);

        program.executeSql("DROP MATERIALIZED TABLE users_shops");
        assertThat(tableExists(program, USERS_SHOPS)).isFalse();
        awaitJobStatus(resumedJobId, JobStatus.CANCELED);
    }

    @Test
    void gatewayTableIsRefreshedFromAProgram() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                USERS_SHOPS,
                List.of(Row.of(1L, 1L, 1L, "2024-01-01"), Row.of(2L, 2L, 2L, "2024-01-02")),
                Map.of("ds", "yyyy-MM-dd"),
                RefreshMode.CONTINUOUS);
        final long refreshStart = System.currentTimeMillis();

        final String refreshJobId =
                refreshJobIdOf(
                        createProgram()
                                .executeSql(
                                        "ALTER MATERIALIZED TABLE users_shops"
                                                + " REFRESH PARTITION (ds = '2024-01-02')"));

        verifyRefreshJobCreated(restClusterClient, refreshJobId, refreshStart);
    }

    @Test
    void programTableIsSuspendedResumedAndDroppedThroughTheGateway() throws Exception {
        createProgram().executeSql(ddl("CREATE", USERS_SHOPS, CONTINUOUS_QUERY));
        final JobID programJobId = refreshJobId(USERS_SHOPS);
        waitUntilAllTasksAreRunning(restClusterClient, programJobId);
        executeInGateway(
                "SET 'execution.checkpointing.savepoint-dir' = '"
                        + savepointDirectory.toUri()
                        + "'");

        executeInGateway("ALTER MATERIALIZED TABLE users_shops SUSPEND");
        assertThat(getTable(identifier(USERS_SHOPS)).getRefreshStatus())
                .isSameAs(RefreshStatus.SUSPENDED);
        awaitJobStatus(programJobId, JobStatus.FINISHED);

        executeInGateway("ALTER MATERIALIZED TABLE users_shops RESUME");
        final JobID resumedJobId = refreshJobId(USERS_SHOPS);
        assertThat(resumedJobId).isNotEqualTo(programJobId);
        waitUntilAllTasksAreRunning(restClusterClient, resumedJobId);

        executeInGateway("DROP MATERIALIZED TABLE users_shops");
        awaitJobStatus(resumedJobId, JobStatus.CANCELED);
        assertThatThrownBy(() -> getTable(identifier(USERS_SHOPS)))
                .isInstanceOf(SqlGatewayException.class);
    }

    @Test
    void tableDeclaredByOneProgramIsOperatedByAnotherWithItsOwnCatalogInstance() throws Exception {
        createProgram().executeSql(ddl("CREATE", USERS_SHOPS, CONTINUOUS_QUERY));
        final JobID declaredJobId = refreshJobId(USERS_SHOPS);
        waitUntilAllTasksAreRunning(restClusterClient, declaredJobId);
        final TableEnvironment otherProgram = createProgram();

        otherProgram.executeSql(ddl("CREATE OR ALTER", USERS_SHOPS, CONTINUOUS_QUERY));
        final JobID unchangedRedeployJobId = refreshJobId(USERS_SHOPS);
        assertThat(unchangedRedeployJobId).isNotEqualTo(declaredJobId);
        awaitJobStatus(declaredJobId, JobStatus.FINISHED);
        waitUntilAllTasksAreRunning(restClusterClient, unchangedRedeployJobId);

        otherProgram.executeSql(ddl("CREATE OR ALTER", USERS_SHOPS, CHANGED_CONTINUOUS_QUERY));
        assertThat(getTable(identifier(USERS_SHOPS)).getExpandedQuery()).contains("max_fee");
        final JobID changedRedeployJobId = refreshJobId(USERS_SHOPS);
        awaitJobStatus(unchangedRedeployJobId, JobStatus.FINISHED);
        waitUntilAllTasksAreRunning(restClusterClient, changedRedeployJobId);

        otherProgram.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND");
        assertThat(getTable(identifier(USERS_SHOPS)).getRefreshStatus())
                .isSameAs(RefreshStatus.SUSPENDED);
        awaitJobStatus(changedRedeployJobId, JobStatus.FINISHED);

        otherProgram.executeSql("ALTER MATERIALIZED TABLE users_shops RESUME");
        final JobID resumedJobId = refreshJobId(USERS_SHOPS);
        waitUntilAllTasksAreRunning(restClusterClient, resumedJobId);

        otherProgram.executeSql("DROP MATERIALIZED TABLE users_shops");
        assertThat(tableExists(otherProgram, USERS_SHOPS)).isFalse();
        awaitJobStatus(resumedJobId, JobStatus.CANCELED);
    }

    @Test
    void gatewayFullModeTableKeepsItsWorkflowWhenAProgramCannotOperateIt() throws Exception {
        createAndVerifyCreateMaterializedTableWithData(
                USERS_SHOPS, List.of(), Map.of(), RefreshMode.FULL);
        final ObjectIdentifier identifier = identifier(USERS_SHOPS);
        final byte[] workflowHandler = getTable(identifier).getSerializedRefreshHandler();
        final TableEnvironment program = createProgram();

        assertThatThrownBy(() -> program.executeSql("ALTER MATERIALIZED TABLE users_shops SUSPEND"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(NO_WORKFLOW_SCHEDULER);
        assertThatThrownBy(() -> program.executeSql("DROP MATERIALIZED TABLE users_shops"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(NO_WORKFLOW_SCHEDULER);
        assertThat(getTable(identifier).getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(workflowTriggerState(identifier)).isEqualTo(Trigger.TriggerState.NORMAL);

        program.executeSql("ALTER MATERIALIZED TABLE users_shops AS " + CHANGED_FULL_QUERY);
        assertThat(getTable(identifier).getExpandedQuery()).contains("order_amount_sum");
        assertThat(getTable(identifier).getSerializedRefreshHandler()).isEqualTo(workflowHandler);

        final long refreshStart = System.currentTimeMillis();
        final String refreshJobId =
                refreshJobIdOf(
                        program.executeSql(
                                "ALTER MATERIALIZED TABLE users_shops"
                                        + " REFRESH PARTITION (ds = '2024-01-02')"));
        verifyRefreshJobCreated(restClusterClient, refreshJobId, refreshStart);
        assertThat(workflowTriggerState(identifier)).isEqualTo(Trigger.TriggerState.NORMAL);

        executeInGateway("ALTER MATERIALIZED TABLE users_shops SUSPEND");
        assertThatThrownBy(() -> program.executeSql("ALTER MATERIALIZED TABLE users_shops RESUME"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(NO_WORKFLOW_SCHEDULER);
        assertThat(getTable(identifier).getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        assertThat(workflowTriggerState(identifier)).isEqualTo(Trigger.TriggerState.PAUSED);
    }

    @Test
    void gatewayApplicationClusterTableIsOnlyPartlyOperableFromAProgram() throws Exception {
        final ObjectIdentifier identifier = createApplicationClusterTableInGateway();
        final ContinuousRefreshHandler deployedHandler = refreshHandler(APPLICATION_MT);
        final TableEnvironment program = createProgram();

        // a restart would deploy an application cluster: rejected before the job is stopped
        for (final String restart :
                List.of(
                        "ALTER MATERIALIZED TABLE app_mode_mt AS " + CHANGED_APPLICATION_QUERY,
                        ddl("CREATE OR ALTER", APPLICATION_MT, APPLICATION_QUERY))) {
            assertThatThrownBy(() -> program.executeSql(restart))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("own application cluster")
                    .hasMessageContaining("SQL Gateway");
        }
        assertThat(DeployScriptITCase.TestApplicationClusterDescriptor.stoppedJobId).isNull();
        assertThat(getTable(identifier).getRefreshStatus()).isSameAs(RefreshStatus.ACTIVATED);
        assertThat(refreshHandler(APPLICATION_MT))
                .usingRecursiveComparison()
                .isEqualTo(deployedHandler);

        // a one-off refresh runs on the program's own target
        final long refreshStart = System.currentTimeMillis();
        final String refreshJobId =
                refreshJobIdOf(program.executeSql("ALTER MATERIALIZED TABLE app_mode_mt REFRESH"));
        verifyRefreshJobCreated(restClusterClient, refreshJobId, refreshStart);

        program.executeSql("ALTER MATERIALIZED TABLE app_mode_mt SUSPEND");
        assertThat(DeployScriptITCase.TestApplicationClusterDescriptor.stoppedJobId)
                .isEqualTo(JobID.fromHexString(deployedHandler.getJobId()));
        assertThat(getTable(identifier).getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        assertThat(refreshHandler(APPLICATION_MT).getRestorePath())
                .hasValue(DeployScriptITCase.TestApplicationClusterDescriptor.SAVEPOINT_PATH);

        assertThatThrownBy(() -> program.executeSql("ALTER MATERIALIZED TABLE app_mode_mt RESUME"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("own application cluster")
                .hasMessageContaining("SQL Gateway");
        assertThat(getTable(identifier).getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);

        // an alter of a suspended table restarts nothing, so it only changes the catalog
        program.executeSql("ALTER MATERIALIZED TABLE app_mode_mt AS " + CHANGED_APPLICATION_QUERY);
        assertThat(getTable(identifier).getRefreshStatus()).isSameAs(RefreshStatus.SUSPENDED);
        assertThat(getTable(identifier).getExpandedQuery()).contains("max_order_id");
        assertThat(refreshHandler(APPLICATION_MT).getExecutionTarget())
                .isEqualTo(APPLICATION_TARGET);
        assertThat(refreshHandler(APPLICATION_MT).getRestorePath()).isEmpty();

        program.executeSql("DROP MATERIALIZED TABLE app_mode_mt");
        assertThat(tableExists(program, APPLICATION_MT)).isFalse();
        assertThat(DeployScriptITCase.TestApplicationClusterDescriptor.cancelledJobId).isNull();
    }

    @Test
    void gatewayApplicationClusterTableIsDroppedFromAProgram() throws Exception {
        createApplicationClusterTableInGateway();
        final ContinuousRefreshHandler deployedHandler = refreshHandler(APPLICATION_MT);
        final TableEnvironment program = createProgram();

        program.executeSql("DROP MATERIALIZED TABLE app_mode_mt");

        assertThat(DeployScriptITCase.TestApplicationClusterDescriptor.retrievedClusterId)
                .isEqualTo(deployedHandler.getClusterId());
        assertThat(DeployScriptITCase.TestApplicationClusterDescriptor.cancelledJobId)
                .isEqualTo(JobID.fromHexString(deployedHandler.getJobId()));
        assertThat(tableExists(program, APPLICATION_MT)).isFalse();
    }

    /**
     * A program with its own TableEnvironment and its own instance of this test's catalog. The
     * target and the MiniCluster's address are on its TableConfig.
     */
    private TableEnvironment createProgram() {
        final Configuration configuration =
                new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(DeploymentOptions.TARGET, "remote");
        configuration.set(SAVEPOINT_DIRECTORY, savepointDirectory.toUri().toString());
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        final TableEnvironment program =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());
        program.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'test-filesystem', 'path' = '%s',"
                                + " 'default-database' = '%s')",
                        fileSystemCatalogName, fileSystemCatalogPath, TEST_DEFAULT_DATABASE));
        program.useCatalog(fileSystemCatalogName);
        return program;
    }

    private ObjectIdentifier createApplicationClusterTableInGateway() throws Exception {
        DeployScriptITCase.TestApplicationClusterClientFactory.id = APPLICATION_TARGET;
        createBoundedValuesSource(List.of());
        final Configuration applicationModeConfig = new Configuration();
        applicationModeConfig.set(DeploymentOptions.TARGET, APPLICATION_TARGET);
        awaitOperationTermination(
                service,
                sessionHandle,
                service.executeStatement(
                        sessionHandle,
                        ddl("CREATE", APPLICATION_MT, APPLICATION_QUERY),
                        -1,
                        applicationModeConfig));
        DeployScriptITCase.TestApplicationClusterDescriptor.resetRecordedRequests();
        return identifier(APPLICATION_MT);
    }

    private static String ddl(String createClause, String tableName, String query) {
        return createClause
                + " MATERIALIZED TABLE "
                + tableName
                + "\n WITH ('format' = 'debezium-json')\n"
                + " FRESHNESS = INTERVAL '30' SECOND\n"
                + " AS "
                + query;
    }

    private void executeInGateway(String statement) throws Exception {
        awaitOperationTermination(
                service,
                sessionHandle,
                MaterializedTableTestUtils.executeStatement(service, sessionHandle, statement));
    }

    private ObjectIdentifier identifier(String tableName) {
        return ObjectIdentifier.of(fileSystemCatalogName, TEST_DEFAULT_DATABASE, tableName);
    }

    private ResolvedCatalogMaterializedTable getTable(ObjectIdentifier identifier) {
        return MaterializedTableTestUtils.getTable(service, sessionHandle, identifier);
    }

    private ContinuousRefreshHandler refreshHandler(String tableName) throws Exception {
        return MaterializedTableTestUtils.getContinuousRefreshHandler(
                getTable(identifier(tableName)), getClass().getClassLoader());
    }

    private JobID refreshJobId(String tableName) throws Exception {
        return JobID.fromHexString(refreshHandler(tableName).getJobId());
    }

    private boolean tableExists(TableEnvironment program, String tableName) {
        return program.getCatalog(fileSystemCatalogName)
                .orElseThrow()
                .tableExists(new ObjectPath(TEST_DEFAULT_DATABASE, tableName));
    }

    private static String refreshJobIdOf(TableResult refreshResult) throws Exception {
        try (CloseableIterator<Row> rows = refreshResult.collect()) {
            return (String) rows.next().getField(0);
        }
    }

    private Trigger.TriggerState workflowTriggerState(ObjectIdentifier identifier)
            throws Exception {
        final String workflowName = "quartz_job_" + identifier.asSerializableString();
        return SQL_GATEWAY_REST_ENDPOINT_EXTENSION
                .getSqlGatewayRestEndpoint()
                .getQuartzScheduler()
                .getQuartzScheduler()
                .getTriggerState(TriggerKey.triggerKey(workflowName, "default_group"));
    }

    private void cancelUnlessTerminated(JobID jobId) throws Exception {
        try {
            restClusterClient.cancel(jobId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            final JobStatus jobStatus =
                    restClusterClient
                            .getJobStatus(jobId)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!jobStatus.isTerminalState()) {
                throw e;
            }
        }
    }

    private void awaitJobStatus(JobID jobId, JobStatus expectedStatus) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return restClusterClient
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
}

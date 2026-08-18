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

package org.apache.flink.table.planner.materializedtable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.CreateMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.RefreshJob;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for materialized tables declared and operated purely through the Java Table API.
 *
 * <p>No SQL Gateway anywhere: a plain {@link TableEnvironment} declares the table, submits its
 * refresh job to a real cluster, and drives the whole lifecycle. That is the point of the test —
 * before this, declaring a materialized table on a plain {@link TableEnvironment} reported success
 * while starting no refresh job at all, and dropping one left its job running.
 *
 * <p>Two deliberate departures from a production setup, both properties of the test fixture rather
 * than of the feature:
 *
 * <ul>
 *   <li>Storage options are given explicitly on each declaration, because {@link
 *       GenericInMemoryCatalog} supplies none. A catalog that manages its own storage fills them
 *       in, which is what makes {@code materializeAs(path, freshness)} enough on its own.
 *   <li>The catalog instance is shared between environments rather than reloaded from disk. What
 *       has to be proven about a second environment is that it recovers a job client from the
 *       table's persisted refresh handler, having never submitted the job itself — catalog
 *       persistence is a separate concern this change does not touch.
 * </ul>
 */
class TableApiMaterializedTableITCase {

    private static final AtomicLong COUNTER = new AtomicLong();

    private static final String CATALOG = "mt_catalog";
    private static final String DATABASE = "mt_db";

    @RegisterExtension
    @Order(1)
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .build());

    @TempDir private static Path temporaryFolder;

    private Path warehouse;
    private Catalog sharedCatalog;
    private String dataId;

    @BeforeEach
    void before() throws Exception {
        // MiniClusterExtension installs itself as the context execution environment, which pins
        // execution.target to 'minicluster'. A materialized table needs a target that outlives the
        // statement and can be re-attached to by identifier, so step out of the context environment
        // and submit over the cluster's REST endpoint instead. The extension restores it
        // afterwards.
        TestStreamEnvironment.unsetAsContext();

        warehouse =
                Files.createDirectories(temporaryFolder.resolve("wh_" + COUNTER.incrementAndGet()));
        sharedCatalog = new GenericInMemoryCatalog(CATALOG, DATABASE);
        dataId =
                TestValuesTableFactory.registerData(
                        List.of(
                                Row.of("2026-08-10", "EMEA", 100L),
                                Row.of("2026-08-10", "APAC", 200L),
                                Row.of("2026-08-11", "EMEA", 300L)));

        registerSourceTables(newTableEnvironment());
    }

    @Test
    void testMaterializedTableLifecycleFromTableApi(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        final TableEnvironment tEnv = newTableEnvironment();

        // -----------------------------------------------------------------------------------------
        // CONTINUOUS: declaring must start a refresh job, not merely write a catalog entry
        // -----------------------------------------------------------------------------------------
        final MaterializedTable silver =
                tEnv.from("events")
                        .where($("amount").isGreater(0))
                        .select($("ds"), $("region"), $("amount"))
                        .materializeAs("silver", storedAt("silver", Duration.ofMinutes(1)).build())
                        .execute();

        assertThat(silver.info().getRefreshMode()).isEqualTo(RefreshMode.CONTINUOUS);
        assertThat(silver.info().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(silver.info().getExpandedQuery()).contains("events");

        final RefreshJob declaredJob =
                silver.getRefreshJob()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Declaring a continuous materialized table must start a refresh job."));
        awaitStatus(declaredJob, JobStatus.RUNNING);

        // -----------------------------------------------------------------------------------------
        // A second, independent environment reaches the same job through the catalog alone
        // -----------------------------------------------------------------------------------------
        final MaterializedTable reattached = newTableEnvironment().materializedTable("silver");
        assertThat(reattached.info().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        assertThat(reattached.getRefreshJob()).isPresent();
        assertThat(reattached.getRefreshJob().get().getJobId()).isEqualTo(declaredJob.getJobId());
        awaitStatus(reattached.getRefreshJob().get(), JobStatus.RUNNING);

        // -----------------------------------------------------------------------------------------
        // suspend / resume, driven from the re-attached handle
        // -----------------------------------------------------------------------------------------
        // Suspend stops the job with a savepoint, which cannot be taken while tasks are still being
        // deployed — a RUNNING job is not yet a job whose tasks all run.
        awaitTasksRunning(miniCluster, declaredJob);
        reattached.suspend();
        assertThat(reattached.info().getRefreshStatus()).isEqualTo(RefreshStatus.SUSPENDED);
        assertThat(reattached.getRefreshJob()).isEmpty();

        // Suspending twice is an error unless the caller says it does not care.
        assertThatThrownBy(reattached::suspend).isInstanceOf(Exception.class);
        reattached.suspend(true);

        reattached.resume();
        assertThat(reattached.info().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        final RefreshJob resumedJob =
                reattached
                        .getRefreshJob()
                        .orElseThrow(() -> new AssertionError("Resume must start a new job."));
        assertThat(resumedJob.getJobId()).isNotEqualTo(declaredJob.getJobId());
        awaitStatus(resumedJob, JobStatus.RUNNING);

        // -----------------------------------------------------------------------------------------
        // FULL: a scheduled table, refreshed on demand, whose data is readable afterwards
        // -----------------------------------------------------------------------------------------
        final MaterializedTable daily =
                tEnv.from("events_bounded")
                        .groupBy($("ds"))
                        .select($("ds"), $("amount").sum().as("revenue"))
                        .materializeAs(
                                "daily",
                                storedAt("daily", Duration.ofHours(1))
                                        .refreshMode(LogicalRefreshMode.FULL)
                                        .build())
                        .execute();

        assertThat(daily.info().getRefreshMode()).isEqualTo(RefreshMode.FULL);
        assertThat(daily.info().getRefreshStatus()).isEqualTo(RefreshStatus.ACTIVATED);
        // A full-mode table is refreshed by a schedule, so there is no continuously running job.
        assertThat(daily.getRefreshJob()).isEmpty();

        daily.refresh().await();
        assertThat(CollectionUtil.iteratorToList(daily.asTable().execute().collect()))
                .containsExactlyInAnyOrder(Row.of("2026-08-10", 300L), Row.of("2026-08-11", 300L));

        // -----------------------------------------------------------------------------------------
        // CREATE_OR_FAIL refuses to touch what is already at the path
        // -----------------------------------------------------------------------------------------
        assertThatThrownBy(
                        () ->
                                tEnv.from("events_bounded")
                                        .groupBy($("ds"))
                                        .select($("ds"), $("amount").sum().as("revenue"))
                                        .materializeAs(
                                                "daily",
                                                storedAt("daily", Duration.ofHours(1)).build())
                                        .execute(CreateMode.CREATE_OR_FAIL))
                .isInstanceOf(ValidationException.class);

        // -----------------------------------------------------------------------------------------
        // drop takes the running refresh job with it
        // -----------------------------------------------------------------------------------------
        reattached.drop(true);
        daily.drop(true);
        assertThat(tEnv.listMaterializedTables(CATALOG, DATABASE)).isEmpty();
        awaitGone(resumedJob);
    }

    // -----------------------------------------------------------------------------------------

    /** A descriptor whose storage is a file directory under this test's warehouse. */
    private MaterializedTableDescriptor.Builder storedAt(String name, Duration freshness) {
        return MaterializedTableDescriptor.ofFreshness(freshness)
                .option("connector", "filesystem")
                .option("path", warehouse.resolve(name).toString())
                .option("format", "testcsv");
    }

    /**
     * Builds an environment that submits to the test cluster.
     *
     * <p>{@code remote} rather than {@code local}: a refresh job outlives the statement that
     * declared it, which a mini-cluster tied to that statement cannot express.
     */
    private TableEnvironment newTableEnvironment() {
        final Configuration configuration =
                new Configuration(MINI_CLUSTER.getClientConfiguration());
        configuration.set(DeploymentOptions.TARGET, "remote");
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        // Suspend stops the refresh job with a savepoint, which needs somewhere to write it.
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY,
                warehouse.resolve("savepoints").toUri().toString());
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                warehouse.resolve("checkpoints").toUri().toString());
        configuration.setString(WORKFLOW_SCHEDULER_TYPE.key(), "in-process");

        final TableEnvironment tableEnvironment =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance()
                                .inStreamingMode()
                                .withConfiguration(configuration)
                                .build());
        tableEnvironment.registerCatalog(CATALOG, sharedCatalog);
        tableEnvironment.useCatalog(CATALOG);
        tableEnvironment.useDatabase(DATABASE);
        return tableEnvironment;
    }

    /**
     * Registers the sources in the shared catalog, so every environment built later sees them: a
     * resume in another environment has to resolve the query it inherited.
     */
    private void registerSourceTables(TableEnvironment tableEnvironment) {
        final Schema schema =
                Schema.newBuilder()
                        .column("ds", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT())
                        .build();

        tableEnvironment.createTable(
                "events",
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .option("data-id", dataId)
                        .option("bounded", "false")
                        // Keep the source alive once the rows are gone: a continuous refresh job
                        // that
                        // finishes on its own cannot be suspended. Only the NewSource runtime of
                        // the
                        // values connector can stay open like that.
                        .option("terminating", "false")
                        .option("runtime-source", "NewSource")
                        .build(),
                true);

        tableEnvironment.createTable(
                "events_bounded",
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .option("data-id", dataId)
                        .option("bounded", "true")
                        .build(),
                true);
    }

    private static void awaitTasksRunning(MiniCluster miniCluster, RefreshJob job)
            throws Exception {
        waitForAllTaskRunning(miniCluster, JobID.fromHexString(job.getJobId()), false);
    }

    private static void awaitStatus(RefreshJob job, JobStatus expected) throws Exception {
        CommonTestUtils.waitUtil(
                () -> job.getStatus() == expected,
                Duration.ofSeconds(60),
                Duration.ofMillis(200),
                String.format("Refresh job %s did not reach %s.", job.getJobId(), expected));
    }

    /** A dropped table's job must stop; once the cluster forgets it, it is gone for good. */
    private static void awaitGone(RefreshJob job) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return job.getStatus().isTerminalState();
                    } catch (Exception e) {
                        return true;
                    }
                },
                Duration.ofSeconds(60),
                Duration.ofMillis(200),
                String.format(
                        "Dropping the materialized table left refresh job %s running.",
                        job.getJobId()));
    }
}

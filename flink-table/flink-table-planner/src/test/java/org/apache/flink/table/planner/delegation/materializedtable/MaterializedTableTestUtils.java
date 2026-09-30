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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.delegation.materializedtable.RefreshWorkflowContext;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.utils.CatalogManagerMocks;

import javax.annotation.Nullable;

import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/** Test support for materialized tables: table environments, catalog access and a job submitter. */
public final class MaterializedTableTestUtils {

    private MaterializedTableTestUtils() {}

    /** Builds a table environment by hand to inject {@code jobSubmitter}. */
    static TableEnvironment createTableEnvironment(
            Configuration rootConfiguration,
            String catalogName,
            Catalog catalog,
            MaterializedTableJobSubmitter jobSubmitter) {
        final StreamExecutionEnvironment executionEnvironment =
                new StreamExecutionEnvironment(rootConfiguration);
        final ResourceManager resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0],
                        MaterializedTableTestUtils.class.getClassLoader(),
                        rootConfiguration);
        final ClassLoader userClassLoader = resourceManager.getUserClassLoader();
        final Executor executor =
                AbstractStreamTableEnvironmentImpl.lookupExecutor(
                        userClassLoader, executionEnvironment);

        final TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());

        final ModuleManager moduleManager = new ModuleManager();
        final CatalogManager catalogManager =
                CatalogManagerMocks.preparedCatalogManager()
                        .classLoader(userClassLoader)
                        .config(tableConfig)
                        .defaultCatalog(catalogName, catalog)
                        .executionConfig(executionEnvironment.getConfig())
                        .build();
        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);
        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                functionCatalog,
                tableConfig,
                executionEnvironment,
                planner,
                executor,
                true,
                AbstractStreamTableEnvironmentImpl.lookupMaterializedTableExecutorFactory(
                        userClassLoader),
                jobSubmitter);
    }

    public static CatalogMaterializedTable getMaterializedTable(
            Catalog catalog, ObjectPath tablePath) throws Exception {
        return (CatalogMaterializedTable) catalog.getTable(tablePath);
    }

    public static ContinuousRefreshHandler getRefreshHandler(Catalog catalog, ObjectPath tablePath)
            throws Exception {
        return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                getMaterializedTable(catalog, tablePath).getSerializedRefreshHandler(),
                MaterializedTableTestUtils.class.getClassLoader());
    }

    /** Rewrites the table as another program. */
    public static void recordRefreshHandler(
            Catalog catalog,
            ObjectPath tablePath,
            RefreshStatus refreshStatus,
            ContinuousRefreshHandler refreshHandler)
            throws Exception {
        catalog.alterTable(
                tablePath,
                getMaterializedTable(catalog, tablePath)
                        .copy(
                                refreshStatus,
                                refreshHandler.asSummaryString(),
                                ContinuousRefreshHandlerSerializer.INSTANCE.serialize(
                                        refreshHandler)),
                false);
    }

    public static Predicate<CatalogBaseTable> hasRefreshStatus(RefreshStatus refreshStatus) {
        return table ->
                table instanceof CatalogMaterializedTable
                        && ((CatalogMaterializedTable) table).getRefreshStatus() == refreshStatus;
    }

    /** Fails selected catalog writes once, so tests can observe how a rejected write is handled. */
    public static final class FailingCatalog extends GenericInMemoryCatalog {

        @Nullable private Predicate<CatalogBaseTable> failingAlterMatcher;
        private boolean failNextDrop;

        public FailingCatalog(String name, String defaultDatabase) {
            super(name, defaultDatabase);
        }

        /** The next {@code alterTable} whose new table matches fails; the hook then clears. */
        public void failNextAlterTable(Predicate<CatalogBaseTable> newTableMatches) {
            this.failingAlterMatcher = newTableMatches;
        }

        /** The next {@code dropTable} fails; the hook then clears. */
        public void failNextDropTable() {
            this.failNextDrop = true;
        }

        @Override
        public void alterTable(
                ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
                throws TableNotExistException {
            if (failingAlterMatcher != null && failingAlterMatcher.test(newTable)) {
                failingAlterMatcher = null;
                throw new CatalogException("injected alter failure");
            }
            super.alterTable(tablePath, newTable, ignoreIfNotExists);
        }

        @Override
        public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
                throws TableNotExistException {
            if (failNextDrop) {
                failNextDrop = false;
                throw new CatalogException("injected drop failure");
            }
            super.dropTable(tablePath, ignoreIfNotExists);
        }
    }

    /** Records each submission instead of running the refresh job. */
    static final class TestingJobSubmitter implements MaterializedTableJobSubmitter {

        private final boolean supportsApplicationTargets;
        @Nullable private final Deque<JobClient> scriptedJobClients;
        final List<RefreshJobTarget> targets = new ArrayList<>();

        /** The execution configuration of the last submission. */
        @Nullable Configuration executionConfig;

        /** The job id of the last submission. */
        @Nullable String jobId;

        private TestingJobSubmitter(
                boolean supportsApplicationTargets, @Nullable Deque<JobClient> scriptedJobClients) {
            this.supportsApplicationTargets = supportsApplicationTargets;
            this.scriptedJobClients = scriptedJobClients;
        }

        static TestingJobSubmitter recording(boolean supportsApplicationTargets) {
            return new TestingJobSubmitter(supportsApplicationTargets, null);
        }

        static TestingJobSubmitter scripted(JobClient... jobClients) {
            return new TestingJobSubmitter(false, new ArrayDeque<>(Arrays.asList(jobClients)));
        }

        @Override
        public RefreshJobResult submitRefreshJob(
                RefreshJobTarget target, Configuration executionConfig, String insertStatement) {
            targets.add(target);
            this.executionConfig = new Configuration(executionConfig);
            if (scriptedJobClients == null) {
                jobId = new JobID().toHexString();
                return new RefreshJobResult(target, jobId, null);
            }
            final JobClient jobClient = scriptedJobClients.poll();
            if (jobClient == null) {
                throw new IllegalStateException(
                        "Unexpected refresh job submission: " + insertStatement);
            }
            jobId = jobClient.getJobID().toHexString();
            return new RefreshJobResult(target, jobId, jobClient);
        }

        @Override
        public boolean supportsApplicationTargets() {
            return supportsApplicationTargets;
        }

        @Override
        public Optional<RefreshWorkflowContext> getRefreshWorkflowContext() {
            return Optional.empty();
        }
    }
}

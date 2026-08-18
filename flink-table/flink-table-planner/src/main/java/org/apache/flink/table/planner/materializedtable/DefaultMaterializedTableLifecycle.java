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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.StaticResultProvider;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableRefreshOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableResumeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;
import org.apache.flink.table.operations.materializedtable.ConvertTableToMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.DropMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperation;
import org.apache.flink.table.planner.materializedtable.workflow.InProcessRefreshHandler;
import org.apache.flink.table.planner.materializedtable.workflow.InProcessWorkflowScheduler;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.table.runtime.application.SqlDriver;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowScheduler;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID;
import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.SCHEDULE_TIME_DATE_FORMATTER_DEFAULT;
import static org.apache.flink.table.api.internal.TableResultInternal.TABLE_RESULT_OK;
import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;
import static org.apache.flink.table.catalog.IntervalFreshness.convertFreshnessToCron;
import static org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil.WORKFLOW_SCHEDULER_PREFIX;
import static org.apache.flink.table.utils.DateTimeUtils.formatTimestampStringWithOffset;

/**
 * Flink's own {@link MaterializedTableLifecycle}: it submits refresh jobs through the ordinary
 * Table API execution path and controls them through {@link JobClient}.
 *
 * <p>Moved down from the SQL Gateway, which is now one caller among several. The gateway-specific
 * pieces it used to carry are gone: there is no REST endpoint to call back into, and job control no
 * longer goes through gateway operations.
 */
@Internal
public class DefaultMaterializedTableLifecycle implements MaterializedTableLifecycle {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultMaterializedTableLifecycle.class);

    /** Column names of the result returned by a refresh, kept identical to the gateway's. */
    private static final String JOB_ID = "job id";

    private static final String CLUSTER_INFO = "cluster info";

    private final ClassLoader userCodeClassLoader;

    private final @Nullable WorkflowScheduler<? extends RefreshHandler> workflowScheduler;

    /**
     * Clients for refresh jobs this instance submitted, keyed by job id.
     *
     * <p>Needed because a job cannot always be re-attached to from its coordinates alone: in
     * application mode ({@code execution.target=embedded}) there is no cluster to retrieve, only
     * the client handed back at submission.
     */
    private final Map<String, JobClient> submittedJobClients = new ConcurrentHashMap<>();

    public DefaultMaterializedTableLifecycle(
            Configuration configuration, ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = userCodeClassLoader;
        this.workflowScheduler = buildWorkflowScheduler(configuration, userCodeClassLoader);
    }

    private WorkflowScheduler<? extends RefreshHandler> buildWorkflowScheduler(
            Configuration configuration, ClassLoader userCodeClassLoader) {
        return WorkflowSchedulerFactoryUtil.createWorkflowScheduler(
                configuration, userCodeClassLoader);
    }

    @Override
    public void open() throws Exception {
        if (workflowScheduler != null) {
            workflowScheduler.open();
        }
    }

    @Override
    public void close() throws Exception {
        if (workflowScheduler != null) {
            workflowScheduler.close();
        }
    }

    @Override
    public Optional<JobClient> getRefreshJobClient(Context ctx, ObjectIdentifier identifier) {
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(ctx, identifier);
        // only a continuous table has a long-running job; a full-mode one is driven by a workflow
        if (RefreshMode.CONTINUOUS != materializedTable.getRefreshMode()) {
            return Optional.empty();
        }
        byte[] serializedRefreshHandler = materializedTable.getSerializedRefreshHandler();
        if (serializedRefreshHandler == null) {
            return Optional.empty();
        }
        return resolveJobClient(ctx, deserializeContinuousHandler(serializedRefreshHandler));
    }

    @Override
    public TableResultInternal execute(Context ctx, MaterializedTableOperation op) {
        if (op instanceof CreateMaterializedTableOperation) {
            return callCreateMaterializedTableOperation(ctx, (CreateMaterializedTableOperation) op);
        } else if (op instanceof AlterMaterializedTableRefreshOperation) {
            return callAlterMaterializedTableRefreshOperation(
                    ctx, (AlterMaterializedTableRefreshOperation) op);
        } else if (op instanceof AlterMaterializedTableSuspendOperation) {
            return callAlterMaterializedTableSuspend(
                    ctx, (AlterMaterializedTableSuspendOperation) op);
        } else if (op instanceof AlterMaterializedTableResumeOperation) {
            return callAlterMaterializedTableResume(
                    ctx, (AlterMaterializedTableResumeOperation) op);
        } else if (op instanceof DropMaterializedTableOperation) {
            return callDropMaterializedTableOperation(ctx, (DropMaterializedTableOperation) op);
        } else if (op instanceof AlterMaterializedTableChangeOperation) {
            return callAlterMaterializedTableChangeOperation(
                    ctx, (AlterMaterializedTableChangeOperation) op);
        } else if (op instanceof ConvertTableToMaterializedTableOperation) {
            return callConvertTableToMaterializedTableOperation(
                    ctx, (ConvertTableToMaterializedTableOperation) op);
        }

        throw new TableException(
                String.format(
                        "Unsupported Operation %s for materialized table.", op.asSummaryString()));
    }

    private TableResultInternal callCreateMaterializedTableOperation(
            Context ctx, CreateMaterializedTableOperation createMaterializedTableOperation) {
        ResolvedCatalogMaterializedTable materializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();
        if (RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            createMaterializedTableInContinuousMode(ctx, createMaterializedTableOperation);
        } else {
            createMaterializedTableInFullMode(ctx, createMaterializedTableOperation);
        }
        // Just return ok to unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return TABLE_RESULT_OK;
    }

    private void createMaterializedTableInContinuousMode(
            Context ctx, CreateMaterializedTableOperation createMaterializedTableOperation) {
        // create materialized table first
        createMaterializedTableOperation.execute(ctx);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        try {
            executeContinuousRefreshJob(
                    ctx,
                    catalogMaterializedTable,
                    materializedTableIdentifier,
                    Map.of(),
                    Optional.empty());
        } catch (Exception e) {
            // drop materialized table if submitting the Flink streaming job encounters an
            // exception. Thus, weak
            // atomicity is guaranteed
            new DropMaterializedTableOperation(materializedTableIdentifier, true).execute(ctx);
            throw new TableException(
                    String.format(
                            "Failed to submit continuous refresh job for materialized table %s.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private void createMaterializedTableInFullMode(
            Context ctx, CreateMaterializedTableOperation createMaterializedTableOperation) {
        if (workflowScheduler == null) {
            throw new TableException(
                    "The workflow scheduler must be configured when creating materialized table in full refresh mode.");
        }
        // create materialized table first
        createMaterializedTableOperation.execute(ctx);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        try {
            createPeriodicRefreshWorkflow(
                    ctx, materializedTableIdentifier, catalogMaterializedTable);
        } catch (Exception e) {
            // drop materialized table if creating the refresh workflow encounters an exception, so
            // weak atomicity is guaranteed
            new DropMaterializedTableOperation(materializedTableIdentifier, true).execute(ctx);
            throw new TableException(
                    String.format(
                            "Failed to create refresh workflow for materialized table %s.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private TableResultInternal callConvertTableToMaterializedTableOperation(
            Context ctx, ConvertTableToMaterializedTableOperation convertOperation) {
        ResolvedCatalogMaterializedTable materializedTable =
                convertOperation.getMaterializedTable();
        if (RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            convertTableToMaterializedTableInContinuousMode(ctx, convertOperation);
        } else {
            convertTableToMaterializedTableInFullMode(ctx, convertOperation);
        }
        // Just return ok for unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return TABLE_RESULT_OK;
    }

    private void convertTableToMaterializedTableInContinuousMode(
            Context ctx, ConvertTableToMaterializedTableOperation convertOperation) {
        // swap the catalog entry from a regular table to a materialized table first
        convertOperation.execute(ctx);

        ObjectIdentifier materializedTableIdentifier = convertOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                convertOperation.getMaterializedTable();

        try {
            executeContinuousRefreshJob(
                    ctx,
                    catalogMaterializedTable,
                    materializedTableIdentifier,
                    Map.of(),
                    Optional.empty());
        } catch (Exception e) {
            suspendMaterializedTable(ctx, materializedTableIdentifier, catalogMaterializedTable);
            throw new TableException(
                    String.format(
                            "Failed to start the continuous refresh job when converting table %s to a materialized table. "
                                    + "The table was converted and left in SUSPENDED status; resume it once the issue is resolved.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private void convertTableToMaterializedTableInFullMode(
            Context ctx, ConvertTableToMaterializedTableOperation convertOperation) {
        if (workflowScheduler == null) {
            throw new TableException(
                    "The workflow scheduler must be configured when converting a table to a materialized table in full refresh mode.");
        }
        // swap the catalog entry from a regular table to a materialized table first
        convertOperation.execute(ctx);

        ObjectIdentifier materializedTableIdentifier = convertOperation.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                convertOperation.getMaterializedTable();

        try {
            createPeriodicRefreshWorkflow(
                    ctx, materializedTableIdentifier, catalogMaterializedTable);
        } catch (Exception e) {
            suspendMaterializedTable(ctx, materializedTableIdentifier, catalogMaterializedTable);
            throw new TableException(
                    String.format(
                            "Failed to create the refresh workflow when converting table %s to a materialized table. "
                                    + "The table was converted and left in SUSPENDED status; resume it once the issue is resolved.",
                            materializedTableIdentifier),
                    e);
        }
    }

    /**
     * Creates the periodic refresh workflow for a full-mode materialized table and records the
     * resulting handler with {@code ACTIVATED} status.
     */
    private void createPeriodicRefreshWorkflow(
            Context ctx,
            ObjectIdentifier materializedTableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable)
            throws Exception {
        final IntervalFreshness freshness = catalogMaterializedTable.getDefinitionFreshness();
        final String cronExpression = convertFreshnessToCron(freshness);
        final CreateRefreshWorkflow createRefreshWorkflow =
                new CreatePeriodicRefreshWorkflow(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getExpandedQuery(),
                        cronExpression,
                        getSessionInitializationConf(ctx),
                        Map.of(),
                        // no REST endpoint to call back into: the scheduler invokes the
                        // lifecycle directly, in this process
                        null);

        final RefreshHandler refreshHandler =
                workflowScheduler.createRefreshWorkflow(createRefreshWorkflow);

        // An in-process scheduler triggers refreshes by calling straight back into this lifecycle,
        // so it needs an action bound to the session that declared the table. A scheduler that
        // reaches its engine over the network gets everything it needs from the workflow itself.
        if (workflowScheduler instanceof InProcessWorkflowScheduler) {
            ((InProcessWorkflowScheduler) workflowScheduler)
                    .registerTrigger(
                            ((InProcessRefreshHandler) refreshHandler).getWorkflowName(),
                            (scheduleTime, dynamicOptions) ->
                                    refresh(
                                            ctx,
                                            materializedTableIdentifier,
                                            Map.of(),
                                            dynamicOptions,
                                            true,
                                            scheduleTime));
        }

        final RefreshHandlerSerializer refreshHandlerSerializer =
                workflowScheduler.getRefreshHandlerSerializer();
        final byte[] serializedRefreshHandler = refreshHandlerSerializer.serialize(refreshHandler);

        updateRefreshHandler(
                ctx,
                materializedTableIdentifier,
                catalogMaterializedTable,
                RefreshStatus.ACTIVATED,
                refreshHandler.asSummaryString(),
                serializedRefreshHandler);
    }

    /** Sets a materialized table refresh status to {@code SUSPENDED}. */
    private void suspendMaterializedTable(
            Context ctx,
            ObjectIdentifier materializedTableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable) {
        AlterMaterializedTableChangeOperation alterOperation =
                new AlterMaterializedTableChangeOperation(
                        materializedTableIdentifier,
                        oldTable ->
                                List.of(TableChange.modifyRefreshStatus(RefreshStatus.SUSPENDED)),
                        catalogMaterializedTable);
        alterOperation.execute(ctx);
    }

    private TableResultInternal callAlterMaterializedTableSuspend(
            Context ctx, AlterMaterializedTableSuspendOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(ctx, tableIdentifier);

        // Initialization phase doesn't support resume operation.
        if (RefreshStatus.INITIALIZING == materializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s is being initialized and does not support suspend operation.",
                            tableIdentifier));
        }

        if (RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            suspendContinuousRefreshJob(ctx, tableIdentifier, materializedTable);
        } else {
            suspendRefreshWorkflow(ctx, tableIdentifier, materializedTable);
        }
        return TABLE_RESULT_OK;
    }

    private ResolvedCatalogMaterializedTable suspendContinuousRefreshJob(
            Context ctx,
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogMaterializedTable materializedTable) {
        try {
            ContinuousRefreshHandler refreshHandler =
                    deserializeContinuousHandler(materializedTable.getSerializedRefreshHandler());

            if (RefreshStatus.SUSPENDED == materializedTable.getRefreshStatus()) {
                throw new TableException(
                        String.format(
                                "Materialized table %s continuous refresh job has been suspended, jobId is %s.",
                                tableIdentifier, refreshHandler.getJobId()));
            }

            String savepointPath = stopJobWithSavepoint(ctx, refreshHandler);

            ContinuousRefreshHandler updateRefreshHandler =
                    new ContinuousRefreshHandler(
                            refreshHandler.getExecutionTarget(),
                            refreshHandler.getClusterId(),
                            refreshHandler.getJobId(),
                            savepointPath);

            return updateRefreshHandler(
                    ctx,
                    tableIdentifier,
                    materializedTable,
                    RefreshStatus.SUSPENDED,
                    updateRefreshHandler.asSummaryString(),
                    serializeContinuousHandler(updateRefreshHandler));
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to suspend the continuous refresh job for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void suspendRefreshWorkflow(
            Context ctx,
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogMaterializedTable materializedTable) {
        if (RefreshStatus.SUSPENDED == materializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s refresh workflow has been suspended.",
                            tableIdentifier));
        }

        if (workflowScheduler == null) {
            throw new TableException(
                    "The workflow scheduler must be configured when suspending materialized table in full refresh mode.");
        }

        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            materializedTable.getSerializedRefreshHandler(), userCodeClassLoader);
            ModifyRefreshWorkflow modifyRefreshWorkflow =
                    new SuspendRefreshWorkflow(refreshHandler);
            workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflow);

            updateRefreshHandler(
                    ctx,
                    tableIdentifier,
                    materializedTable,
                    RefreshStatus.SUSPENDED,
                    refreshHandler.asSummaryString(),
                    materializedTable.getSerializedRefreshHandler());
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to suspend the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private TableResultInternal callAlterMaterializedTableResume(
            Context ctx, AlterMaterializedTableResumeOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable catalogMaterializedTable =
                getCatalogMaterializedTable(ctx, tableIdentifier);

        // Initialization phase doesn't support resume operation.
        if (RefreshStatus.INITIALIZING == catalogMaterializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s is being initialized and does not support resume operation.",
                            tableIdentifier));
        }

        if (RefreshMode.CONTINUOUS == catalogMaterializedTable.getRefreshMode()) {
            resumeContinuousRefreshJob(
                    ctx, tableIdentifier, catalogMaterializedTable, op.getDynamicOptions());
        } else {
            resumeRefreshWorkflow(
                    ctx, tableIdentifier, catalogMaterializedTable, op.getDynamicOptions());
        }

        return TABLE_RESULT_OK;
    }

    private void resumeContinuousRefreshJob(
            Context ctx,
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            Map<String, String> dynamicOptions) {
        ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(
                        catalogMaterializedTable.getSerializedRefreshHandler());

        // Repeated resume continuous refresh job is not supported
        if (RefreshStatus.ACTIVATED == catalogMaterializedTable.getRefreshStatus()) {
            JobStatus jobStatus = getJobStatus(ctx, refreshHandler);
            if (!jobStatus.isGloballyTerminalState()) {
                throw new TableException(
                        String.format(
                                "Materialized table %s continuous refresh job has been resumed, jobId is %s.",
                                tableIdentifier, refreshHandler.getJobId()));
            }
        }

        Optional<String> restorePath = refreshHandler.getRestorePath();
        try {
            executeContinuousRefreshJob(
                    ctx, catalogMaterializedTable, tableIdentifier, dynamicOptions, restorePath);
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to resume the continuous refresh job for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void resumeRefreshWorkflow(
            Context ctx,
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            Map<String, String> dynamicOptions) {
        // Repeated resume refresh workflow is not supported
        if (RefreshStatus.ACTIVATED == catalogMaterializedTable.getRefreshStatus()) {
            throw new TableException(
                    String.format(
                            "Materialized table %s refresh workflow has been resumed.",
                            tableIdentifier));
        }

        if (workflowScheduler == null) {
            throw new TableException(
                    "The workflow scheduler must be configured when resuming materialized table in full refresh mode.");
        }
        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            catalogMaterializedTable.getSerializedRefreshHandler(),
                            userCodeClassLoader);
            ModifyRefreshWorkflow modifyRefreshWorkflow =
                    new ResumeRefreshWorkflow(refreshHandler, dynamicOptions);
            workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflow);

            updateRefreshHandler(
                    ctx,
                    tableIdentifier,
                    catalogMaterializedTable,
                    RefreshStatus.ACTIVATED,
                    refreshHandler.asSummaryString(),
                    catalogMaterializedTable.getSerializedRefreshHandler());
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to resume the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    private void executeContinuousRefreshJob(
            Context ctx,
            CatalogMaterializedTable catalogMaterializedTable,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> dynamicOptions,
            Optional<String> restorePath) {
        // Set job name, runtime mode, checkpoint interval
        // TODO: Set minibatch related optimization options.
        Configuration customConfig = new Configuration();
        String jobName =
                String.format(
                        "Materialized_table_%s_continuous_refresh_job",
                        materializedTableIdentifier.asSerializableString());
        customConfig.set(NAME, jobName);
        customConfig.set(RUNTIME_MODE, STREAMING);
        restorePath.ifPresent(s -> customConfig.set(SAVEPOINT_PATH, s));

        // Do not override the user-defined checkpoint interval
        if (!ctx.getTableConfig()
                .getConfiguration()
                .contains(CheckpointingOptions.CHECKPOINTING_INTERVAL)) {

            final Duration freshness =
                    validateAndGetIntervalFreshness(catalogMaterializedTable).toDuration();
            customConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, freshness);
        }

        String insertStatement =
                getInsertStatement(
                        materializedTableIdentifier,
                        catalogMaterializedTable.getExpandedQuery(),
                        dynamicOptions);

        JobExecutionResult result = executeRefreshJob(insertStatement, customConfig, ctx);
        ContinuousRefreshHandler continuousRefreshHandler =
                new ContinuousRefreshHandler(
                        result.executionTarget, result.clusterId, result.jobId);
        byte[] serializedBytes = serializeContinuousHandler(continuousRefreshHandler);

        updateRefreshHandler(
                ctx,
                materializedTableIdentifier,
                resolveCatalogMaterializedTable(ctx, catalogMaterializedTable),
                RefreshStatus.ACTIVATED,
                continuousRefreshHandler.asSummaryString(),
                serializedBytes);
    }

    private TableResultInternal callAlterMaterializedTableRefreshOperation(
            Context ctx,
            AlterMaterializedTableRefreshOperation alterMaterializedTableRefreshOperation) {
        ObjectIdentifier materializedTableIdentifier =
                alterMaterializedTableRefreshOperation.getTableIdentifier();

        Map<String, String> partitionSpec =
                alterMaterializedTableRefreshOperation.getPartitionSpec();

        return refresh(ctx, materializedTableIdentifier, partitionSpec, Map.of(), false, null);
    }

    @Override
    public TableResultInternal refresh(
            Context ctx,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> staticPartitions,
            Map<String, String> dynamicOptions,
            boolean isPeriodic,
            @Nullable String scheduleTime) {
        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(ctx, materializedTableIdentifier);
        Map<String, String> refreshPartitions =
                isPeriodic
                        ? getPeriodRefreshPartition(
                                scheduleTime,
                                materializedTable.getDefinitionFreshness(),
                                materializedTableIdentifier,
                                materializedTable.getOptions(),
                                ctx.getTableConfig().getLocalTimeZone())
                        : staticPartitions;

        validatePartitionSpec(refreshPartitions, materializedTable);

        // Set job name, runtime mode
        Configuration customConfig = new Configuration();
        String jobName =
                isPeriodic
                        ? String.format(
                                "Materialized_table_%s_periodic_refresh_job",
                                materializedTableIdentifier.asSerializableString())
                        : String.format(
                                "Materialized_table_%s_one_time_refresh_job",
                                materializedTableIdentifier.asSerializableString());

        customConfig.set(NAME, jobName);
        customConfig.set(RUNTIME_MODE, BATCH);

        String insertStatement =
                getRefreshStatement(
                        materializedTableIdentifier,
                        materializedTable.getExpandedQuery(),
                        refreshPartitions,
                        dynamicOptions);

        try {
            LOG.info(
                    "Starting refresh of the materialized table {}, statement: {}",
                    materializedTableIdentifier,
                    insertStatement);
            JobExecutionResult result = executeRefreshJob(insertStatement, customConfig, ctx);

            Map<String, String> clusterInfo =
                    MaterializedTableLifecycle.clusterInfo(
                            result.executionTarget, result.clusterId);

            // Schema and row shape kept identical to the gateway's, whose REST handlers read them
            // positionally. The default Row->RowData converter rejects MAP, so convert explicitly.
            // The job client rides along for Table API callers that want to await or inspect the
            // job; the gateway builds its ResultFetcher with isQueryResult=false and ignores it.
            return TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                    .jobClient(submittedJobClients.get(result.jobId))
                    .schema(
                            ResolvedSchema.of(
                                    Column.physical(JOB_ID, DataTypes.STRING()),
                                    Column.physical(
                                            CLUSTER_INFO,
                                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))))
                    .resultProvider(
                            new StaticResultProvider(
                                    Collections.singletonList(Row.of(result.jobId, clusterInfo)),
                                    DefaultMaterializedTableLifecycle::toRefreshResultRow))
                    .build();
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to refresh the materialized table %s.",
                            materializedTableIdentifier),
                    e);
        }
    }

    @VisibleForTesting
    static Map<String, String> getPeriodRefreshPartition(
            String scheduleTime,
            IntervalFreshness freshness,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> materializedTableOptions,
            ZoneId localZoneId) {
        if (scheduleTime == null) {
            throw new ValidationException(
                    String.format(
                            "The scheduler time must not be null during the periodic refresh of the materialized table %s.",
                            materializedTableIdentifier));
        }

        Set<String> partitionFields =
                materializedTableOptions.keySet().stream()
                        .filter(k -> k.startsWith(PARTITION_FIELDS))
                        .collect(Collectors.toSet());
        Map<String, String> refreshPartitions = new HashMap<>();
        for (String partKey : partitionFields) {
            String partField =
                    partKey.substring(
                            PARTITION_FIELDS.length() + 1,
                            partKey.length() - (DATE_FORMATTER.length() + 1));
            String partFieldFormatter = materializedTableOptions.get(partKey);

            String partFiledValue =
                    formatTimestampStringWithOffset(
                            scheduleTime,
                            SCHEDULE_TIME_DATE_FORMATTER_DEFAULT,
                            partFieldFormatter,
                            TimeZone.getTimeZone(localZoneId),
                            -freshness.toDuration().toMillis());
            if (partFiledValue == null) {
                throw new TableException(
                        String.format(
                                "Failed to parse a valid partition value for the field '%s' in materialized table %s using the scheduler time '%s' based on the date format '%s'.",
                                partField,
                                materializedTableIdentifier.asSerializableString(),
                                scheduleTime,
                                SCHEDULE_TIME_DATE_FORMATTER_DEFAULT));
            }
            refreshPartitions.put(partField, partFiledValue);
        }

        return refreshPartitions;
    }

    private void validatePartitionSpec(
            Map<String, String> partitionSpec, ResolvedCatalogMaterializedTable table) {
        ResolvedSchema schema = table.getResolvedSchema();
        Set<String> allPartitionKeys = new HashSet<>(table.getPartitionKeys());

        Set<String> unknownPartitionKeys = new HashSet<>();
        Set<String> nonStringPartitionKeys = new HashSet<>();

        for (String partitionKey : partitionSpec.keySet()) {
            if (!schema.getColumn(partitionKey).isPresent()) {
                unknownPartitionKeys.add(partitionKey);
                continue;
            }

            if (!schema.getColumn(partitionKey)
                    .get()
                    .getDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                nonStringPartitionKeys.add(partitionKey);
            }
        }

        if (!unknownPartitionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "The partition spec contains unknown partition keys:\n\n%s\n\nAll known partition keys are:\n\n%s",
                            String.join("\n", unknownPartitionKeys),
                            String.join("\n", allPartitionKeys)));
        }

        if (!nonStringPartitionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Currently, refreshing materialized table only supports referring to char, varchar and string type"
                                    + " partition keys. All specified partition keys in partition specs with unsupported types are:\n\n%s",
                            String.join("\n", nonStringPartitionKeys)));
        }
    }

    @VisibleForTesting
    protected static String getRefreshStatement(
            ObjectIdentifier tableIdentifier,
            String definitionQuery,
            Map<String, String> partitionSpec,
            Map<String, String> dynamicOptions) {
        String tableIdentifierWithDynamicOptions =
                generateTableWithDynamicOptions(tableIdentifier, dynamicOptions);
        StringBuilder insertStatement =
                new StringBuilder(
                        String.format(
                                "INSERT OVERWRITE %s\n  SELECT * FROM (%s)",
                                tableIdentifierWithDynamicOptions, definitionQuery));
        if (!partitionSpec.isEmpty()) {
            insertStatement.append("\n  WHERE ");
            insertStatement.append(
                    partitionSpec.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "%s = '%s'", entry.getKey(), entry.getValue()))
                            .reduce((s1, s2) -> s1 + " AND " + s2)
                            .get());
        }

        return insertStatement.toString();
    }

    private TableResultInternal callAlterMaterializedTableChangeOperation(
            Context ctx, AlterMaterializedTableChangeOperation op) {
        ObjectIdentifier tableIdentifier = op.getTableIdentifier();
        ResolvedCatalogMaterializedTable oldMaterializedTable =
                getCatalogMaterializedTable(ctx, tableIdentifier);

        if (RefreshMode.FULL == oldMaterializedTable.getRefreshMode()) {
            // directly apply the alter operation
            return op.copyAsTableChangeOperation().execute(ctx);
        }

        if (RefreshStatus.ACTIVATED == oldMaterializedTable.getRefreshStatus()) {
            // 1. suspend the materialized table
            ResolvedCatalogMaterializedTable suspendMaterializedTable =
                    suspendContinuousRefreshJob(ctx, tableIdentifier, oldMaterializedTable);

            // 2. alter materialized table schema & query definition
            AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                    new AlterMaterializedTableChangeOperation(
                            op.getTableIdentifier(),
                            oldTable -> op.getTableChanges(),
                            suspendMaterializedTable,
                            op.getAsQueryOperation());
            alterMaterializedTableChangeOperation.execute(ctx);

            // 3. resume the materialized table
            try {
                executeContinuousRefreshJob(
                        ctx,
                        alterMaterializedTableChangeOperation.getNewTable(),
                        tableIdentifier,
                        Map.of(),
                        Optional.empty());
            } catch (Exception e) {
                // Roll back the changes to the materialized table and restore the continuous
                // refresh job
                LOG.warn(
                        "Failed to start the continuous refresh job for materialized table {} using new query {}, rollback to origin query {}.",
                        tableIdentifier,
                        op.getNewTable().getExpandedQuery(),
                        suspendMaterializedTable.getExpandedQuery(),
                        e);

                AlterMaterializedTableChangeOperation rollbackChangeOperation =
                        generateRollbackAlterMaterializedTableOperation(
                                suspendMaterializedTable, alterMaterializedTableChangeOperation);
                rollbackChangeOperation.execute(ctx);

                ContinuousRefreshHandler continuousRefreshHandler =
                        deserializeContinuousHandler(
                                suspendMaterializedTable.getSerializedRefreshHandler());
                executeContinuousRefreshJob(
                        ctx,
                        suspendMaterializedTable,
                        tableIdentifier,
                        Map.of(),
                        continuousRefreshHandler.getRestorePath());

                throw new TableException(
                        String.format(
                                "Failed to start the continuous refresh job using new query %s when altering materialized table %s select query.",
                                op.getNewTable().getExpandedQuery(), tableIdentifier),
                        e);
            }
        } else if (RefreshStatus.SUSPENDED == oldMaterializedTable.getRefreshStatus()) {
            // alter schema & definition query & refresh handler (reset savepoint path of refresh
            // handler)
            List<TableChange> tableChanges = new ArrayList<>(op.getTableChanges());
            TableChange.ModifyRefreshHandler modifyRefreshHandler =
                    generateResetSavepointTableChange(
                            oldMaterializedTable.getSerializedRefreshHandler());
            tableChanges.add(modifyRefreshHandler);

            AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                    new AlterMaterializedTableChangeOperation(
                            tableIdentifier,
                            oldTable -> tableChanges,
                            oldMaterializedTable,
                            op.getAsQueryOperation());

            alterMaterializedTableChangeOperation.execute(ctx);
        } else {
            throw new TableException(
                    String.format(
                            "Materialized table %s is being initialized and does not support alter operation.",
                            tableIdentifier));
        }

        return TABLE_RESULT_OK;
    }

    private AlterMaterializedTableChangeOperation generateRollbackAlterMaterializedTableOperation(
            ResolvedCatalogMaterializedTable oldMaterializedTable,
            AlterMaterializedTableChangeOperation op) {

        return new AlterMaterializedTableChangeOperation(
                op.getTableIdentifier(),
                oldTable -> List.of(),
                oldMaterializedTable,
                op.getAsQueryOperation());
    }

    private TableChange.ModifyRefreshHandler generateResetSavepointTableChange(
            byte[] serializedContinuousHandler) {
        ContinuousRefreshHandler continuousRefreshHandler =
                deserializeContinuousHandler(serializedContinuousHandler);
        ContinuousRefreshHandler resetContinuousRefreshHandler =
                new ContinuousRefreshHandler(
                        continuousRefreshHandler.getExecutionTarget(),
                        continuousRefreshHandler.getClusterId(),
                        continuousRefreshHandler.getJobId());

        return TableChange.modifyRefreshHandler(
                resetContinuousRefreshHandler.asSummaryString(),
                serializeContinuousHandler(resetContinuousRefreshHandler));
    }

    private TableResultInternal callDropMaterializedTableOperation(
            Context ctx, DropMaterializedTableOperation dropMaterializedTableOperation) {
        ObjectIdentifier tableIdentifier = dropMaterializedTableOperation.getTableIdentifier();
        boolean tableExists = ctx.getCatalogManager().getTable(tableIdentifier).isPresent();
        if (!tableExists) {
            if (dropMaterializedTableOperation.isIfExists()) {
                LOG.info(
                        "Materialized table {} does not exists, skip the drop operation.",
                        tableIdentifier);
                return TABLE_RESULT_OK;
            } else {
                throw new ValidationException(
                        String.format(
                                "Materialized table with identifier %s does not exist.",
                                tableIdentifier));
            }
        }

        ResolvedCatalogMaterializedTable materializedTable =
                getCatalogMaterializedTable(ctx, tableIdentifier);
        RefreshStatus refreshStatus = materializedTable.getRefreshStatus();
        if (RefreshStatus.ACTIVATED == refreshStatus || RefreshStatus.SUSPENDED == refreshStatus) {
            RefreshMode refreshMode = materializedTable.getRefreshMode();
            if (RefreshMode.FULL == refreshMode) {
                deleteRefreshWorkflow(tableIdentifier, materializedTable);
            } else if (RefreshMode.CONTINUOUS == refreshMode
                    && RefreshStatus.ACTIVATED == refreshStatus) {
                cancelContinuousRefreshJob(ctx, tableIdentifier, materializedTable);
            }
        } else if (RefreshStatus.INITIALIZING == refreshStatus) {
            throw new ValidationException(
                    String.format(
                            "Current refresh status of materialized table %s is initializing, skip the drop operation.",
                            tableIdentifier.asSerializableString()));
        }

        dropMaterializedTableOperation.execute(ctx);

        return TABLE_RESULT_OK;
    }

    private void cancelContinuousRefreshJob(
            Context ctx,
            ObjectIdentifier tableIdentifier,
            CatalogMaterializedTable materializedTable) {
        ContinuousRefreshHandler refreshHandler =
                deserializeContinuousHandler(materializedTable.getSerializedRefreshHandler());
        // get job running status
        JobStatus jobStatus = getJobStatus(ctx, refreshHandler);
        if (!jobStatus.isTerminalState()) {
            try {
                cancelJob(ctx, refreshHandler);
            } catch (Exception e) {
                jobStatus = getJobStatus(ctx, refreshHandler);
                if (!jobStatus.isTerminalState()) {
                    throw new TableException(
                            String.format(
                                    "Failed to drop the materialized table %s because the continuous refresh job %s could not be canceled."
                                            + " The current status of the continuous refresh job is %s.",
                                    tableIdentifier, refreshHandler.getJobId(), jobStatus),
                            e);
                } else {
                    LOG.warn(
                            "An exception occurred while canceling the continuous refresh job {} for materialized table {},"
                                    + " but since the job is in a terminal state, skip the cancel operation.",
                            refreshHandler.getJobId(),
                            tableIdentifier);
                }
            }
        } else {
            LOG.info(
                    "No need to cancel the continuous refresh job {} for materialized table {} as it is not currently running.",
                    refreshHandler.getJobId(),
                    tableIdentifier);
        }
    }

    private void deleteRefreshWorkflow(
            ObjectIdentifier tableIdentifier, CatalogMaterializedTable catalogMaterializedTable) {
        if (workflowScheduler == null) {
            throw new TableException(
                    "The workflow scheduler must be configured when dropping materialized table in full refresh mode.");
        }
        try {
            RefreshHandlerSerializer<?> refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            RefreshHandler refreshHandler =
                    refreshHandlerSerializer.deserialize(
                            catalogMaterializedTable.getSerializedRefreshHandler(),
                            userCodeClassLoader);
            DeleteRefreshWorkflow deleteRefreshWorkflow = new DeleteRefreshWorkflow(refreshHandler);
            workflowScheduler.deleteRefreshWorkflow(deleteRefreshWorkflow);
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to delete the refresh workflow for materialized table %s.",
                            tableIdentifier),
                    e);
        }
    }

    /**
     * Retrieves the session configuration for initializing the periodic refresh job. The function
     * filters out default context configurations and removes unnecessary configurations such as
     * resources download directory and workflow scheduler related configurations.
     *
     * @param ctx the session the lifecycle was invoked from.
     * @return A Map containing the session configurations for initializing session for executing
     *     the periodic refresh job.
     */
    private Map<String, String> getSessionInitializationConf(Context ctx) {
        Map<String, String> sessionConf =
                new HashMap<>(ctx.getTableConfig().getConfiguration().toMap());

        // we only keep the session conf that is not in the default context or the conf value is
        // different from the default context.
        Map<String, String> defaultContextConf =
                ctx.getTableConfig().getRootConfiguration().toMap();
        sessionConf
                .entrySet()
                .removeIf(
                        entry -> {
                            String key = entry.getKey();
                            String value = entry.getValue();
                            return defaultContextConf.containsKey(key)
                                    && defaultContextConf.get(key).equals(value);
                        });

        // remove useless conf
        sessionConf.remove(TableConfigOptions.RESOURCES_DOWNLOAD_DIR.key());
        sessionConf.keySet().removeIf(key -> key.startsWith(WORKFLOW_SCHEDULER_PREFIX));

        return sessionConf;
    }

    private JobStatus getJobStatus(Context ctx, ContinuousRefreshHandler refreshHandler) {
        JobClient jobClient =
                resolveJobClient(ctx, refreshHandler)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                String.format(
                                                        "Could not reach refresh job %s to read its status.",
                                                        refreshHandler.getJobId())));
        return awaitJobAction(
                jobClient.getJobStatus(),
                ctx,
                String.format("get status of job %s", refreshHandler.getJobId()));
    }

    private void cancelJob(Context ctx, ContinuousRefreshHandler refreshHandler) {
        JobClient jobClient =
                resolveJobClient(ctx, refreshHandler)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                String.format(
                                                        "Could not reach refresh job %s to cancel it.",
                                                        refreshHandler.getJobId())));
        awaitJobAction(
                jobClient.cancel(), ctx, String.format("cancel job %s", refreshHandler.getJobId()));
    }

    private String stopJobWithSavepoint(Context ctx, ContinuousRefreshHandler refreshHandler) {
        // check savepoint dir is configured
        Optional<String> savepointDir =
                ctx.getTableConfig().getConfiguration().getOptional(SAVEPOINT_DIRECTORY);
        if (savepointDir.isEmpty()) {
            throw new ValidationException(
                    "Savepoint directory is not configured, can't stop job with savepoint.");
        }
        String jobId = refreshHandler.getJobId();
        JobClient jobClient =
                resolveJobClient(ctx, refreshHandler)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                String.format(
                                                        "Could not reach refresh job %s to stop it with a savepoint.",
                                                        jobId)));
        return awaitJobAction(
                jobClient.stopWithSavepoint(false, savepointDir.get(), SavepointFormatType.DEFAULT),
                ctx,
                String.format("stop job %s with savepoint", jobId));
    }

    private <T> T awaitJobAction(CompletableFuture<T> action, Context ctx, String description) {
        Duration clientTimeout =
                ctx.getTableConfig().getConfiguration().get(ClientOptions.CLIENT_TIMEOUT);
        try {
            return action.get(clientTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TableException(
                    String.format("Interrupted while trying to %s.", description), e);
        } catch (Exception e) {
            throw new TableException(String.format("Could not %s.", description), e);
        }
    }

    /**
     * Resolves a client for a refresh job.
     *
     * <p>Prefers the client handed back when this instance submitted the job, and falls back to
     * re-attaching to the cluster named by the handler. The fallback is what makes a job started by
     * an earlier session reachable; it is also the only path that cannot serve {@code
     * execution.target=embedded}, where there is no separate cluster to retrieve.
     */
    private Optional<JobClient> resolveJobClient(
            Context ctx, ContinuousRefreshHandler refreshHandler) {
        JobClient submitted = submittedJobClients.get(refreshHandler.getJobId());
        if (submitted != null) {
            return Optional.of(submitted);
        }
        return retrieveJobClient(ctx, refreshHandler);
    }

    private Optional<JobClient> retrieveJobClient(
            Context ctx, ContinuousRefreshHandler refreshHandler) {
        String target = refreshHandler.getExecutionTarget();
        Configuration configuration = new Configuration(ctx.getTableConfig().getConfiguration());
        configuration.set(TARGET, target);
        getClusterIdKeyName(target)
                .ifPresent(key -> configuration.setString(key, refreshHandler.getClusterId()));

        ClusterClientServiceLoader serviceLoader = new DefaultClusterClientServiceLoader();
        try {
            ClusterClientFactory<Object> clusterClientFactory =
                    serviceLoader.getClusterClientFactory(configuration);
            Object clusterId = clusterClientFactory.getClusterId(configuration);
            if (clusterId == null) {
                return Optional.empty();
            }
            try (ClusterDescriptor<Object> descriptor =
                    clusterClientFactory.createClusterDescriptor(configuration)) {
                ClusterClientProvider<Object> provider = descriptor.retrieve(clusterId);
                return Optional.of(
                        new ClusterClientJobClientAdapter<>(
                                provider,
                                JobID.fromHexString(refreshHandler.getJobId()),
                                userCodeClassLoader));
            }
        } catch (Exception e) {
            LOG.debug(
                    "Could not retrieve a job client for refresh job {} on execution target {}.",
                    refreshHandler.getJobId(),
                    target,
                    e);
            return Optional.empty();
        }
    }

    private ContinuousRefreshHandler deserializeContinuousHandler(byte[] serializedRefreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.deserialize(
                    serializedRefreshHandler, userCodeClassLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new TableException("Failed to deserialize ContinuousRefreshHandler.", e);
        }
    }

    private byte[] serializeContinuousHandler(ContinuousRefreshHandler refreshHandler) {
        try {
            return ContinuousRefreshHandlerSerializer.INSTANCE.serialize(refreshHandler);
        } catch (IOException e) {
            throw new TableException("Failed to serialize ContinuousRefreshHandler.", e);
        }
    }

    private ResolvedCatalogMaterializedTable getCatalogMaterializedTable(
            Context ctx, ObjectIdentifier tableIdentifier) {
        ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable =
                ctx.getCatalogManager().getTableOrError(tableIdentifier).getResolvedTable();
        if (MATERIALIZED_TABLE != resolvedCatalogBaseTable.getTableKind()) {
            throw new ValidationException(
                    String.format(
                            "Table %s is not a materialized table, does not support materialized table related operation.",
                            tableIdentifier));
        }

        return (ResolvedCatalogMaterializedTable) resolvedCatalogBaseTable;
    }

    private ResolvedCatalogMaterializedTable resolveCatalogMaterializedTable(
            Context ctx, CatalogMaterializedTable materializedTable) {
        return ctx.getCatalogManager().resolveCatalogMaterializedTable(materializedTable);
    }

    private ResolvedCatalogMaterializedTable updateRefreshHandler(
            Context ctx,
            ObjectIdentifier materializedTableIdentifier,
            ResolvedCatalogMaterializedTable catalogMaterializedTable,
            RefreshStatus refreshStatus,
            String refreshHandlerSummary,
            byte[] serializedRefreshHandler) {
        List<TableChange> tableChanges = new ArrayList<>();
        tableChanges.add(TableChange.modifyRefreshStatus(refreshStatus));
        tableChanges.add(
                TableChange.modifyRefreshHandler(refreshHandlerSummary, serializedRefreshHandler));
        AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                new AlterMaterializedTableChangeOperation(
                        materializedTableIdentifier,
                        oldTable -> tableChanges,
                        catalogMaterializedTable);
        // update RefreshHandler to Catalog
        alterMaterializedTableChangeOperation.execute(ctx);

        return resolveCatalogMaterializedTable(
                ctx, alterMaterializedTableChangeOperation.getNewTable());
    }

    /** Generate insert statement for materialized table. */
    @VisibleForTesting
    protected static String getInsertStatement(
            ObjectIdentifier materializedTableIdentifier,
            String definitionQuery,
            Map<String, String> dynamicOptions) {

        return String.format(
                "INSERT INTO %s\n%s",
                generateTableWithDynamicOptions(materializedTableIdentifier, dynamicOptions),
                definitionQuery);
    }

    private static String generateTableWithDynamicOptions(
            ObjectIdentifier objectIdentifier, Map<String, String> dynamicOptions) {
        StringBuilder builder = new StringBuilder(objectIdentifier.asSerializableString());

        if (!dynamicOptions.isEmpty()) {
            String hints =
                    dynamicOptions.entrySet().stream()
                            .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(", "));
            builder.append(String.format(" /*+ OPTIONS(%s) */", hints));
        }

        return builder.toString();
    }

    private JobExecutionResult executeRefreshJob(
            String script, Configuration executionConfig, Context ctx) {
        String executeTarget = ctx.getTableConfig().getConfiguration().get(TARGET);
        if (executeTarget == null || executeTarget.isEmpty() || "local".equals(executeTarget)) {
            String errorMessage =
                    String.format(
                            "Unsupported execution target detected: %s."
                                    + "Currently, only the following execution targets are supported: "
                                    + "'remote', 'yarn-session', 'yarn-application', 'kubernetes-session', "
                                    + "'kubernetes-application', 'embedded'. ",
                            executeTarget);
            LOG.error(errorMessage);
            throw new ValidationException(errorMessage);
        }

        if (executeTarget.endsWith("application")) {
            return executeApplicationJob(script, executionConfig, ctx);
        } else {
            return executeNonApplicationJob(script, executionConfig, ctx);
        }
    }

    private JobExecutionResult executeNonApplicationJob(
            String script, Configuration executionConfig, Context ctx) {
        String executeTarget = ctx.getTableConfig().getConfiguration().get(TARGET);
        String clusterId = resolveClusterId(ctx, executeTarget);

        TableResultInternal result = ctx.executeStatement(script, executionConfig);
        JobClient jobClient =
                result.getJobClient()
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                "Materialized table refresh statement did not produce a job. "
                                                        + "This usually means the statement was not an INSERT."));

        String jobId = jobClient.getJobID().toString();
        // keep the client: in application mode there is no cluster to re-attach to later
        submittedJobClients.put(jobId, jobClient);

        return new JobExecutionResult(executeTarget, clusterId, jobId);
    }

    /**
     * Determines the cluster identifier to persist alongside the refresh job.
     *
     * <p>Reads the deployment-specific option directly where there is one, because {@code
     * ClusterClientFactory} cannot resolve {@code execution.target=embedded} — the application mode
     * a Table API program runs under beneath the Kubernetes operator. Falls back to the factory for
     * targets that name their cluster no other way, and to an empty identifier when the job is
     * reachable only through the client returned at submission.
     */
    private String resolveClusterId(Context ctx, String executeTarget) {
        Configuration configuration = ctx.getTableConfig().getConfiguration();

        Optional<String> fromOption =
                getClusterIdKeyName(executeTarget)
                        .map(key -> configuration.getString(key, ""))
                        .filter(id -> !StringUtils.isNullOrWhitespaceOnly(id));
        if (fromOption.isPresent()) {
            return fromOption.get();
        }

        try {
            ClusterClientFactory<?> factory =
                    new DefaultClusterClientServiceLoader().getClusterClientFactory(configuration);
            Object clusterId = factory.getClusterId(configuration);
            if (clusterId != null) {
                return clusterId.toString();
            }
        } catch (Exception e) {
            LOG.debug("No cluster id available for execution target {}.", executeTarget, e);
        }

        return "";
    }

    private JobExecutionResult executeApplicationJob(
            String script, Configuration executionConfig, Context ctx) {
        List<String> arguments = new ArrayList<>();
        arguments.add("--" + SqlDriver.OPTION_SQL_SCRIPT.getLongOpt());
        arguments.add(script);

        Configuration mergedConfig = new Configuration(ctx.getTableConfig().getConfiguration());
        mergedConfig.addAll(executionConfig);
        JobID jobId = new JobID();
        mergedConfig.set(PIPELINE_FIXED_JOB_ID, jobId.toString());

        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        arguments.toArray(new String[0]), SqlDriver.class.getName());
        try {
            String clusterId =
                    new ApplicationClusterDeployer(new DefaultClusterClientServiceLoader())
                            .run(mergedConfig, applicationConfiguration)
                            .toString();

            return new JobExecutionResult(mergedConfig.get(TARGET), clusterId, jobId.toString());
        } catch (Throwable t) {
            LOG.error("Failed to deploy script {} to application cluster.", script, t);
            throw new TableException("Failed to deploy script to cluster.", t);
        }
    }

    private static class JobExecutionResult {

        private final String executionTarget;
        private final String clusterId;
        private final String jobId;

        public JobExecutionResult(String executionTarget, String clusterId, String jobId) {
            this.executionTarget = executionTarget;
            this.clusterId = clusterId;
            this.jobId = jobId;
        }
    }

    /**
     * Converts a refresh result row into the internal shape the gateway's REST handlers expect: a
     * job id string and the cluster coordinates as a map.
     */
    @SuppressWarnings("unchecked")
    private static RowData toRefreshResultRow(Row row) {
        Map<String, String> clusterInfo = (Map<String, String>) row.getField(1);
        Map<StringData, StringData> internalClusterInfo = new HashMap<>();
        clusterInfo.forEach(
                (key, value) ->
                        internalClusterInfo.put(
                                StringData.fromString(key), StringData.fromString(value)));
        return GenericRowData.of(
                StringData.fromString((String) row.getField(0)),
                new GenericMapData(internalClusterInfo));
    }

    private static Optional<String> getClusterIdKeyName(String targetName) {
        return MaterializedTableLifecycle.clusterIdKey(targetName);
    }

    private static IntervalFreshness validateAndGetIntervalFreshness(
            final CatalogMaterializedTable catalogMaterializedTable) {
        return Optional.ofNullable(catalogMaterializedTable.getDefinitionFreshness())
                .orElseThrow(() -> new TableException("Freshness cannot be null"));
    }
}

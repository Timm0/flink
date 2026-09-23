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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableClusterUtils;
import org.apache.flink.table.delegation.materializedtable.MaterializedTableJobSubmitter;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshWorkflowContext;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

/**
 * The default implementation of a {@link MaterializedTableJobSubmitter}.
 *
 * <p>It does not expose a {@link WorkflowScheduler}, so full-mode materialized tables cannot be
 * created, suspended, resumed or dropped here, while a one-time REFRESH still works.
 */
@Internal
public class DefaultMaterializedTableJobSubmitter implements MaterializedTableJobSubmitter {

    private static final String EMBEDDED_TARGET = "embedded";

    private static final String DEFAULT_REFRESH_JOB_NAME = "materialized-table-refresh";

    private final ExecutableOperation.Context operationCtx;
    private final Planner planner;
    private final Executor sessionExecutor;

    private final Map<String, JobClient> refreshJobClients = new HashMap<>();

    public DefaultMaterializedTableJobSubmitter(
            ExecutableOperation.Context operationCtx, Planner planner, Executor sessionExecutor) {
        this.operationCtx = operationCtx;
        this.planner = planner;
        this.sessionExecutor = sessionExecutor;
    }

    @Override
    public RefreshJobResult submitRefreshJob(
            String executionTarget, Configuration executionConfig, String insertStatement) {
        if (executionTarget.endsWith("application") || EMBEDDED_TARGET.equals(executionTarget)) {
            throw new TableException(
                    "Application-mode materialized table refresh is not supported in this environment.");
        }

        String clusterId =
                MaterializedTableClusterUtils.getClusterId(
                        executionTarget, operationCtx.getTableConfig());

        boolean isStreamingRefresh = STREAMING == executionConfig.get(RUNTIME_MODE);
        JobClient jobClient =
                isStreamingRefresh == operationCtx.isStreamingMode()
                        ? submitWithSessionPlanner(
                                executionTarget, executionConfig, insertStatement)
                        : submitWithThrowawayPlanner(
                                executionTarget, executionConfig, insertStatement);

        String jobId = jobClient.getJobID().toString();
        if (isStreamingRefresh) {
            refreshJobClients.put(jobId, jobClient);
        }

        return new RefreshJobResult(
                executionTarget,
                clusterId,
                jobId,
                MaterializedTableClusterUtils.buildClusterInfo(executionTarget, clusterId));
    }

    @Override
    public JobStatus getJobStatus(ContinuousRefreshHandler refreshHandler) {
        JobClient jobClient = requireLocalJobClient(refreshHandler);
        return awaitJobClientResult(
                jobClient.getJobStatus(),
                String.format(
                        "Failed to get the status of refresh job %s.", refreshHandler.getJobId()));
    }

    @Override
    public void cancelJob(ContinuousRefreshHandler refreshHandler) {
        JobClient jobClient = requireLocalJobClient(refreshHandler);
        awaitJobClientResult(
                jobClient.cancel(),
                String.format("Failed to cancel the refresh job %s.", refreshHandler.getJobId()));
        refreshJobClients.remove(refreshHandler.getJobId());
    }

    @Override
    public String stopJobWithSavepoint(ContinuousRefreshHandler refreshHandler) {
        JobClient jobClient = requireLocalJobClient(refreshHandler);
        String savepointDir =
                operationCtx.getTableConfig().get(CheckpointingOptions.SAVEPOINT_DIRECTORY);
        if (savepointDir == null || savepointDir.isEmpty()) {
            throw new ValidationException(
                    "Savepoint directory is not configured, can't stop job with savepoint.");
        }
        String savepointPath =
                awaitJobClientResult(
                        jobClient.stopWithSavepoint(
                                false, savepointDir, SavepointFormatType.DEFAULT),
                        String.format(
                                "Failed to stop the refresh job %s with a savepoint.",
                                refreshHandler.getJobId()));
        refreshJobClients.remove(refreshHandler.getJobId());
        return savepointPath;
    }

    @Override
    public Optional<RefreshWorkflowContext> getRefreshWorkflowContext() {
        return Optional.empty();
    }

    private JobClient submitWithSessionPlanner(
            String executionTarget, Configuration executionConfig, String insertStatement) {
        ModifyOperation modifyOperation = parseSingleInsertOperation(planner, insertStatement);
        operationCtx.getResourceManager().addJarConfiguration(operationCtx.getTableConfig());

        Configuration pipelineConfig =
                new Configuration(operationCtx.getTableConfig().getConfiguration());
        pipelineConfig.addAll(executionConfig);
        return submitPipeline(
                planner, modifyOperation, createRefreshExecutor(), pipelineConfig, executionTarget);
    }

    private JobClient submitWithThrowawayPlanner(
            String executionTarget, Configuration executionConfig, String insertStatement) {
        Configuration refreshConfig =
                new Configuration(operationCtx.getTableConfig().getConfiguration());
        refreshConfig.addAll(executionConfig);
        TableConfig refreshTableConfig = TableConfig.getDefault();
        refreshTableConfig.setRootConfiguration(
                operationCtx.getTableConfig().getRootConfiguration());
        refreshTableConfig.addConfiguration(refreshConfig);

        Executor refreshExecutor = createRefreshExecutor();
        Planner refreshPlanner =
                PlannerFactoryUtil.createPlanner(
                        refreshExecutor,
                        refreshTableConfig,
                        operationCtx.getResourceManager().getUserClassLoader(),
                        operationCtx.getModuleManager(),
                        operationCtx.getCatalogManager(),
                        operationCtx.getFunctionCatalog());

        ModifyOperation modifyOperation =
                parseSingleInsertOperation(refreshPlanner, insertStatement);
        operationCtx.getResourceManager().addJarConfiguration(refreshTableConfig);

        return submitPipeline(
                refreshPlanner,
                modifyOperation,
                refreshExecutor,
                refreshTableConfig.getConfiguration(),
                executionTarget);
    }

    private Executor createRefreshExecutor() {
        ExecutorFactory executorFactory =
                FactoryUtil.discoverFactory(
                        operationCtx.getResourceManager().getUserClassLoader(),
                        ExecutorFactory.class,
                        ExecutorFactory.DEFAULT_IDENTIFIER);
        return executorFactory.create(
                Configuration.fromMap(sessionExecutor.getConfiguration().toMap()));
    }

    private static JobClient submitPipeline(
            Planner planner,
            ModifyOperation modifyOperation,
            Executor refreshExecutor,
            Configuration pipelineConfig,
            String executionTarget) {
        List<Transformation<?>> transformations = planner.translate(List.of(modifyOperation));
        Configuration jobConfig = new Configuration(pipelineConfig);
        jobConfig.set(DeploymentOptions.TARGET, executionTarget);
        Pipeline pipeline =
                refreshExecutor.createPipeline(
                        transformations, jobConfig, DEFAULT_REFRESH_JOB_NAME);
        try {
            return refreshExecutor.executeAsync(pipeline);
        } catch (Exception e) {
            throw new TableException("Failed to submit the materialized table refresh job.", e);
        }
    }

    private static ModifyOperation parseSingleInsertOperation(
            Planner planner, String insertStatement) {
        List<Operation> operations = planner.getParser().parse(insertStatement);
        if (operations.size() != 1 || !(operations.get(0) instanceof ModifyOperation)) {
            throw new TableException(
                    String.format(
                            "The materialized table refresh statement must translate to a single"
                                    + " INSERT operation, but was: %s",
                            insertStatement));
        }
        return (ModifyOperation) operations.get(0);
    }

    @VisibleForTesting
    static <T> T awaitJobClientResult(CompletableFuture<T> request, String errorMessage) {
        try {
            return request.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TableException(errorMessage, e);
        } catch (Exception e) {
            throw new TableException(errorMessage, e);
        }
    }

    private JobClient requireLocalJobClient(ContinuousRefreshHandler refreshHandler) {
        JobClient jobClient = refreshJobClients.get(refreshHandler.getJobId());
        if (jobClient == null) {
            throw new TableException(
                    "Controlling a refresh job started by another session is not supported in this environment.");
        }
        return jobClient;
    }
}

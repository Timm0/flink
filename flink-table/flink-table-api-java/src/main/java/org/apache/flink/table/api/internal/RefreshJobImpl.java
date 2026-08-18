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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.RefreshJob;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link RefreshJob} over a {@link JobClient}, or over nothing but the job's coordinates when no
 * client is reachable.
 *
 * <p>Status and {@link #await()} need a client and say so plainly when there is none, rather than
 * inventing a status. The identity of the job — its id and cluster info — is always available,
 * because it comes from the catalog.
 */
@Internal
class RefreshJobImpl implements RefreshJob {

    private static final long POLL_INTERVAL_MILLIS = 200;

    private final String jobId;
    private final Map<String, String> clusterInfo;
    private final @Nullable JobClient jobClient;

    private RefreshJobImpl(
            String jobId, Map<String, String> clusterInfo, @Nullable JobClient jobClient) {
        this.jobId = jobId;
        this.clusterInfo = clusterInfo;
        this.jobClient = jobClient;
    }

    /** The continuous refresh job recorded in a table's persisted refresh handler. */
    static RefreshJob ofContinuousRefresh(
            ContinuousRefreshHandler handler, @Nullable JobClient jobClient) {
        return new RefreshJobImpl(
                handler.getJobId(),
                MaterializedTableLifecycle.clusterInfo(
                        handler.getExecutionTarget(), handler.getClusterId()),
                jobClient);
    }

    /**
     * The job a refresh just submitted, read out of the lifecycle's result.
     *
     * <p>The row shape — {@code (job id STRING, cluster info MAP<STRING, STRING>)} — is the
     * lifecycle's published contract, shared with the SQL Gateway's REST responses.
     */
    @SuppressWarnings("unchecked")
    static RefreshJob ofRefreshResult(TableResultInternal refreshResult) {
        final Row row;
        try {
            row = refreshResult.collect().next();
        } catch (Exception e) {
            throw new TableException("Materialized table refresh returned no job.", e);
        }
        return new RefreshJobImpl(
                (String) row.getField(0),
                (Map<String, String>) row.getField(1),
                refreshResult.getJobClient().orElse(null));
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public JobStatus getStatus() {
        try {
            return requireJobClient("read the status of").getJobStatus().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TableException(
                    String.format("Interrupted while reading the status of job %s.", jobId), e);
        } catch (ExecutionException e) {
            throw new TableException(
                    String.format("Failed to read the status of job %s.", jobId), e);
        }
    }

    @Override
    public Map<String, String> getClusterInfo() {
        return Map.copyOf(clusterInfo);
    }

    @Override
    public Optional<JobClient> getJobClient() {
        return Optional.ofNullable(jobClient);
    }

    @Override
    public void await() throws InterruptedException, ExecutionException {
        pollUntilTerminal(Long.MAX_VALUE);
    }

    @Override
    public void await(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!pollUntilTerminal(System.nanoTime() + unit.toNanos(timeout))) {
            throw new TimeoutException(
                    String.format(
                            "Job %s did not reach a terminal state within %d %s.",
                            jobId, timeout, unit.name().toLowerCase()));
        }
    }

    /**
     * Polls rather than waiting on {@code getJobExecutionResult()}, which is only available for
     * jobs submitted in attached mode. A refresh job is always detached — it outlives the statement
     * that started it — so status polling is the only way that works for every execution target.
     *
     * @return false if the deadline passed before the job became terminal
     */
    private boolean pollUntilTerminal(long deadlineNanos)
            throws InterruptedException, ExecutionException {
        final JobClient client = requireJobClient("wait for");
        while (true) {
            if (client.getJobStatus().get().isTerminalState()) {
                return true;
            }
            if (System.nanoTime() - deadlineNanos >= 0) {
                return false;
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
    }

    private JobClient requireJobClient(String action) {
        if (jobClient == null) {
            throw new TableException(
                    String.format(
                            "Cannot %s refresh job %s: no client for it is available. The job runs on "
                                    + "cluster %s, which this session cannot reach.",
                            action, jobId, clusterInfo));
        }
        return jobClient;
    }
}

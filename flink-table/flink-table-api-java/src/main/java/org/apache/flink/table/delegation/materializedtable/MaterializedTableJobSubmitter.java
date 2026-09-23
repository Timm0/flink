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

package org.apache.flink.table.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import java.util.Optional;

/**
 * Provides the caller-specific job-submission and refresh capabilities a {@link
 * MaterializedTableExecutor} needs to run materialized table operations.
 *
 * <p>The job-control methods apply to every job submitter. Periodic refresh scheduling is an
 * optional capability exposed through {@link #getRefreshWorkflowContext()}.
 */
@Internal
public interface MaterializedTableJobSubmitter {

    /**
     * Submits a refresh job with the given execution configuration and INSERT statement. The
     * implementation owns how the job reaches the cluster for the given target.
     *
     * @param executionTarget the resolved execution target the job is submitted to.
     * @param executionConfig the configuration used to submit the refresh job.
     * @param insertStatement the INSERT statement executed to refresh the materialized table.
     * @return the result identifying the submitted refresh job.
     */
    RefreshJobResult submitRefreshJob(
            String executionTarget, Configuration executionConfig, String insertStatement);

    /** Returns the current status of the job identified by the given refresh handler. */
    JobStatus getJobStatus(ContinuousRefreshHandler refreshHandler);

    /** Cancels the job identified by the given refresh handler. */
    void cancelJob(ContinuousRefreshHandler refreshHandler);

    /**
     * Stops the job identified by the given refresh handler with a savepoint.
     *
     * @return the path of the savepoint taken while stopping the job.
     */
    String stopJobWithSavepoint(ContinuousRefreshHandler refreshHandler);

    /**
     * Returns the periodic-refresh scheduling capability for full-mode materialized tables, or
     * {@link Optional#empty()} when this job submitter cannot schedule refresh workflows.
     */
    Optional<RefreshWorkflowContext> getRefreshWorkflowContext();
}

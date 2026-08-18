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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Flink job that refreshes a materialized table.
 *
 * <p>{@link MaterializedTable#refresh()} returns the batch job it just submitted; {@link
 * MaterializedTable#getRefreshJob()} returns the continuously running one, if there is one.
 */
@PublicEvolving
public interface RefreshJob {

    /** The refresh job's id. */
    String getJobId();

    /** Current status of the job. */
    JobStatus getStatus();

    /**
     * The coordinates needed to reach this job from a later process: {@code execution.target}, plus
     * {@code yarn.application.id} or {@code kubernetes.cluster-id} for those targets.
     */
    Map<String, String> getClusterInfo();

    /**
     * A client for the refresh job. Absent when the backend does not expose one, or when the job
     * has already been cleaned up.
     */
    Optional<JobClient> getJobClient();

    /**
     * Blocks until the job reaches a terminal state; returns immediately if it is already terminal.
     *
     * <p>For a continuous refresh job that means blocking until it is suspended or fails.
     */
    void await() throws InterruptedException, ExecutionException;

    /** As {@link #await()}, but gives up after the given timeout. */
    void await(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;
}

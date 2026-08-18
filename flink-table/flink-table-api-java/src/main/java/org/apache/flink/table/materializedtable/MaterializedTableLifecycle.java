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

package org.apache.flink.table.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperation;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Executes the lifecycle of a materialized table: creating it together with its refresh job or
 * refresh workflow, suspending and resuming that job, refreshing partitions on demand, and dropping
 * the table together with whatever is running behind it.
 *
 * <p>This is the single extension point for materialized tables. A vendor whose control plane owns
 * job submission and scheduling replaces the whole implementation rather than individual steps,
 * because the interesting part of the lifecycle is the ordering between catalog writes and job
 * actions — not the individual actions. Flink ships a working default, so nothing needs to be
 * configured for materialized tables to work.
 *
 * <p>Implementations are stateful (they own a workflow scheduler and any cached job handles) and
 * are created once per session via {@link
 * org.apache.flink.table.factories.MaterializedTableLifecycleFactory}.
 *
 * <p>Note: this interface traffics in {@code @Internal} types ({@link TableResultInternal}, {@link
 * MaterializedTableOperation}) and is therefore itself internal. Stabilising it is future work.
 */
@Internal
public interface MaterializedTableLifecycle extends AutoCloseable {

    /** Opens any resources the implementation needs, e.g. a workflow scheduler. */
    void open() throws Exception;

    @Override
    void close() throws Exception;

    /**
     * Executes a materialized table operation — CREATE, DROP, the ALTER variants, or an in-place
     * conversion of a regular table.
     *
     * <p>Catalog mutation is the caller's job only in the sense that the implementation performs it
     * through {@code operation.execute(ctx)}; the implementation owns the surrounding job and
     * workflow actions, and the compensation when they fail.
     */
    TableResultInternal execute(Context ctx, MaterializedTableOperation operation);

    /**
     * Refreshes a materialized table, either on demand (from {@code ALTER MATERIALIZED TABLE ...
     * REFRESH} or {@code MaterializedTable#refresh}) or from a periodic trigger.
     *
     * <p>Separate from {@link #execute} because a periodic trigger has no operation to hand over:
     * it carries a schedule time from which the refreshed partitions are derived.
     *
     * @param staticPartitions the partitions to refresh; ignored when {@code periodic} is true
     * @param scheduleTime the trigger's fire time, required when {@code periodic} is true
     */
    TableResultInternal refresh(
            Context ctx,
            ObjectIdentifier identifier,
            Map<String, String> staticPartitions,
            Map<String, String> dynamicOptions,
            boolean periodic,
            @Nullable String scheduleTime);

    /**
     * Returns a client for the table's continuous refresh job, if one is reachable.
     *
     * <p>Not limited to jobs this session submitted — a job started by an earlier session is
     * resolved from the table's persisted refresh handler. Empty when the table has no continuous
     * refresh job, or when the backend cannot produce a client for it.
     */
    Optional<JobClient> getRefreshJobClient(Context ctx, ObjectIdentifier identifier);

    /**
     * The coordinates a later process needs to reach a refresh job: {@code execution.target}, plus
     * the cluster identifier under the key that deployment mode reads it from.
     *
     * <p>Defined here because it is part of the lifecycle's contract — the same shape is persisted
     * in the refresh handler, returned from a refresh, and reported by {@code
     * RefreshJob#getClusterInfo()}.
     */
    static Map<String, String> clusterInfo(String executionTarget, @Nullable String clusterId) {
        final Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(DeploymentOptions.TARGET.key(), executionTarget);
        clusterIdKey(executionTarget)
                .ifPresent(key -> clusterInfo.put(key, clusterId == null ? "" : clusterId));
        return clusterInfo;
    }

    /** The configuration key under which the given execution target names its cluster, if any. */
    static Optional<String> clusterIdKey(String executionTarget) {
        if (executionTarget.startsWith("yarn")) {
            return Optional.of("yarn.application.id");
        } else if (executionTarget.startsWith("kubernetes")) {
            return Optional.of("kubernetes.cluster-id");
        }
        return Optional.empty();
    }

    /**
     * Everything the lifecycle needs from the session that invoked it.
     *
     * <p>Extends {@link ExecutableOperation.Context} so that the catalog-writing half of each
     * operation is literally {@code operation.execute(ctx)} — the same call a plain {@code
     * TableEnvironment} makes. That base already supplies the catalog manager, the resource manager
     * (and through it the user classloader), and the {@link org.apache.flink.table.api.TableConfig}
     * carrying the session configuration, the root configuration and the local time zone.
     *
     * <p>The one thing it adds is statement execution, which is how a refresh job is submitted. The
     * SQL Gateway and a plain {@code TableEnvironment} differ only in how they satisfy it.
     */
    @Internal
    interface Context extends ExecutableOperation.Context {

        /**
         * Executes a single statement — in practice the refresh {@code INSERT} — with the given
         * configuration layered on top of the session configuration.
         *
         * <p>The returned result carries a {@link JobClient} when the submission produced one,
         * which is what lets the lifecycle control a job it started without re-attaching to the
         * cluster.
         */
        TableResultInternal executeStatement(String statement, Configuration executionConfig);
    }
}

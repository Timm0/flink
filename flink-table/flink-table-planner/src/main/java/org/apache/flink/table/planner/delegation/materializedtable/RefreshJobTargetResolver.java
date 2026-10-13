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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.materializedtable.RefreshJobConfiguration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import java.util.Optional;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Resolves and validates where this program's materialized table refresh jobs run.
 *
 * <p>It reads the {@link TableConfig} on every call, so options set after it was created apply.
 */
@Internal
final class RefreshJobTargetResolver {

    private static final String LOCAL_TARGET = "local";
    private static final String EMBEDDED_TARGET = "embedded";
    private static final String REMOTE_TARGET = "remote";

    private static final String TABLE_CONFIG_LAYER =
            "TableConfig (TableConfig#set, SQL SET or EnvironmentSettings.Builder#withConfiguration)";

    @VisibleForTesting
    static final String ROOT_LAYER =
            "execution environment's configuration (config.yaml, the options the program was "
                    + "submitted with, or the Configuration passed to StreamExecutionEnvironment)";

    private static final String ON_THE_TABLE_CONFIG_WITH_THE_TARGET =
            "the " + TABLE_CONFIG_LAYER + ", where 'execution.target' is set";
    private static final String ON_EITHER_LAYER = "TableConfig or the " + ROOT_LAYER;

    private final TableConfig tableConfig;
    private final boolean supportsApplicationTargets;

    RefreshJobTargetResolver(TableConfig tableConfig, boolean supportsApplicationTargets) {
        this.tableConfig = checkNotNull(tableConfig);
        this.supportsApplicationTargets = supportsApplicationTargets;
    }

    /** Resolves the target a new refresh job is submitted to. */
    RefreshJobTarget resolve() {
        final boolean targetOnTableConfig =
                RefreshJobConfiguration.isTargetOnTableConfig(tableConfig);
        final ReadableConfig layer =
                targetOnTableConfig
                        ? tableConfig.getConfiguration()
                        : tableConfig.getRootConfiguration();
        final String layerName = targetOnTableConfig ? TABLE_CONFIG_LAYER : ROOT_LAYER;
        final String executionTarget = layer.getOptional(TARGET).orElse(null);

        rejectUnsetLocalAndEmbeddedTargets(executionTarget, layerName);
        final RefreshJobTarget unresolvedTarget = new RefreshJobTarget(executionTarget, "");
        rejectApplicationTargetWithoutSupport(unresolvedTarget);
        requireRemoteRestAddress(executionTarget);

        final ReadableConfig clusterIdLayer = targetOnTableConfig ? layer : tableConfig;
        final String clusterIdLayerName =
                targetOnTableConfig ? ON_THE_TABLE_CONFIG_WITH_THE_TARGET : ON_EITHER_LAYER;
        return new RefreshJobTarget(
                executionTarget,
                resolveClusterId(clusterIdLayer, unresolvedTarget, clusterIdLayerName));
    }

    /**
     * Resolves the target for restarting a recorded refresh job where restarts stay on the recorded
     * cluster.
     *
     * <p>Cluster ids are compared only for targets that identify their cluster by id, for the
     * others only the target is compared.
     *
     * @throws ValidationException if the job runs in its own application cluster, if this program's
     *     target differs from the recorded one, or if this program's target cannot be resolved (see
     *     {@link #resolve})
     */
    RefreshJobTarget resolveOnRecordedCluster(
            ObjectIdentifier tableIdentifier, ContinuousRefreshHandler refreshHandler) {
        final RefreshJobTarget recordedTarget = RefreshJobTarget.recordedIn(refreshHandler);
        if (recordedTarget.isApplicationTarget()) {
            throw new ValidationException(
                    String.format(
                            "The continuous refresh job of materialized table %s runs in its own "
                                    + "application cluster (%s). Restarting it deploys a new "
                                    + "application cluster, which only the SQL Gateway can do, so "
                                    + "resume, alter or redeploy the table through the SQL Gateway. "
                                    + "Suspending, refreshing and dropping it work here.",
                            tableIdentifier, describe(recordedTarget)));
        }

        final RefreshJobTarget programTarget = resolve();
        final boolean sameTarget =
                recordedTarget.getExecutionTarget().equals(programTarget.getExecutionTarget());
        final boolean sameCluster =
                recordedTarget.identifyingClusterIdKey().isEmpty()
                        || recordedTarget.getClusterId().equals(programTarget.getClusterId());
        if (!sameTarget || !sameCluster) {
            throw new ValidationException(
                    String.format(
                            "The continuous refresh job of materialized table %s runs on %s, but "
                                    + "this program submits refresh jobs to %s. RESUME, ALTER and a "
                                    + "redeploy never move a refresh job to another cluster; set "
                                    + "'execution.target' and its cluster id to the recorded values "
                                    + "to restart it here.",
                            tableIdentifier, describe(recordedTarget), describe(programTarget)));
        }
        return programTarget;
    }

    /**
     * Requires the REST address of a {@code remote} cluster on the TableConfig if it sets {@code
     * execution.target}.
     *
     * @param executionTarget the target this program submits to, or the recorded target of a job it
     *     controls
     */
    void requireRemoteRestAddress(String executionTarget) {
        if (!REMOTE_TARGET.equals(executionTarget)
                || !RefreshJobConfiguration.isTargetOnTableConfig(tableConfig)) {
            return;
        }
        final Configuration tableConfigLayer = tableConfig.getConfiguration();
        // with high availability, leader retrieval finds the cluster and no REST address is read
        if (!HighAvailabilityMode.isHighAvailabilityModeActivated(tableConfigLayer)) {
            requireOption(
                    tableConfigLayer,
                    RestOptions.ADDRESS,
                    executionTarget,
                    ON_THE_TABLE_CONFIG_WITH_THE_TARGET);
        }
    }

    private void rejectApplicationTargetWithoutSupport(RefreshJobTarget target) {
        if (target.isApplicationTarget() && !supportsApplicationTargets) {
            throw new ValidationException(
                    String.format(
                            "Application-mode materialized table refresh is not supported in this "
                                    + "environment: execution target '%s' requires the SQL Gateway. Use a "
                                    + "session cluster target such as 'remote', 'yarn-session' or "
                                    + "'kubernetes-session'.",
                            target.getExecutionTarget()));
        }
    }

    private static void rejectUnsetLocalAndEmbeddedTargets(
            String executionTarget, String layerName) {
        if (executionTarget == null
                || executionTarget.isEmpty()
                || LOCAL_TARGET.equals(executionTarget)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported execution target detected: %s (read from 'execution.target' "
                                    + "in the %s). Materialized table refresh jobs require a remote or "
                                    + "cluster execution target; 'local' is not supported.",
                            executionTarget, layerName));
        }
        if (EMBEDDED_TARGET.equals(executionTarget)) {
            throw new ValidationException(
                    "Execution target 'embedded' is not supported for materialized table refresh "
                            + "jobs. A program running in application mode must set 'execution.target' "
                            + "and the cluster id of a session cluster on its TableConfig.");
        }
    }

    private static String describe(RefreshJobTarget target) {
        return target.identifyingClusterIdKey()
                .map(
                        clusterIdKey ->
                                String.format(
                                        "execution.target '%s', %s '%s'",
                                        target.getExecutionTarget(),
                                        clusterIdKey,
                                        target.getClusterId()))
                .orElseGet(
                        () -> String.format("execution.target '%s'", target.getExecutionTarget()));
    }

    private static String resolveClusterId(
            ReadableConfig layer, RefreshJobTarget target, String layerName) {
        final Optional<String> clusterIdKey = target.getClusterIdKey();
        if (target.isApplicationTarget() || clusterIdKey.isEmpty()) {
            return "";
        }
        return requireOption(
                layer,
                ConfigOptions.key(clusterIdKey.get()).stringType().noDefaultValue(),
                target.getExecutionTarget(),
                layerName);
    }

    private static String requireOption(
            ReadableConfig layer,
            ConfigOption<String> option,
            String executionTarget,
            String layerName) {
        final Optional<String> value =
                layer.getOptional(option).filter(configured -> !configured.isBlank());
        if (value.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Execution target '%s' requires '%s' to be configured to submit or "
                                    + "control a materialized table refresh job. It must be set on "
                                    + "%s.",
                            executionTarget, option.key(), layerName));
        }
        return value.get();
    }
}

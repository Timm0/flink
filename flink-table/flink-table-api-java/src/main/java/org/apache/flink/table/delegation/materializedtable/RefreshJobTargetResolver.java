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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import java.util.Optional;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.table.delegation.materializedtable.MaterializedTableClusterUtils.EMBEDDED_TARGET;
import static org.apache.flink.table.delegation.materializedtable.MaterializedTableClusterUtils.LOCAL_TARGET;
import static org.apache.flink.table.delegation.materializedtable.MaterializedTableClusterUtils.REMOTE_TARGET;

/** Resolves and validates where a materialized table refresh job runs. */
@Internal
public final class RefreshJobTargetResolver {

    private static final String TABLE_CONFIG_LAYER =
            "TableConfig (TableConfig#set, SQL SET or EnvironmentSettings.Builder#withConfiguration)";

    @VisibleForTesting
    public static final String ROOT_LAYER =
            "execution environment's configuration (config.yaml, the options the program was "
                    + "submitted with, or the Configuration passed to StreamExecutionEnvironment)";

    public static RefreshJobTarget resolve(
            TableConfig tableConfig, boolean supportsApplicationTargets) {
        final boolean targetOnTableConfig = isTargetOnTableConfig(tableConfig);
        final ReadableConfig layer =
                targetOnTableConfig
                        ? tableConfig.getConfiguration()
                        : tableConfig.getRootConfiguration();
        final String layerName = targetOnTableConfig ? TABLE_CONFIG_LAYER : ROOT_LAYER;
        final String executionTarget = layer.getOptional(TARGET).orElse(null);

        rejectUnsupportedTarget(executionTarget, layerName, supportsApplicationTargets);
        validateUnrecordedCoordinates(tableConfig, executionTarget);
        return new RefreshJobTarget(
                executionTarget, resolveClusterId(layer, executionTarget, layerName));
    }

    /** The target the catalog records for a refresh job, as recorded. */
    public static RefreshJobTarget recordedTarget(ContinuousRefreshHandler refreshHandler) {
        return new RefreshJobTarget(
                refreshHandler.getExecutionTarget(),
                refreshHandler.getClusterId() == null ? "" : refreshHandler.getClusterId());
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
    public static RefreshJobTarget resolveOnRecordedCluster(
            TableConfig tableConfig,
            ObjectIdentifier tableIdentifier,
            RefreshJobTarget recordedTarget) {
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

        final RefreshJobTarget programTarget = resolve(tableConfig, false);
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

    /**
     * Pins the coordinates the catalog does not record onto a configuration that is applied over
     * the merged TableConfig and root layers, if the TableConfig sets {@code execution.target}.
     */
    public static void pinUnrecordedCoordinates(
            TableConfig tableConfig, String executionTarget, Configuration configuration) {
        if (!isTargetOnTableConfig(tableConfig)) {
            return;
        }
        final Configuration tableConfigLayer = tableConfig.getConfiguration();
        if (REMOTE_TARGET.equals(executionTarget)) {
            // read with its 'jobmanager.rpc.address' fallback, which a 'rest.address' in the root
            // layer would otherwise override
            tableConfigLayer
                    .getOptional(RestOptions.ADDRESS)
                    .ifPresent(address -> configuration.set(RestOptions.ADDRESS, address));
            tableConfigLayer
                    .getOptional(RestOptions.PORT)
                    .ifPresent(port -> configuration.set(RestOptions.PORT, port));
        }
        configuration.set(
                HighAvailabilityOptions.HA_MODE,
                tableConfigLayer.get(HighAvailabilityOptions.HA_MODE));
        tableConfigLayer
                .getOptional(HighAvailabilityOptions.HA_CLUSTER_ID)
                .ifPresent(
                        clusterId ->
                                configuration.set(
                                        HighAvailabilityOptions.HA_CLUSTER_ID, clusterId));
    }

    private static boolean isTargetOnTableConfig(TableConfig tableConfig) {
        return tableConfig.getConfiguration().getOptional(TARGET).isPresent();
    }

    public static void validateUnrecordedCoordinates(
            TableConfig tableConfig, String executionTarget) {
        if (!isTargetOnTableConfig(tableConfig)) {
            return;
        }
        final Configuration tableConfigLayer = tableConfig.getConfiguration();
        final boolean highAvailability = isHighAvailabilityActivated(tableConfigLayer);
        final boolean remoteTargetOnTableConfig =
                REMOTE_TARGET.equals(executionTarget)
                        && REMOTE_TARGET.equals(tableConfigLayer.get(TARGET));
        // with high availability, leader retrieval finds the cluster and no REST address is read
        if (remoteTargetOnTableConfig && !highAvailability) {
            requireOption(
                    tableConfigLayer, RestOptions.ADDRESS, executionTarget, TABLE_CONFIG_LAYER);
        }
        if (highAvailability
                && tableConfigLayer.getOptional(HighAvailabilityOptions.HA_CLUSTER_ID).isEmpty()
                && tableConfig
                        .getRootConfiguration()
                        .getOptional(HighAvailabilityOptions.HA_CLUSTER_ID)
                        .isPresent()) {
            throw new ValidationException(
                    String.format(
                            "High availability is enabled on the %s, where 'execution.target' is "
                                    + "set, so '%s' must be set there too: the one in the %s "
                                    + "identifies the cluster this program runs on.",
                            TABLE_CONFIG_LAYER,
                            HighAvailabilityOptions.HA_CLUSTER_ID.key(),
                            ROOT_LAYER));
        }
    }

    /**
     * Mirrors {@code HighAvailabilityMode#isHighAvailabilityModeActivated} in flink-runtime, which
     * this module does not depend on: unset, 'NONE' and the legacy 'standalone' mean no HA.
     */
    private static boolean isHighAvailabilityActivated(ReadableConfig configuration) {
        final String haMode = configuration.get(HighAvailabilityOptions.HA_MODE);
        return haMode != null
                && !haMode.equalsIgnoreCase("NONE")
                && !haMode.equalsIgnoreCase("standalone");
    }

    private static void rejectUnsupportedTarget(
            String executionTarget, String layerName, boolean supportsApplicationTargets) {
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
        if (MaterializedTableClusterUtils.isApplicationTarget(executionTarget)
                && !supportsApplicationTargets) {
            throw new ValidationException(
                    String.format(
                            "Application-mode materialized table refresh is not supported in this "
                                    + "environment: execution target '%s' requires the SQL Gateway. Use a "
                                    + "session cluster target such as 'remote', 'yarn-session' or "
                                    + "'kubernetes-session'.",
                            executionTarget));
        }
    }

    private static String resolveClusterId(
            ReadableConfig layer, String executionTarget, String layerName) {
        final Optional<String> clusterIdKey =
                MaterializedTableClusterUtils.getClusterIdKey(executionTarget);
        if (MaterializedTableClusterUtils.isApplicationTarget(executionTarget)
                || clusterIdKey.isEmpty()) {
            return "";
        }
        return requireOption(
                layer,
                ConfigOptions.key(clusterIdKey.get()).stringType().noDefaultValue(),
                executionTarget,
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
                                    + "the %s, where 'execution.target' is set.",
                            executionTarget, option.key(), layerName));
        }
        return value.get();
    }

    private RefreshJobTargetResolver() {}
}

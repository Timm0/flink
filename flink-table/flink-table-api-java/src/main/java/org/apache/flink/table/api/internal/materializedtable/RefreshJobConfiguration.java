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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The root-configuration layer that materialized table refresh jobs are submitted and controlled
 * with, below the {@link TableConfig}.
 */
@Internal
public final class RefreshJobConfiguration {

    private static final Set<String> CLUSTER_SELECTING_KEYS =
            Set.of(
                    RestOptions.ADDRESS.key(),
                    JobManagerOptions.ADDRESS.key(),
                    RefreshJobTarget.KUBERNETES_CLUSTER_ID_KEY,
                    RefreshJobTarget.YARN_APPLICATION_ID_KEY);

    private static final List<String> HIGH_AVAILABILITY_KEY_PREFIXES =
            List.of("high-availability", "recovery.");

    public static boolean isTargetOnTableConfig(TableConfig tableConfig) {
        return tableConfig.getConfiguration().getOptional(DeploymentOptions.TARGET).isPresent();
    }

    /**
     * Returns a copy of the root configuration for refresh-job submission and control, without the
     * options that select a cluster (addresses, cluster ids, high availability) if the {@link
     * TableConfig} sets {@code execution.target}, because the TableConfig alone then selects the
     * cluster.
     */
    public static Configuration rootLayer(
            ReadableConfig rootConfiguration, TableConfig tableConfig) {
        final Map<String, String> rootOptions = new HashMap<>(rootConfiguration.toMap());
        if (isTargetOnTableConfig(tableConfig)) {
            rootOptions.keySet().removeIf(RefreshJobConfiguration::selectsACluster);
        }
        return Configuration.fromMap(rootOptions);
    }

    private static boolean selectsACluster(String key) {
        return CLUSTER_SELECTING_KEYS.contains(key)
                || HIGH_AVAILABILITY_KEY_PREFIXES.stream().anyMatch(key::startsWith);
    }

    private RefreshJobConfiguration() {}
}

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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;

/** Utilities for the cluster coordinates recorded for materialized table refresh jobs. */
@Internal
public final class MaterializedTableClusterUtils {

    /**
     * Returns the cluster id configured for the given execution target, or an empty string when the
     * target does not identify its cluster by id.
     *
     * @throws ValidationException if the target identifies its cluster by id but none is
     *     configured, as a refresh job recorded without it could never be re-attached to.
     */
    public static String getClusterId(String executionTarget, ReadableConfig config) {
        Optional<String> clusterIdKey = getClusterIdKey(executionTarget);
        if (clusterIdKey.isEmpty()) {
            return "";
        }
        String key = clusterIdKey.get();
        return config.getOptional(ConfigOptions.key(key).stringType().noDefaultValue())
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        String.format(
                                                "Execution target '%s' requires '%s' to be configured to submit a materialized table refresh job.",
                                                executionTarget, key)));
    }

    /** Builds the cluster coordinates of a refresh job. */
    public static Map<String, String> buildClusterInfo(String executionTarget, String clusterId) {
        Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(TARGET.key(), executionTarget);
        getClusterIdKey(executionTarget).ifPresent(key -> clusterInfo.put(key, clusterId));
        return clusterInfo;
    }

    /** Returns the configuration key that carries the cluster id for the given execution target. */
    public static Optional<String> getClusterIdKey(String executionTarget) {
        if (executionTarget.startsWith("yarn")) {
            return Optional.of("yarn.application.id");
        } else if (executionTarget.startsWith("kubernetes")) {
            return Optional.of("kubernetes.cluster-id");
        }
        return Optional.empty();
    }

    private MaterializedTableClusterUtils() {}
}

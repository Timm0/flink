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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;

/**
 * Utilities for the execution targets of materialized table refresh jobs and the cluster
 * coordinates recorded for them.
 */
@Internal
public final class MaterializedTableClusterUtils {

    public static final String LOCAL_TARGET = "local";
    public static final String EMBEDDED_TARGET = "embedded";
    public static final String REMOTE_TARGET = "remote";
    public static final String MINICLUSTER_TARGET = "minicluster";

    /** Builds the cluster coordinates of a refresh job. */
    public static Map<String, String> buildClusterInfo(String executionTarget, String clusterId) {
        Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(TARGET.key(), executionTarget);
        getClusterIdKey(executionTarget).ifPresent(key -> clusterInfo.put(key, clusterId));
        return Collections.unmodifiableMap(clusterInfo);
    }

    /** Whether the target deploys each job in its own application cluster. */
    public static boolean isApplicationTarget(String executionTarget) {
        return executionTarget.endsWith("application");
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

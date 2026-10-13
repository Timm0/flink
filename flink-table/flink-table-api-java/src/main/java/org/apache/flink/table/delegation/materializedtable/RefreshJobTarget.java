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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The execution target and cluster id a materialized table refresh job is submitted to. */
@Internal
public final class RefreshJobTarget {

    public static final String YARN_APPLICATION_ID_KEY = "yarn.application.id";
    public static final String KUBERNETES_CLUSTER_ID_KEY = "kubernetes.cluster-id";

    private final String executionTarget;
    private final String clusterId;

    public RefreshJobTarget(String executionTarget, String clusterId) {
        this.executionTarget = checkNotNull(executionTarget);
        this.clusterId = checkNotNull(clusterId);
        checkArgument(!executionTarget.isEmpty(), "The execution target must not be empty.");
    }

    /** The target a refresh handler records. */
    public static RefreshJobTarget recordedIn(ContinuousRefreshHandler refreshHandler) {
        final String clusterId = refreshHandler.getClusterId();
        return new RefreshJobTarget(
                refreshHandler.getExecutionTarget(), clusterId == null ? "" : clusterId);
    }

    public String getExecutionTarget() {
        return executionTarget;
    }

    /**
     * The cluster id, or an empty string when the target does not identify its cluster by id or the
     * id is not yet known.
     */
    public String getClusterId() {
        return clusterId;
    }

    /** The configuration key that carries this target's cluster id, whether or not it is known. */
    public Optional<String> getClusterIdKey() {
        if (executionTarget.startsWith("yarn")) {
            return Optional.of(YARN_APPLICATION_ID_KEY);
        } else if (executionTarget.startsWith("kubernetes")) {
            return Optional.of(KUBERNETES_CLUSTER_ID_KEY);
        }
        return Optional.empty();
    }

    /**
     * The configuration key that identifies the cluster, present only when the target has a
     * cluster-id key and the cluster id is known.
     */
    public Optional<String> identifyingClusterIdKey() {
        if (clusterId.isEmpty()) {
            return Optional.empty();
        }
        return getClusterIdKey();
    }

    /** Whether the target deploys each job in its own application cluster. */
    public boolean isApplicationTarget() {
        return executionTarget.endsWith("application");
    }

    /**
     * The target and, under its cluster-id key, the cluster id, as a REFRESH statement reports
     * them.
     */
    public Map<String, String> toClusterInfo() {
        final Map<String, String> clusterInfo = new HashMap<>();
        clusterInfo.put(DeploymentOptions.TARGET.key(), executionTarget);
        getClusterIdKey().ifPresent(key -> clusterInfo.put(key, clusterId));
        return Collections.unmodifiableMap(clusterInfo);
    }

    public void applyTo(Configuration configuration) {
        configuration.set(DeploymentOptions.TARGET, executionTarget);
        identifyingClusterIdKey().ifPresent(key -> configuration.setString(key, clusterId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RefreshJobTarget that = (RefreshJobTarget) o;
        return executionTarget.equals(that.executionTarget) && clusterId.equals(that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionTarget, clusterId);
    }

    @Override
    public String toString() {
        return "RefreshJobTarget{executionTarget='"
                + executionTarget
                + "', clusterId='"
                + clusterId
                + "'}";
    }
}

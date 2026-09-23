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

import java.util.Map;
import java.util.Objects;

/**
 * The result of submitting a materialized table refresh job, describing where the job was deployed
 * and how to identify it.
 */
@Internal
public final class RefreshJobResult {

    private final String executionTarget;
    private final String clusterId;
    private final String jobId;

    private final Map<String, String> clusterInfo;

    public RefreshJobResult(
            String executionTarget,
            String clusterId,
            String jobId,
            Map<String, String> clusterInfo) {
        this.executionTarget = executionTarget;
        this.clusterId = clusterId;
        this.jobId = jobId;
        this.clusterInfo = Map.copyOf(clusterInfo);
    }

    public String getExecutionTarget() {
        return executionTarget;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getJobId() {
        return jobId;
    }

    public Map<String, String> getClusterInfo() {
        return clusterInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RefreshJobResult that = (RefreshJobResult) o;
        return Objects.equals(executionTarget, that.executionTarget)
                && Objects.equals(clusterId, that.clusterId)
                && Objects.equals(jobId, that.jobId)
                && Objects.equals(clusterInfo, that.clusterInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionTarget, clusterId, jobId, clusterInfo);
    }

    @Override
    public String toString() {
        return "RefreshJobResult{"
                + "executionTarget='"
                + executionTarget
                + '\''
                + ", clusterId='"
                + clusterId
                + '\''
                + ", jobId='"
                + jobId
                + '\''
                + ", clusterInfo="
                + clusterInfo
                + '}';
    }
}

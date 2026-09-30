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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link RefreshJobResult}. */
class RefreshJobResultTest {

    private static final RefreshJobTarget REMOTE = new RefreshJobTarget("remote", "");

    @Test
    void testGetters() {
        final RefreshJobResult result =
                new RefreshJobResult(
                        new RefreshJobTarget("kubernetes-session", "c1"), "job-1", null);

        assertThat(result.getExecutionTarget()).isEqualTo("kubernetes-session");
        assertThat(result.getClusterId()).isEqualTo("c1");
        assertThat(result.getJobId()).isEqualTo("job-1");
    }

    @Test
    void testClusterInfoOfTargetWithoutClusterIdKey() {
        assertThat(new RefreshJobResult(REMOTE, "job-1", null).getClusterInfo())
                .containsOnly(entry("execution.target", "remote"));
    }

    @Test
    void testClusterInfoOfKubernetesTarget() {
        assertThat(clusterInfoOf("kubernetes-session", "c1"))
                .containsOnly(
                        entry("execution.target", "kubernetes-session"),
                        entry("kubernetes.cluster-id", "c1"));
    }

    @Test
    void testClusterInfoOfYarnTarget() {
        assertThat(clusterInfoOf("yarn-application", "application_1_1"))
                .containsOnly(
                        entry("execution.target", "yarn-application"),
                        entry("yarn.application.id", "application_1_1"));
    }

    @Test
    void testToStringContainsFields() {
        final RefreshJobResult result =
                new RefreshJobResult(
                        new RefreshJobTarget("kubernetes-session", "c1"), "job-1", null);

        assertThat(result.toString()).contains("kubernetes-session", "c1", "job-1");
    }

    @Test
    void testJobClientIsOptional() {
        assertThat(new RefreshJobResult(REMOTE, "job-1", null).getJobClient()).isEmpty();
    }

    @Test
    void testJobClientIsExposedButNotPartOfEquality() {
        final JobID jobId = new JobID();
        final JobClient jobClient = new IdOnlyJobClient(jobId);
        final RefreshJobResult withClient =
                new RefreshJobResult(REMOTE, jobId.toHexString(), jobClient);
        final RefreshJobResult withoutClient =
                new RefreshJobResult(REMOTE, jobId.toHexString(), null);

        assertThat(withClient.getJobClient()).containsSame(jobClient);
        assertThat(withClient).isEqualTo(withoutClient).hasSameHashCodeAs(withoutClient);
    }

    @Test
    void testJobClientOfAnotherJobIsRejected() {
        final JobID otherJobId = new JobID();

        assertThatThrownBy(
                        () ->
                                new RefreshJobResult(
                                        REMOTE,
                                        new JobID().toHexString(),
                                        new IdOnlyJobClient(otherJobId)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(otherJobId.toHexString());
    }

    @Test
    void testClusterInfoIsUnmodifiable() {
        final Map<String, String> clusterInfo = clusterInfoOf("kubernetes-session", "c1");

        assertThatThrownBy(() -> clusterInfo.put("rest.address", "somewhere"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static Map<String, String> clusterInfoOf(String executionTarget, String clusterId) {
        return new RefreshJobResult(new RefreshJobTarget(executionTarget, clusterId), "job-1", null)
                .getClusterInfo();
    }

    /** A {@link JobClient} that only knows its job id; it is never asked to control the job. */
    private static final class IdOnlyJobClient implements JobClient {

        private final JobID jobId;

        private IdOnlyJobClient(JobID jobId) {
            this.jobId = jobId;
        }

        @Override
        public JobID getJobID() {
            return jobId;
        }

        @Override
        public CompletableFuture<JobStatus> getJobStatus() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> cancel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> stopWithSavepoint(
                boolean advanceToEndOfEventTime,
                @Nullable String savepointDirectory,
                SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<String> triggerSavepoint(
                @Nullable String savepointDirectory, SavepointFormatType formatType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Map<String, Object>> getAccumulators() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
            throw new UnsupportedOperationException();
        }
    }
}

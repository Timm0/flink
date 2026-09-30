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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobClientRetriever;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RefreshJobController}. */
class RefreshJobControllerTest {

    private final JobID jobId = new JobID();
    private final TableConfig tableConfig = TableConfig.getDefault();

    @Test
    void registeredJobClientIsUsedWithoutRetrieval() {
        RefreshJobController controller =
                controller(
                        configuration -> {
                            throw new AssertionError("A registered job must not be retrieved.");
                        });
        controller.register(submitted(new TestingJobClient(jobId, JobStatus.RUNNING)));

        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void registeredJobClientOnlyControlsItsOwnJob() {
        final JobID otherJobId = new JobID();
        final CapturingRetriever retriever =
                new CapturingRetriever(new TestingJobClient(otherJobId, JobStatus.FINISHED));
        final RefreshJobController controller = controller(retriever);
        controller.register(submitted(new TestingJobClient(jobId, JobStatus.RUNNING)));

        assertThat(
                        controller.getJobStatus(
                                new ContinuousRefreshHandler(
                                        "remote", "", otherJobId.toHexString())))
                .isEqualTo(JobStatus.FINISHED);
        assertThat(retriever.retrievedJobId).isEqualTo(otherJobId);
    }

    @Test
    void globallyTerminalStatusEvictsTheSubmittedJobClient() {
        final CapturingRetriever retriever =
                new CapturingRetriever(new TestingJobClient(jobId, JobStatus.FINISHED));
        final RefreshJobController controller = controller(retriever);
        controller.register(submitted(new TestingJobClient(jobId, JobStatus.FAILED)));

        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.FAILED);
        assertThat(retriever.retrievedJobId).isNull();

        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.FINISHED);
        assertThat(retriever.retrievedJobId).isEqualTo(jobId);
    }

    @Test
    void locallyTerminalStatusKeepsTheSubmittedJobClient() {
        final RefreshJobController controller =
                controller(
                        configuration -> {
                            throw new AssertionError("A registered job must not be retrieved.");
                        });
        controller.register(submitted(new TestingJobClient(jobId, JobStatus.SUSPENDED)));

        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.SUSPENDED);
        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.SUSPENDED);
    }

    @Test
    void unregisteredJobIsRetrievedWithTheRecordedCoordinates() {
        Configuration root = new Configuration();
        root.setString("kubernetes.namespace", "mt");
        tableConfig.setRootConfiguration(root);
        final CapturingRetriever retriever = runningJobRetriever();
        final AtomicReference<Map<String, String>> lookupConfiguration = new AtomicReference<>();

        final JobStatus jobStatus =
                controller(
                                configuration -> {
                                    lookupConfiguration.set(configuration.toMap());
                                    return Optional.of(retriever);
                                })
                        .getJobStatus(handler("kubernetes-session", "mt-refresh"));

        assertThat(jobStatus).isEqualTo(JobStatus.RUNNING);
        assertThat(lookupConfiguration.get())
                .containsEntry("execution.target", "kubernetes-session")
                .containsEntry("kubernetes.cluster-id", "mt-refresh");
        assertThat(retriever.retrievedJobId).isEqualTo(jobId);
        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("execution.target", "kubernetes-session")
                .containsEntry("kubernetes.cluster-id", "mt-refresh")
                .containsEntry("kubernetes.namespace", "mt");
    }

    @Test
    void configurationSetAfterCreationIsUsed() {
        final CapturingRetriever retriever = runningJobRetriever();
        final RefreshJobController controller = controller(retriever);

        tableConfig.set("rest.address", "late-host");
        controller.getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "late-host");
    }

    @Test
    void legacyStandaloneClusterIdIsIgnored() {
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler("remote", "StandaloneClusterId"));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("execution.target", "remote")
                .doesNotContainValue("StandaloneClusterId");
    }

    @Test
    void applicationTargetHandlerIsReachable() {
        Configuration root = new Configuration();
        root.setString("execution.target", "remote");
        root.setString("kubernetes.cluster-id", "other");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "remote");
        tableConfig.set("kubernetes.cluster-id", "other");
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler("kubernetes-application", "app-cluster"));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("execution.target", "kubernetes-application")
                .containsEntry("kubernetes.cluster-id", "app-cluster");
    }

    @Test
    void tableConfigOverridesTheRootConfigurationForRetrieval() {
        Configuration root = new Configuration();
        root.setString("rest.address", "root-host");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("rest.address", "table-config-host");
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "table-config-host");
    }

    @Test
    void tableConfigCoordinatesArePinnedForRetrievalWhenItSetsTheTarget() {
        final Configuration root = new Configuration();
        root.setString("rest.address", "program-cluster");
        root.setString("high-availability.type", "zookeeper");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "remote");
        tableConfig.set("jobmanager.rpc.address", "refresh-cluster");
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "refresh-cluster")
                .containsEntry("high-availability.type", "NONE");
    }

    @Test
    void rootCoordinatesAreKeptForRetrievalWhenTheTableConfigSetsNoTarget() {
        final Configuration root = new Configuration();
        root.setString("execution.target", "remote");
        root.setString("rest.address", "session-cluster");
        root.setString("high-availability.type", "zookeeper");
        tableConfig.setRootConfiguration(root);
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "session-cluster")
                .containsEntry("high-availability.type", "zookeeper");
    }

    @Test
    void retrievalKeepsTheMergedRestAddressWhenTheTableConfigSetsNone() {
        final Configuration root = new Configuration();
        root.setString("rest.address", "program-cluster");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "kubernetes-session");
        final CapturingRetriever retriever = runningJobRetriever();

        final JobStatus jobStatus = controller(retriever).getJobStatus(handler("remote", ""));

        assertThat(jobStatus).isEqualTo(JobStatus.RUNNING);
        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "program-cluster")
                .containsEntry("high-availability.type", "NONE");
    }

    @Test
    void legacyHandlerWithoutClusterIdIsReachable() {
        final CapturingRetriever retriever = runningJobRetriever();

        final JobStatus jobStatus = controller(retriever).getJobStatus(handler("remote", null));

        assertThat(jobStatus).isEqualTo(JobStatus.RUNNING);
        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("execution.target", "remote")
                .doesNotContainKeys("kubernetes.cluster-id", "yarn.application.id");
    }

    @Test
    void failingRetrieverLookupNamesTheRecordedCoordinates() {
        assertThatThrownBy(
                        () ->
                                controller(
                                                configuration -> {
                                                    throw new IllegalStateException(
                                                            "Multiple compatible job client retrievers found.");
                                                })
                                        .getJobStatus(handler("kubernetes-session", "mt-refresh")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("kubernetes-session")
                .hasMessageContaining("mt-refresh")
                .hasMessageContaining("'rest.address'")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void failureOfALegacyHandlerShowsAnEmptyClusterId() {
        final CompletableFuture<JobStatus> jobStatus =
                FutureUtils.completedExceptionally(new RuntimeException("Job not found."));

        assertThatThrownBy(
                        () -> controllerAwaiting(jobStatus).getJobStatus(handler("remote", null)))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("execution.target=remote")
                .hasMessageNotContaining("cluster id=null");
    }

    @Test
    void noCompatibleRetrieverFailsClosed() {
        assertThatThrownBy(
                        () ->
                                controller(configuration -> Optional.empty())
                                        .getJobStatus(handler("minicluster", "")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("'minicluster'")
                .hasMessageContaining("missing from this program's class path")
                .hasMessageContaining("more than one ClusterClientFactory")
                .hasMessageContaining("TableEnvironment that submitted them");
    }

    @Test
    void failedRequestNamesTheRecordedCoordinates() {
        final CompletableFuture<JobStatus> jobStatus =
                FutureUtils.completedExceptionally(new RuntimeException("Job not found."));

        assertThatThrownBy(
                        () ->
                                controllerAwaiting(jobStatus)
                                        .getJobStatus(handler("kubernetes-session", "mt-refresh")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("kubernetes-session")
                .hasMessageContaining("mt-refresh")
                .hasMessageContaining("'rest.address'");
    }

    @Test
    void requestFailingBeforeItsFutureExistsNamesTheRecordedCoordinates() {
        final RuntimeException unreachable =
                new RuntimeException("Couldn't retrieve standalone cluster");
        final JobClient jobClient =
                new TestingJobClient(jobId, JobStatus.RUNNING)
                        .withJobStatusRequest(
                                () -> {
                                    throw unreachable;
                                });

        assertThatThrownBy(() -> controllerReaching(jobClient).getJobStatus(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("execution.target=remote")
                .hasMessageContaining("'rest.address'")
                .hasCauseReference(unreachable);
    }

    @Test
    void failureOfTheSubmittedJobClientDoesNotBlameTheConfiguredCoordinates() {
        final RefreshJobController controller =
                controller(
                        configuration -> {
                            throw new AssertionError("A registered job must not be retrieved.");
                        });
        controller.register(
                submitted(
                        new TestingJobClient(
                                jobId,
                                FutureUtils.completedExceptionally(
                                        new RuntimeException("Job failed.")))));

        assertThatThrownBy(() -> controller.getJobStatus(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(jobId.toHexString())
                .hasMessageNotContaining("'rest.address'");
    }

    @Test
    void timeoutOfTheSubmittedJobClientDoesNotBlameTheConfiguredCoordinates() {
        tableConfig.set("client.timeout", "10 ms");
        final RefreshJobController controller =
                controller(
                        configuration -> {
                            throw new AssertionError("A registered job must not be retrieved.");
                        });
        controller.register(submitted(new TestingJobClient(jobId, new TimeoutRecordingFuture<>())));

        assertThatThrownBy(() -> controller.getJobStatus(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("10 ms ('client.timeout')")
                .hasMessageNotContaining("'rest.address'");
    }

    @Test
    void statusWaitIsBoundedByClientTimeout() {
        tableConfig.set("client.timeout", "10 ms");
        tableConfig.set("execution.checkpointing.timeout", "20 ms");
        final TimeoutRecordingFuture<JobStatus> jobStatus = new TimeoutRecordingFuture<>();

        assertThatThrownBy(() -> controllerAwaiting(jobStatus).getJobStatus(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("10 ms ('client.timeout')")
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("'rest.address'")
                .hasMessageNotContaining("may still complete");
        assertThat(jobStatus.requestedTimeout).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    void cancelWaitIsBoundedByClientTimeout() {
        tableConfig.set("client.timeout", "10 ms");
        tableConfig.set("execution.checkpointing.timeout", "20 ms");
        final TimeoutRecordingFuture<Void> cancellation = new TimeoutRecordingFuture<>();
        final JobClient jobClient =
                new TestingJobClient(jobId, JobStatus.RUNNING)
                        .withCancelRequest(() -> cancellation);

        assertThatThrownBy(() -> controllerReaching(jobClient).cancel(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("10 ms ('client.timeout')")
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("'rest.address'")
                .hasMessageContaining("may still complete on the cluster");
        assertThat(cancellation.requestedTimeout).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    void failedCancelMayStillComplete() {
        final JobClient jobClient =
                new TestingJobClient(jobId, JobStatus.RUNNING).withFailingCancel();

        assertThatThrownBy(() -> controllerReaching(jobClient).cancel(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("may still complete on the cluster");
    }

    @Test
    void stopWithSavepointWaitIsBoundedByClientAndCheckpointTimeouts() {
        tableConfig.set("client.timeout", "10 ms");
        tableConfig.set("execution.checkpointing.timeout", "20 ms");
        final TimeoutRecordingFuture<String> savepointPath = new TimeoutRecordingFuture<>();

        assertThatThrownBy(
                        () ->
                                controllerStopping(savepointPath)
                                        .stopWithSavepoint(
                                                handler("remote", ""), "file:///savepoints"))
                .isInstanceOf(TableException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining(
                        "30 ms ('client.timeout' + 'execution.checkpointing.timeout')")
                .hasMessageContaining(jobId.toHexString())
                .hasMessageContaining("'rest.address'")
                .hasMessageContaining("may still complete on the cluster");
        assertThat(savepointPath.requestedTimeout).isEqualTo(Duration.ofMillis(30));
    }

    @Test
    void stopTimeoutExplainsHowToResumeFromTheSavepoint() {
        assertThatThrownBy(
                        () ->
                                controllerStopping(new TimeoutRecordingFuture<>())
                                        .stopWithSavepoint(
                                                handler("remote", ""), "file:///savepoints/mt"))
                .isInstanceOf(TableException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("this program's 'execution.checkpointing.timeout'")
                .hasMessageContaining("'file:///savepoints/mt'")
                .hasMessageContaining("stays ACTIVATED")
                .hasMessageContaining("'execution.state-recovery.path'")
                .hasMessageContaining("and unset it afterwards (RESET in SQL)");
    }

    @Test
    void failedStopExplainsHowToResumeFromTheSavepoint() {
        final CompletableFuture<String> savepointPath =
                FutureUtils.completedExceptionally(new RuntimeException("Connection refused"));

        assertThatThrownBy(
                        () ->
                                controllerStopping(savepointPath)
                                        .stopWithSavepoint(
                                                handler("remote", ""), "file:///savepoints/mt"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("may still complete on the cluster")
                .hasMessageContaining("'file:///savepoints/mt'")
                .hasMessageContaining("'execution.state-recovery.path'")
                .hasMessageNotContaining("'execution.checkpointing.timeout'");
    }

    @Test
    void deprecatedClientTimeoutKeyIsHonoured() {
        tableConfig.set("akka.client.timeout", "10 ms");
        final TimeoutRecordingFuture<JobStatus> jobStatus = new TimeoutRecordingFuture<>();

        assertThatThrownBy(() -> controllerAwaiting(jobStatus).getJobStatus(handler("remote", "")))
                .isInstanceOf(TableException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("'client.timeout'")
                .hasMessageContaining("10 ms")
                .hasMessageContaining(jobId.toHexString());
        assertThat(jobStatus.requestedTimeout).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    void clientTimeoutStaysInSyncWithClientOptions() {
        assertThat(RefreshJobController.CLIENT_TIMEOUT.key())
                .isEqualTo(ClientOptions.CLIENT_TIMEOUT.key());
        assertThat(RefreshJobController.CLIENT_TIMEOUT.defaultValue())
                .isEqualTo(ClientOptions.CLIENT_TIMEOUT.defaultValue());
        assertThat(RefreshJobController.CLIENT_TIMEOUT.fallbackKeys())
                .containsExactlyElementsOf(ClientOptions.CLIENT_TIMEOUT.fallbackKeys());
    }

    @Test
    void interruptedWaitRestoresTheInterruptFlag() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    controllerAwaiting(new CompletableFuture<>())
                                            .getJobStatus(handler("remote", "")))
                    .isInstanceOf(TableException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptedStopMayStillComplete() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    controllerStopping(new CompletableFuture<>())
                                            .stopWithSavepoint(
                                                    handler("remote", ""), "file:///savepoints/mt"))
                    .isInstanceOf(TableException.class)
                    .hasCauseInstanceOf(InterruptedException.class)
                    .hasMessageContaining("may still complete on the cluster")
                    .hasMessageContaining("'file:///savepoints/mt'")
                    .hasMessageContaining("'execution.state-recovery.path'")
                    .hasMessageNotContaining("'execution.checkpointing.timeout'");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void stopThatWasNeverSentIsNotReportedAsPending() {
        final RuntimeException unreachable =
                new RuntimeException("Couldn't retrieve standalone cluster");
        final JobClient jobClient =
                new TestingJobClient(jobId, JobStatus.RUNNING)
                        .withStopRequest(
                                savepointDirectory -> {
                                    throw unreachable;
                                });

        assertThatThrownBy(
                        () ->
                                controllerReaching(jobClient)
                                        .stopWithSavepoint(
                                                handler("remote", ""), "file:///savepoints/mt"))
                .isInstanceOf(TableException.class)
                .hasCauseReference(unreachable)
                .hasMessageContaining("'rest.address'")
                .hasMessageNotContaining("may still complete")
                .hasMessageNotContaining("'execution.state-recovery.path'");
    }

    @Test
    void stopWithSavepointEvictsTheSubmittedJobClient() {
        final CapturingRetriever retriever =
                new CapturingRetriever(new TestingJobClient(jobId, JobStatus.FINISHED));
        final RefreshJobController controller = controller(retriever);
        controller.register(submitted(new TestingJobClient(jobId, JobStatus.RUNNING)));

        assertThat(controller.stopWithSavepoint(handler("remote", ""), "file:///savepoints"))
                .isEqualTo("file:///savepoints/savepoint-1");
        assertThat(retriever.retrievedJobId).isNull();

        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.FINISHED);
        assertThat(retriever.retrievedJobId).isEqualTo(jobId);
    }

    @Test
    void cancelEvictsTheSubmittedJobClient() {
        final CapturingRetriever retriever =
                new CapturingRetriever(new TestingJobClient(jobId, JobStatus.CANCELED));
        final RefreshJobController controller = controller(retriever);
        controller.register(submitted(new TestingJobClient(jobId, JobStatus.RUNNING)));

        controller.cancel(handler("remote", ""));

        assertThat(controller.getJobStatus(handler("remote", ""))).isEqualTo(JobStatus.CANCELED);
        assertThat(retriever.retrievedJobId).isEqualTo(jobId);
    }

    @Test
    void retrievalRejectsARemoteTargetOnTableConfigWithoutRestAddress() {
        final Configuration root = new Configuration();
        root.setString("rest.address", "program-cluster");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "remote");

        assertThatThrownBy(
                        () ->
                                controller(
                                                configuration -> {
                                                    throw new AssertionError(
                                                            "A misconfigured job must not be retrieved.");
                                                })
                                        .getJobStatus(handler("remote", "")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Execution target 'remote' requires 'rest.address' to be configured");
    }

    @Test
    void retrievalRejectsHighAvailabilityOnTableConfigWithTheRootClusterIdOnly() {
        final Configuration root = new Configuration();
        root.setString("high-availability.cluster-id", "/program-cluster");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "remote");
        tableConfig.set("high-availability.type", "zookeeper");

        assertThatThrownBy(
                        () ->
                                controller(
                                                configuration -> {
                                                    throw new AssertionError(
                                                            "A misconfigured job must not be retrieved.");
                                                })
                                        .getJobStatus(handler("remote", "")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'high-availability.cluster-id' must be set there too");
    }

    @Test
    void restPortOnTableConfigIsPinnedForRetrieval() {
        final Configuration root = new Configuration();
        root.setString("rest.address", "program-cluster");
        root.setString("rest.port", "8081");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "remote");
        tableConfig.set("rest.address", "refresh-cluster");
        tableConfig.set("rest.port", "9091");
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "refresh-cluster")
                .containsEntry("rest.port", "9091");
    }

    @Test
    void retrievedJobClientIsReusedWithinOneOperationAndClosedAtItsEnd() {
        final CloseableTestingJobClient jobClient =
                new CloseableTestingJobClient(jobId, JobStatus.RUNNING);
        final CapturingRetriever retriever = new CapturingRetriever(jobClient);
        final RefreshJobController controller = controller(retriever);

        controller.getJobStatus(handler("remote", ""));
        controller.cancel(handler("remote", ""));
        controller.getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievals).isEqualTo(1);
        assertThat(jobClient.isCancelRequested()).isTrue();
        assertThat(jobClient.closed).isFalse();

        controller.releaseRetrievedJobClients();

        assertThat(jobClient.closed).isTrue();
        controller.getJobStatus(handler("remote", ""));
        assertThat(retriever.retrievals).isEqualTo(2);
    }

    private RefreshJobController controller(
            Function<Configuration, Optional<JobClientRetriever>> retrieverLookup) {
        return new RefreshJobController(tableConfig, getClass().getClassLoader(), retrieverLookup);
    }

    private RefreshJobController controller(CapturingRetriever retriever) {
        return controller(configuration -> Optional.of(retriever));
    }

    private RefreshJobController controllerAwaiting(CompletableFuture<JobStatus> jobStatus) {
        return controllerReaching(new TestingJobClient(jobId, jobStatus));
    }

    private RefreshJobController controllerStopping(CompletableFuture<String> savepointPath) {
        return controllerReaching(
                new TestingJobClient(jobId, JobStatus.RUNNING)
                        .withStopRequest(savepointDirectory -> savepointPath));
    }

    private RefreshJobController controllerReaching(JobClient jobClient) {
        return controller(new CapturingRetriever(jobClient));
    }

    private CapturingRetriever runningJobRetriever() {
        return new CapturingRetriever(new TestingJobClient(jobId, JobStatus.RUNNING));
    }

    private ContinuousRefreshHandler handler(String executionTarget, @Nullable String clusterId) {
        return new ContinuousRefreshHandler(executionTarget, clusterId, jobId.toHexString());
    }

    private RefreshJobResult submitted(JobClient jobClient) {
        return new RefreshJobResult(
                new RefreshJobTarget("remote", ""), jobId.toHexString(), jobClient);
    }

    private static final class CapturingRetriever implements JobClientRetriever {

        private final JobClient jobClient;
        private Configuration retrievalConfiguration;
        private JobID retrievedJobId;
        private int retrievals;

        private CapturingRetriever(JobClient jobClient) {
            this.jobClient = jobClient;
        }

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return true;
        }

        @Override
        public JobClient retrieveJobClient(
                JobID jobId, Configuration configuration, ClassLoader userCodeClassLoader) {
            this.retrievedJobId = jobId;
            this.retrievalConfiguration = configuration;
            retrievals++;
            return jobClient;
        }
    }

    private static final class CloseableTestingJobClient extends TestingJobClient
            implements AutoCloseable {

        private boolean closed;

        private CloseableTestingJobClient(JobID jobId, JobStatus jobStatus) {
            super(jobId, jobStatus);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class TimeoutRecordingFuture<T> extends CompletableFuture<T> {

        private Duration requestedTimeout;

        @Override
        public T get(long timeout, TimeUnit unit) throws TimeoutException {
            requestedTimeout = Duration.ofNanos(unit.toNanos(timeout));
            throw new TimeoutException();
        }

        @Override
        public T get() {
            throw new AssertionError("unbounded wait");
        }

        @Override
        public T join() {
            throw new AssertionError("unbounded wait");
        }
    }
}

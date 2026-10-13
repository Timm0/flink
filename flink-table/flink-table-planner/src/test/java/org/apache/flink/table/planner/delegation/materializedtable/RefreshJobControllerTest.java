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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

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
        final Configuration root = new Configuration();
        root.setString("high-availability.type", "zookeeper");
        tableConfig.setRootConfiguration(root);
        final CapturingRetriever retriever = runningJobRetriever();
        final RefreshJobController controller = controller(retriever);

        tableConfig.set("execution.target", "remote");
        tableConfig.set("rest.address", "late-host");
        controller.getJobStatus(handler("remote", ""));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsEntry("rest.address", "late-host")
                .doesNotContainKey("high-availability.type");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("retrievalConfigurations")
    void unregisteredJobIsRetrievedWithTheRecordedTargetOverTheProgramsLayers(
            String description,
            Map<String, String> rootOptions,
            Map<String, String> tableConfigOptions,
            String recordedTarget,
            @Nullable String recordedClusterId,
            Map<String, String> expectedEntries,
            List<String> absentKeys) {
        tableConfig.setRootConfiguration(Configuration.fromMap(rootOptions));
        tableConfigOptions.forEach(tableConfig::set);
        final CapturingRetriever retriever = runningJobRetriever();

        controller(retriever).getJobStatus(handler(recordedTarget, recordedClusterId));

        assertThat(retriever.retrievalConfiguration.toMap())
                .containsAllEntriesOf(expectedEntries)
                .doesNotContainKeys(absentKeys.toArray(new String[0]));
    }

    private static Stream<Arguments> retrievalConfigurations() {
        final List<String> clusterIdKeys = List.of("kubernetes.cluster-id", "yarn.application.id");
        return Stream.of(
                Arguments.of(
                        "old StandaloneClusterId of a remote job",
                        Map.of(),
                        Map.of(),
                        "remote",
                        "StandaloneClusterId",
                        Map.of("execution.target", "remote"),
                        clusterIdKeys),
                Arguments.of(
                        "session handler without a cluster id",
                        Map.of(),
                        Map.of(
                                "execution.target", "kubernetes-session",
                                "kubernetes.cluster-id", "program-cluster"),
                        "kubernetes-session",
                        null,
                        Map.of(
                                "execution.target", "kubernetes-session",
                                "kubernetes.cluster-id", "program-cluster"),
                        List.of()),
                Arguments.of(
                        "application target handler",
                        Map.of("execution.target", "remote", "kubernetes.cluster-id", "other"),
                        Map.of("execution.target", "remote", "kubernetes.cluster-id", "other"),
                        "kubernetes-application",
                        "app-cluster",
                        Map.of(
                                "execution.target", "kubernetes-application",
                                "kubernetes.cluster-id", "app-cluster"),
                        List.of()),
                Arguments.of(
                        "no target on the TableConfig",
                        Map.of(
                                "execution.target", "remote",
                                "rest.address", "root-host",
                                "high-availability.type", "zookeeper"),
                        Map.of("rest.address", "table-config-host"),
                        "remote",
                        "",
                        Map.of(
                                "rest.address", "table-config-host",
                                "high-availability.type", "zookeeper"),
                        List.of()),
                Arguments.of(
                        "target on the TableConfig strips the root's cluster options",
                        Map.of(
                                "rest.address", "program-cluster",
                                "jobmanager.rpc.address", "program-jobmanager",
                                "high-availability.type", "zookeeper",
                                "high-availability.cluster-id", "/program-cluster",
                                "high-availability", "zookeeper",
                                "recovery.mode", "zookeeper",
                                "kubernetes.cluster-id", "program-cluster",
                                "yarn.application.id", "application_1_0001"),
                        Map.of("execution.target", "remote", "rest.address", "refresh-cluster"),
                        "remote",
                        "",
                        Map.of("rest.address", "refresh-cluster"),
                        List.of(
                                "jobmanager.rpc.address",
                                "high-availability.type",
                                "high-availability.cluster-id",
                                "high-availability",
                                "recovery.mode",
                                "kubernetes.cluster-id",
                                "yarn.application.id")),
                Arguments.of(
                        "target on the TableConfig keeps the root's other options",
                        Map.of(
                                "rest.address", "program-cluster",
                                "rest.port", "8082",
                                "kubernetes.namespace", "flink",
                                "kubernetes.cluster-id", "program-cluster"),
                        Map.of(
                                "execution.target", "kubernetes-session",
                                "kubernetes.cluster-id", "mt-refresh"),
                        "kubernetes-session",
                        "mt-refresh",
                        Map.of(
                                "rest.port", "8082",
                                "kubernetes.namespace", "flink",
                                "kubernetes.cluster-id", "mt-refresh"),
                        List.of("rest.address")),
                Arguments.of(
                        "deprecated high-availability keys on the TableConfig",
                        Map.of(
                                "high-availability.type", "zookeeper",
                                "high-availability.cluster-id", "/program-cluster"),
                        Map.of(
                                "execution.target", "remote",
                                "rest.address", "refresh-cluster",
                                "high-availability", "zookeeper",
                                "high-availability.zookeeper.path.namespace", "/refresh-cluster"),
                        "remote",
                        "",
                        Map.of(
                                "high-availability", "zookeeper",
                                "high-availability.zookeeper.path.namespace", "/refresh-cluster"),
                        List.of("high-availability.type", "high-availability.cluster-id")),
                Arguments.of(
                        "another target on the TableConfig, remote job",
                        Map.of("rest.address", "program-cluster"),
                        Map.of(
                                "execution.target", "kubernetes-session",
                                "rest.address", "standalone-cluster"),
                        "remote",
                        "",
                        Map.of("execution.target", "remote", "rest.address", "standalone-cluster"),
                        List.of()));
    }

    @Test
    void retrievalOfARemoteJobRequiresARestAddressOnATableConfigThatSetsAnotherTarget() {
        final Configuration root = new Configuration();
        root.setString("rest.address", "program-cluster");
        tableConfig.setRootConfiguration(root);
        tableConfig.set("execution.target", "kubernetes-session");

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
                .hasMessageContaining("(execution.target=remote, cluster id=)");
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
        return new RefreshJobController(
                tableConfig,
                new RefreshJobTargetResolver(tableConfig, false),
                getClass().getClassLoader(),
                retrieverLookup);
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

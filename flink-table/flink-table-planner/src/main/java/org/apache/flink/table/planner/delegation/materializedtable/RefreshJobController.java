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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobClientRetriever;
import org.apache.flink.core.execution.JobClientRetrieverLoader;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.materializedtable.RefreshJobResult;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTargetResolver;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.util.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Controls the continuous refresh jobs of materialized tables.
 *
 * <p>Only the requests sent through a client are bounded: a stop with savepoint waits at most
 * {@code client.timeout} plus {@code execution.checkpointing.timeout}, any other request {@code
 * client.timeout}. Retrieving the client is not bounded by them and may block longer, for example
 * while a YARN or Kubernetes client retries reaching its resource manager.
 */
@Internal
final class RefreshJobController {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshJobController.class);

    static final ConfigOption<Duration> CLIENT_TIMEOUT =
            ConfigOptions.key("client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDeprecatedKeys("akka.client.timeout");

    private static final String CLIENT_TIMEOUT_OPTIONS = "'client.timeout'";

    private static final String STOP_WITH_SAVEPOINT_TIMEOUT_OPTIONS =
            "'client.timeout' + 'execution.checkpointing.timeout'";

    private static final String CONFIGURED_COORDINATES_HINT =
            "The job was looked up with coordinates the catalog does not record, such as "
                    + "'rest.address', 'kubernetes.namespace' and high-availability settings, "
                    + "which are taken from this program's configuration (the REST address and the "
                    + "high-availability type and cluster id from its TableConfig alone when "
                    + "'execution.target' is set there; 'rest.port' and the other REST client "
                    + "options from its TableConfig if set there, otherwise from its merged "
                    + "configuration) and must point at the cluster the job runs on.";

    private static final String REQUEST_MAY_STILL_COMPLETE =
            "The request may still complete on the cluster, so check the status of the refresh "
                    + "job before retrying.";

    private static final String SAVEPOINT_WAIT_HINT =
            "The wait allows this program's 'execution.checkpointing.timeout' for the savepoint, "
                    + "which may be lower than the refresh job's own.";

    private static final String SAVEPOINT_RECOVERY_HINT =
            "If the job still stops, its savepoint is written under '%s' while the materialized "
                    + "table stays ACTIVATED; set 'execution.state-recovery.path' to that savepoint "
                    + "before resuming, and unset it afterwards (RESET in SQL), otherwise RESUME "
                    + "starts the refresh job without restoring state.";

    private final TableConfig tableConfig;
    private final ClassLoader userClassLoader;
    private final Function<Configuration, Optional<JobClientRetriever>> retrieverLookup;

    /** Keyed by job id. */
    private final Map<String, JobClient> submittedJobClients = new HashMap<>();

    /** Keyed by job id; only for the current operation, see {@link #releaseRetrievedJobClients}. */
    private final Map<String, JobClient> retrievedJobClients = new HashMap<>();

    RefreshJobController(TableConfig tableConfig, ClassLoader userClassLoader) {
        this(tableConfig, userClassLoader, JobClientRetrieverLoader::findCompatible);
    }

    @VisibleForTesting
    RefreshJobController(
            TableConfig tableConfig,
            ClassLoader userClassLoader,
            Function<Configuration, Optional<JobClientRetriever>> retrieverLookup) {
        this.tableConfig = checkNotNull(tableConfig);
        this.userClassLoader = checkNotNull(userClassLoader);
        this.retrieverLookup = checkNotNull(retrieverLookup);
    }

    /** Remembers the client of a job this program submitted. */
    void register(RefreshJobResult result) {
        result.getJobClient()
                .ifPresent(jobClient -> submittedJobClients.put(result.getJobId(), jobClient));
    }

    JobStatus getJobStatus(ContinuousRefreshHandler refreshHandler) {
        final JobStatus jobStatus =
                await(refreshHandler, JobRequest.GET_STATUS, JobClient::getJobStatus, "");
        if (jobStatus.isGloballyTerminalState()) {
            submittedJobClients.remove(refreshHandler.getJobId());
        }
        return jobStatus;
    }

    void cancel(ContinuousRefreshHandler refreshHandler) {
        await(refreshHandler, JobRequest.CANCEL, JobClient::cancel, "");
        submittedJobClients.remove(refreshHandler.getJobId());
    }

    String stopWithSavepoint(ContinuousRefreshHandler refreshHandler, String savepointDirectory) {
        final String savepointPath =
                await(
                        refreshHandler,
                        JobRequest.STOP_WITH_SAVEPOINT,
                        jobClient ->
                                jobClient.stopWithSavepoint(
                                        false, savepointDirectory, SavepointFormatType.DEFAULT),
                        String.format(SAVEPOINT_RECOVERY_HINT, savepointDirectory));
        submittedJobClients.remove(refreshHandler.getJobId());
        return savepointPath;
    }

    /**
     * Releases the clients retrieved during the current operation, closing those that hold
     * resources.
     */
    void releaseRetrievedJobClients() {
        for (JobClient jobClient : retrievedJobClients.values()) {
            if (jobClient instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) jobClient).close();
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to close the client of refresh job {}.",
                            jobClient.getJobID(),
                            e);
                }
            }
        }
        retrievedJobClients.clear();
    }

    private JobClient lookup(ContinuousRefreshHandler refreshHandler) {
        final JobClient submittedJobClient = submittedJobClients.get(refreshHandler.getJobId());
        if (submittedJobClient != null) {
            return submittedJobClient;
        }
        final JobClient cachedJobClient = retrievedJobClients.get(refreshHandler.getJobId());
        if (cachedJobClient != null) {
            return cachedJobClient;
        }

        RefreshJobTargetResolver.validateUnrecordedCoordinates(
                tableConfig, refreshHandler.getExecutionTarget());
        final Optional<JobClient> retrievedJobClient;
        try {
            retrievedJobClient = retrieve(refreshHandler);
        } catch (Exception e) {
            throw new TableException(
                    withHints(
                            failureMessage("reach refresh job %s", refreshHandler),
                            CONFIGURED_COORDINATES_HINT),
                    e);
        }
        final JobClient jobClient =
                retrievedJobClient.orElseThrow(() -> noCompatibleRetriever(refreshHandler));
        retrievedJobClients.put(refreshHandler.getJobId(), jobClient);
        return jobClient;
    }

    /** Empty when no job client retriever supports the recorded execution target. */
    private Optional<JobClient> retrieve(ContinuousRefreshHandler refreshHandler) throws Exception {
        final Configuration retrievalConfiguration = programConfiguration();
        RefreshJobTargetResolver.recordedTarget(refreshHandler).applyTo(retrievalConfiguration);
        RefreshJobTargetResolver.pinUnrecordedCoordinates(
                tableConfig, refreshHandler.getExecutionTarget(), retrievalConfiguration);
        final Optional<JobClientRetriever> retriever =
                retrieverLookup.apply(retrievalConfiguration);
        if (retriever.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                retriever
                        .get()
                        .retrieveJobClient(
                                JobID.fromHexString(refreshHandler.getJobId()),
                                retrievalConfiguration,
                                userClassLoader));
    }

    /** Read on every retrieval, so options set after this controller was created still apply. */
    private Configuration programConfiguration() {
        final Configuration configuration =
                Configuration.fromMap(tableConfig.getRootConfiguration().toMap());
        configuration.addAll(tableConfig.getConfiguration());
        return configuration;
    }

    /** Sends a request to the job and waits for its result. */
    private <T> T await(
            ContinuousRefreshHandler refreshHandler,
            JobRequest jobRequest,
            Function<JobClient, CompletableFuture<T>> request,
            String recoveryHint) {
        final String action = jobRequest.action;
        final String coordinatesHint =
                submittedJobClients.containsKey(refreshHandler.getJobId())
                        ? ""
                        : CONFIGURED_COORDINATES_HINT;
        final JobClient jobClient = lookup(refreshHandler);

        final CompletableFuture<T> response;
        try {
            response = request.apply(jobClient);
        } catch (Exception e) {
            throw new TableException(
                    withHints(failureMessage(action, refreshHandler), coordinatesHint), e);
        }

        final String pendingRequestHint =
                jobRequest.mayStillComplete
                        ? withHints(REQUEST_MAY_STILL_COMPLETE, recoveryHint)
                        : "";
        final Duration timeout = jobRequest.timeout(tableConfig);
        try {
            return response.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new TableException(
                    withHints(
                            timeoutMessage(
                                    action, refreshHandler, timeout, jobRequest.timeoutOptions),
                            jobRequest.timeoutHint,
                            coordinatesHint,
                            pendingRequestHint),
                    e);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new TableException(
                    withHints(
                            failureMessage(action, refreshHandler),
                            coordinatesHint,
                            pendingRequestHint),
                    e);
        }
    }

    private static String failureMessage(String action, ContinuousRefreshHandler refreshHandler) {
        return String.format("Failed to %s.", describe(action, refreshHandler));
    }

    private static TableException noCompatibleRetriever(ContinuousRefreshHandler refreshHandler) {
        return new TableException(
                String.format(
                        "Cannot reach refresh job %s: no job client retriever supports its "
                                + "execution target '%s'. Either the client classes for this "
                                + "target are missing from this program's class path "
                                + "(flink-clients, plus flink-yarn with HADOOP_CLASSPATH or "
                                + "flink-kubernetes), more than one ClusterClientFactory on the "
                                + "class path supports the target (logged by "
                                + "ClusterClientJobClientRetriever), or jobs on this target can "
                                + "only be controlled from the TableEnvironment that submitted "
                                + "them.",
                        refreshHandler.getJobId(), refreshHandler.getExecutionTarget()));
    }

    private static String timeoutMessage(
            String action,
            ContinuousRefreshHandler refreshHandler,
            Duration timeout,
            String timeoutOptions) {
        return String.format(
                "Timed out after %s (%s) waiting to %s.",
                TimeUtils.formatWithHighestUnit(timeout),
                timeoutOptions,
                describe(action, refreshHandler));
    }

    /** Appends each non-empty hint, separated by a space. */
    private static String withHints(String message, String... hints) {
        final StringBuilder messageWithHints = new StringBuilder(message);
        for (String hint : hints) {
            if (!hint.isEmpty()) {
                messageWithHints.append(' ').append(hint);
            }
        }
        return messageWithHints.toString();
    }

    private static String describe(String action, ContinuousRefreshHandler refreshHandler) {
        return String.format(
                "%s (execution.target=%s, cluster id=%s)",
                String.format(action, refreshHandler.getJobId()),
                refreshHandler.getExecutionTarget(),
                RefreshJobTargetResolver.recordedTarget(refreshHandler).getClusterId());
    }

    /** A request sent to a refresh job: how long it is awaited and what its failure tells. */
    private enum JobRequest {
        GET_STATUS("get the status of refresh job %s", CLIENT_TIMEOUT_OPTIONS, "", false),
        CANCEL("cancel refresh job %s", CLIENT_TIMEOUT_OPTIONS, "", true),
        STOP_WITH_SAVEPOINT(
                "stop refresh job %s with a savepoint",
                STOP_WITH_SAVEPOINT_TIMEOUT_OPTIONS, SAVEPOINT_WAIT_HINT, true);

        private final String action;
        private final String timeoutOptions;
        private final String timeoutHint;
        private final boolean mayStillComplete;

        JobRequest(
                String action,
                String timeoutOptions,
                String timeoutHint,
                boolean mayStillComplete) {
            this.action = action;
            this.timeoutOptions = timeoutOptions;
            this.timeoutHint = timeoutHint;
            this.mayStillComplete = mayStillComplete;
        }

        Duration timeout(TableConfig tableConfig) {
            final Duration clientTimeout = tableConfig.get(CLIENT_TIMEOUT);
            if (this != STOP_WITH_SAVEPOINT) {
                return clientTimeout;
            }

            return clientTimeout.plus(tableConfig.get(CheckpointingOptions.CHECKPOINTING_TIMEOUT));
        }
    }
}

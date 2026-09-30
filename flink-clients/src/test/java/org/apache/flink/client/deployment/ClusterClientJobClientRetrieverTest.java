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

package org.apache.flink.client.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.util.DummyClusterDescriptor;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.client.program.TestingClusterClient;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobClientRetrieverLoader;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterClientJobClientRetriever}. */
class ClusterClientJobClientRetrieverTest {

    private static final String FAKE_TARGET = "fake";

    private static final ConfigOption<String> WRITTEN_BY_FACTORY =
            ConfigOptions.key("test.written-by-factory").stringType().noDefaultValue();

    @RegisterExtension
    final LoggerAuditingExtension retrieverLog =
            new LoggerAuditingExtension(
                    ClusterClientJobClientRetriever.class, org.slf4j.event.Level.DEBUG);

    private final ClusterClientJobClientRetriever retriever = new ClusterClientJobClientRetriever();

    @Test
    void remoteTargetIsCompatible() {
        assertThat(retriever.isCompatibleWith(configurationFor(RemoteExecutor.NAME))).isTrue();
    }

    @Test
    void targetsWithoutClusterClientFactoryAreNotCompatible() {
        assertThat(retriever.isCompatibleWith(configurationFor("non-existing"))).isFalse();
        assertThat(retriever.isCompatibleWith(configurationFor("minicluster"))).isFalse();
    }

    @Test
    void incompatibilityReasonIsOnlyLoggedAtDebug() {
        final ClusterClientJobClientRetriever retrieverWithoutUsableFactory =
                new ClusterClientJobClientRetriever(
                        new FailingClusterClientServiceLoader(
                                new IllegalStateException(
                                        "Multiple compatible client factories found for:\n"
                                                + "security.password=secret-value.")));

        assertThat(retrieverWithoutUsableFactory.isCompatibleWith(configurationFor(FAKE_TARGET)))
                .isFalse();

        assertThat(retrieverLog.getEvents())
                .filteredOn(event -> event.getLevel().isMoreSpecificThan(Level.INFO))
                .singleElement()
                .satisfies(
                        event -> {
                            assertThat(event.getThrown()).isNull();
                            assertThat(event.getMessage().getFormattedMessage())
                                    .contains("'fake'")
                                    .doesNotContain("secret-value");
                        });
        assertThat(retrieverLog.getEvents())
                .filteredOn(event -> Level.DEBUG.equals(event.getLevel()))
                .singleElement()
                .satisfies(
                        event ->
                                assertThat(event.getThrown()).hasMessageContaining("secret-value"));
    }

    @Test
    void retrievedClientIsBoundToTheJob() throws Exception {
        final JobID jobId = new JobID();

        final JobClient jobClient =
                retriever.retrieveJobClient(
                        jobId, configurationFor(RemoteExecutor.NAME), getClass().getClassLoader());

        assertThat(jobClient).isInstanceOf(ClusterClientJobClientAdapter.class);
        assertThat(jobClient.getJobID()).isEqualTo(jobId);
    }

    @Test
    void retrieverIsDiscoveredThroughTheServiceLoader() {
        assertThat(JobClientRetrieverLoader.findCompatible(configurationFor(RemoteExecutor.NAME)))
                .containsInstanceOf(ClusterClientJobClientRetriever.class);
    }

    @Test
    void missingClusterIdIsRejectedBeforeTheClusterDescriptorIsCreated() {
        final ConfigurationWritingClusterClientFactory clusterClientFactory =
                new ConfigurationWritingClusterClientFactory(null);
        final ClusterClientJobClientRetriever retrieverWithoutClusterId =
                new ClusterClientJobClientRetriever(
                        new FixedClusterClientServiceLoader(clusterClientFactory));

        assertThatThrownBy(
                        () ->
                                retrieverWithoutClusterId.retrieveJobClient(
                                        new JobID(),
                                        configurationFor(FAKE_TARGET),
                                        getClass().getClassLoader()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No cluster id is configured for execution target 'fake'.");
        assertThat(clusterClientFactory.clusterDescriptorCreated).isFalse();
    }

    @Test
    void callerConfigurationIsNotModified() throws Exception {
        final ClusterClientJobClientRetriever retrieverWithClusterId =
                new ClusterClientJobClientRetriever(
                        new FixedClusterClientServiceLoader(
                                new ConfigurationWritingClusterClientFactory(42)));
        final Configuration configuration = configurationFor(FAKE_TARGET);

        retrieverWithClusterId.retrieveJobClient(
                new JobID(), configuration, getClass().getClassLoader());

        assertThat(configuration.contains(WRITTEN_BY_FACTORY)).isFalse();
    }

    private static Configuration configurationFor(String executionTarget) {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, executionTarget);
        return configuration;
    }

    private static final class FixedClusterClientServiceLoader
            implements ClusterClientServiceLoader {

        private final ClusterClientFactory<?> clusterClientFactory;

        private FixedClusterClientServiceLoader(ClusterClientFactory<?> clusterClientFactory) {
            this.clusterClientFactory = clusterClientFactory;
        }

        @Override
        public <ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(
                Configuration configuration) {
            return (ClusterClientFactory<ClusterID>) clusterClientFactory;
        }

        @Override
        public Stream<String> getApplicationModeTargetNames() {
            return Stream.empty();
        }
    }

    private static final class FailingClusterClientServiceLoader
            implements ClusterClientServiceLoader {

        private final IllegalStateException failure;

        private FailingClusterClientServiceLoader(IllegalStateException failure) {
            this.failure = failure;
        }

        @Override
        public <ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(
                Configuration configuration) {
            throw failure;
        }

        @Override
        public Stream<String> getApplicationModeTargetNames() {
            return Stream.empty();
        }
    }

    private static final class ConfigurationWritingClusterClientFactory
            extends ClusterClientServiceLoaderTest.BaseTestingClusterClientFactory {

        @Nullable private final Integer clusterId;

        private boolean clusterDescriptorCreated;

        private ConfigurationWritingClusterClientFactory(@Nullable Integer clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public ClusterDescriptor<Integer> createClusterDescriptor(Configuration configuration) {
            clusterDescriptorCreated = true;
            configuration.set(WRITTEN_BY_FACTORY, "written");
            return new DummyClusterDescriptor<>(new TestingClusterClient<>());
        }

        @Nullable
        @Override
        public Integer getClusterId(Configuration configuration) {
            return clusterId;
        }
    }
}

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

package org.apache.flink.core.execution;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JobClientRetrieverLoader}. */
class JobClientRetrieverLoaderTest {

    @Test
    void noRetrieverOnTheClassPathIsEmpty() {
        assertThat(JobClientRetrieverLoader.findCompatible(configurationFor("remote"))).isEmpty();
    }

    @Test
    void incompatibleRetrieversAreSkipped() {
        assertThat(
                        JobClientRetrieverLoader.findCompatible(
                                configurationFor("remote"),
                                List.of(new TargetRetriever("yarn-session"))))
                .isEmpty();
    }

    @Test
    void theCompatibleRetrieverIsReturned() {
        TargetRetriever remote = new TargetRetriever("remote");

        assertThat(
                        JobClientRetrieverLoader.findCompatible(
                                configurationFor("remote"),
                                List.of(new TargetRetriever("yarn-session"), remote)))
                .containsSame(remote);
    }

    @Test
    void multipleCompatibleRetrieversAreRejected() {
        assertThatThrownBy(
                        () ->
                                JobClientRetrieverLoader.findCompatible(
                                        configurationFor("remote"),
                                        List.of(
                                                new TargetRetriever("remote"),
                                                new TargetRetriever("remote"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple compatible job client retrievers")
                .hasMessageContaining("remote");
    }

    @Test
    void providersWithMissingDependenciesAreSkipped() {
        final TargetRetriever remote = new TargetRetriever("remote");

        assertThat(
                        JobClientRetrieverLoader.findCompatible(
                                configurationFor("remote"),
                                new ScriptedProviders(
                                        List.of(
                                                failingToLoad(
                                                        new NoClassDefFoundError("missing/Class")),
                                                failingToLoad(
                                                        new ServiceConfigurationError(
                                                                "Provider could not be instantiated",
                                                                new NoClassDefFoundError(
                                                                        "missing/Class"))),
                                                loaded(remote)))))
                .containsSame(remote);
    }

    @Test
    void otherProviderLoadingFailuresAreRethrown() {
        final ServiceConfigurationError failure =
                new ServiceConfigurationError(
                        "Provider could not be instantiated", new IllegalStateException("broken"));

        assertThatThrownBy(
                        () ->
                                JobClientRetrieverLoader.findCompatible(
                                        configurationFor("remote"),
                                        new ScriptedProviders(
                                                List.of(
                                                        failingToLoad(failure),
                                                        loaded(new TargetRetriever("remote"))))))
                .isSameAs(failure);
    }

    @Test
    void compatibilityCheckFailuresAreRethrown() {
        final IllegalStateException failure = new IllegalStateException("broken");

        assertThatThrownBy(
                        () ->
                                JobClientRetrieverLoader.findCompatible(
                                        configurationFor("remote"),
                                        List.of(
                                                new CompatibilityCheckFailingRetriever(failure),
                                                new TargetRetriever("remote"))))
                .isSameAs(failure);
    }

    private static Supplier<JobClientRetriever> loaded(JobClientRetriever retriever) {
        return () -> retriever;
    }

    private static Supplier<JobClientRetriever> failingToLoad(Error failure) {
        return () -> {
            throw failure;
        };
    }

    private static Configuration configurationFor(String executionTarget) {
        Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, executionTarget);
        return configuration;
    }

    /** A retriever that is compatible with exactly one execution target. */
    private static final class TargetRetriever implements JobClientRetriever {

        private final String executionTarget;

        private TargetRetriever(String executionTarget) {
            this.executionTarget = executionTarget;
        }

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return executionTarget.equals(configuration.get(DeploymentOptions.TARGET));
        }

        @Override
        public JobClient retrieveJobClient(
                JobID jobId, Configuration configuration, ClassLoader userCodeClassLoader) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "TargetRetriever(" + executionTarget + ")";
        }
    }

    private static final class ScriptedProviders implements Iterable<JobClientRetriever> {

        private final List<Supplier<JobClientRetriever>> providers;

        private ScriptedProviders(List<Supplier<JobClientRetriever>> providers) {
            this.providers = providers;
        }

        @Override
        public Iterator<JobClientRetriever> iterator() {
            final Iterator<Supplier<JobClientRetriever>> remainingProviders = providers.iterator();
            return new Iterator<JobClientRetriever>() {
                @Override
                public boolean hasNext() {
                    return remainingProviders.hasNext();
                }

                @Override
                public JobClientRetriever next() {
                    return remainingProviders.next().get();
                }
            };
        }
    }

    /** A retriever whose compatibility check fails. */
    private static final class CompatibilityCheckFailingRetriever implements JobClientRetriever {

        private final RuntimeException failure;

        private CompatibilityCheckFailingRetriever(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            throw failure;
        }

        @Override
        public JobClient retrieveJobClient(
                JobID jobId, Configuration configuration, ClassLoader userCodeClassLoader) {
            throw new UnsupportedOperationException();
        }
    }
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Discovers the {@link JobClientRetriever} that is compatible with a configuration. */
@Internal
public final class JobClientRetrieverLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JobClientRetrieverLoader.class);

    /**
     * Returns the retriever on the class path that is compatible with the given configuration, or
     * {@link Optional#empty()} if there is none.
     *
     * @throws IllegalStateException if more than one retriever is compatible
     */
    public static Optional<JobClientRetriever> findCompatible(Configuration configuration) {
        return findCompatible(configuration, ServiceLoader.load(JobClientRetriever.class));
    }

    @VisibleForTesting
    static Optional<JobClientRetriever> findCompatible(
            Configuration configuration, Iterable<JobClientRetriever> retrievers) {
        checkNotNull(configuration);

        final List<JobClientRetriever> compatibleRetrievers = new ArrayList<>();
        final Iterator<JobClientRetriever> iterator = retrievers.iterator();
        while (true) {
            try {
                // hasNext is inside the try because it might throw on Java 24 and 25, see
                // https://bugs.openjdk.org/browse/JDK-8196182 and
                // https://bugs.openjdk.org/browse/JDK-8350481
                if (!iterator.hasNext()) {
                    break;
                }
                final JobClientRetriever retriever = iterator.next();
                if (retriever != null && retriever.isCompatibleWith(configuration)) {
                    compatibleRetrievers.add(retriever);
                }
            } catch (Throwable e) {
                if (e instanceof NoClassDefFoundError
                        || e.getCause() instanceof NoClassDefFoundError) {
                    LOG.info(
                            "Could not load a job client retriever due to missing dependencies.",
                            e);
                } else {
                    throw e;
                }
            }
        }

        if (compatibleRetrievers.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple compatible job client retrievers found for execution target '%s': %s.",
                            configuration.get(DeploymentOptions.TARGET), compatibleRetrievers));
        }
        return compatibleRetrievers.stream().findFirst();
    }

    private JobClientRetrieverLoader() {}
}

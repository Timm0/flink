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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobClientRetriever;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link JobClientRetriever} backed by the {@link ClusterClientFactory} that matches the
 * execution target.
 */
@Internal
public final class ClusterClientJobClientRetriever implements JobClientRetriever {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClusterClientJobClientRetriever.class);

    private final ClusterClientServiceLoader clusterClientServiceLoader;

    public ClusterClientJobClientRetriever() {
        this(new DefaultClusterClientServiceLoader());
    }

    @VisibleForTesting
    ClusterClientJobClientRetriever(ClusterClientServiceLoader clusterClientServiceLoader) {
        this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);
    }

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        try {
            clusterClientServiceLoader.getClusterClientFactory(configuration);
            return true;
        } catch (IllegalStateException e) {
            final String target = configuration.get(DeploymentOptions.TARGET);
            LOG.info(
                    "No usable ClusterClientFactory for execution target '{}'. Enable DEBUG logging for {} to see why.",
                    target,
                    ClusterClientJobClientRetriever.class.getName());
            LOG.debug("No usable ClusterClientFactory for execution target '{}'.", target, e);
            return false;
        }
    }

    @Override
    public JobClient retrieveJobClient(
            JobID jobId, Configuration configuration, ClassLoader userCodeClassLoader)
            throws Exception {
        return retrieveJobClient(
                clusterClientServiceLoader.getClusterClientFactory(configuration),
                jobId,
                configuration,
                userCodeClassLoader);
    }

    private static <ClusterID> JobClient retrieveJobClient(
            ClusterClientFactory<ClusterID> clusterClientFactory,
            JobID jobId,
            Configuration configuration,
            ClassLoader userCodeClassLoader)
            throws Exception {
        final ClusterID clusterId = clusterClientFactory.getClusterId(configuration);
        checkState(
                clusterId != null,
                "No cluster id is configured for execution target '%s'.",
                configuration.get(DeploymentOptions.TARGET));

        try (ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(new Configuration(configuration))) {
            return new ClusterClientJobClientAdapter<>(
                    clusterDescriptor.retrieve(clusterId), jobId, userCodeClassLoader);
        }
    }
}

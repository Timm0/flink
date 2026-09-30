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
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

/**
 * Attaches to a job that runs on an existing cluster.
 *
 * <p>Counterpart of {@link PipelineExecutorFactory} for jobs the caller did not submit itself.
 */
@Internal
public interface JobClientRetriever {

    /** Returns whether this retriever supports the execution target of the given configuration. */
    boolean isCompatibleWith(Configuration configuration);

    /**
     * Returns a {@link JobClient} for the given job on the cluster the configuration names.
     *
     * <p>The call may block while it resolves the cluster.
     *
     * @param jobId the job to attach to
     * @param configuration the execution target, the cluster id and every further coordinate needed
     *     to reach the cluster
     * @param userCodeClassLoader the non-null class loader used to deserialize job results
     * @throws Exception if the cluster cannot be resolved from the configuration
     */
    JobClient retrieveJobClient(
            JobID jobId, Configuration configuration, ClassLoader userCodeClassLoader)
            throws Exception;
}

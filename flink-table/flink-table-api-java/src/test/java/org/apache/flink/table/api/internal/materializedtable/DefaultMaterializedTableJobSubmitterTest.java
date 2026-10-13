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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.delegation.materializedtable.RefreshJobTarget;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultMaterializedTableJobSubmitter}. */
class DefaultMaterializedTableJobSubmitterTest {

    @Test
    void testTargetOverridesTheTableAndExecutionConfiguration() {
        final Configuration tableConfiguration = new Configuration();
        tableConfiguration.set(DeploymentOptions.TARGET, "remote");
        tableConfiguration.setString("kubernetes.cluster-id", "from-table-config");
        tableConfiguration.set(PipelineOptions.NAME, "from-table-config");
        tableConfiguration.setString("rest.address", "table-config-host");
        final Configuration executionConfig = new Configuration();
        executionConfig.setString("kubernetes.cluster-id", "from-execution-config");
        executionConfig.set(PipelineOptions.NAME, "refresh-job");

        final Configuration refreshConfig =
                DefaultMaterializedTableJobSubmitter.refreshConfiguration(
                        tableConfiguration,
                        executionConfig,
                        new RefreshJobTarget("kubernetes-session", "resolved"));

        assertThat(refreshConfig.toMap())
                .containsEntry("execution.target", "kubernetes-session")
                .containsEntry("kubernetes.cluster-id", "resolved")
                .containsEntry("pipeline.name", "refresh-job")
                .containsEntry("rest.address", "table-config-host");
        assertThat(tableConfiguration.get(DeploymentOptions.TARGET)).isEqualTo("remote");
    }
}

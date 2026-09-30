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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedTableClusterUtils}. */
class MaterializedTableClusterUtilsTest {

    @Test
    void testClusterInfoContainsTargetAndTargetSpecificClusterId() {
        assertThat(MaterializedTableClusterUtils.buildClusterInfo("remote", ""))
                .isEqualTo(Map.of("execution.target", "remote"));
        assertThat(
                        MaterializedTableClusterUtils.buildClusterInfo(
                                "yarn-session", "application_1_0001"))
                .isEqualTo(
                        Map.of(
                                "execution.target", "yarn-session",
                                "yarn.application.id", "application_1_0001"));
        assertThat(
                        MaterializedTableClusterUtils.buildClusterInfo(
                                "kubernetes-application", "my-k8s-cluster"))
                .isEqualTo(
                        Map.of(
                                "execution.target", "kubernetes-application",
                                "kubernetes.cluster-id", "my-k8s-cluster"));
    }
}

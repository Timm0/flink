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

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RefreshJobTarget}. */
class RefreshJobTargetTest {

    @Test
    void applicationTargetsAreRecognised() {
        assertThat(new RefreshJobTarget("kubernetes-application", "").isApplicationTarget())
                .isTrue();
        assertThat(new RefreshJobTarget("yarn-application", "").isApplicationTarget()).isTrue();
        assertThat(new RefreshJobTarget("kubernetes-session", "c").isApplicationTarget()).isFalse();
        assertThat(new RefreshJobTarget("remote", "").isApplicationTarget()).isFalse();
    }

    @Test
    void applyToSetsTheTargetAndItsClusterIdKey() {
        Configuration configuration = new Configuration();

        new RefreshJobTarget("kubernetes-session", "mt-refresh").applyTo(configuration);

        assertThat(configuration.toMap())
                .isEqualTo(
                        Map.of(
                                "execution.target", "kubernetes-session",
                                "kubernetes.cluster-id", "mt-refresh"));
    }

    @Test
    void applyToSetsOnlyTheTargetWhenItHasNoClusterIdKey() {
        Configuration configuration = new Configuration();

        new RefreshJobTarget("remote", "StandaloneClusterId").applyTo(configuration);

        assertThat(configuration.toMap()).isEqualTo(Map.of("execution.target", "remote"));
    }

    @Test
    void applyToSkipsAnEmptyClusterId() {
        Configuration configuration = new Configuration();

        new RefreshJobTarget("kubernetes-application", "").applyTo(configuration);

        assertThat(configuration.toMap())
                .isEqualTo(Map.of("execution.target", "kubernetes-application"));
    }

    @Test
    void emptyTargetIsRejected() {
        assertThatThrownBy(() -> new RefreshJobTarget("", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void onlyAKnownClusterIdOfATargetWithAClusterIdKeyIdentifiesTheCluster() {
        assertThat(new RefreshJobTarget("kubernetes-session", "c1").identifyingClusterIdKey())
                .contains("kubernetes.cluster-id");
        assertThat(new RefreshJobTarget("yarn-application", "").identifyingClusterIdKey())
                .isEmpty();
        assertThat(new RefreshJobTarget("remote", "StandaloneClusterId").identifyingClusterIdKey())
                .isEmpty();
    }

    @Test
    void equalityIsByValue() {
        assertThat(new RefreshJobTarget("yarn-session", "application_1_0001"))
                .isEqualTo(new RefreshJobTarget("yarn-session", "application_1_0001"))
                .hasSameHashCodeAs(new RefreshJobTarget("yarn-session", "application_1_0001"))
                .isNotEqualTo(new RefreshJobTarget("yarn-session", "application_1_0002"));
        assertThat(new RefreshJobTarget("yarn-session", "application_1_0001").toString())
                .contains("yarn-session", "application_1_0001");
    }
}

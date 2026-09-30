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
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.flink.table.delegation.materializedtable.RefreshJobTargetResolver.ROOT_LAYER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RefreshJobTargetResolver}. */
class RefreshJobTargetResolverTest {

    private static final ObjectIdentifier TABLE = ObjectIdentifier.of("cat", "db", "mt");

    @Test
    void targetAndClusterIdOnTableConfigAreResolved() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");
        tableConfig.set("kubernetes.cluster-id", "mt-refresh");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, false))
                .isEqualTo(new RefreshJobTarget("kubernetes-session", "mt-refresh"));
    }

    @Test
    void targetAndClusterIdInRootAreResolved() {
        Configuration root = new Configuration();
        root.set(DeploymentOptions.TARGET, "yarn-session");
        root.setString("yarn.application.id", "application_1_0001");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig(root), false))
                .isEqualTo(new RefreshJobTarget("yarn-session", "application_1_0001"));
    }

    @Test
    void clusterIdOnlyInRootWhileTargetIsOnTableConfigIsRejected() {
        Configuration root = new Configuration();
        root.setString("kubernetes.cluster-id", "mt-refresh");
        TableConfig tableConfig = tableConfig(root);
        tableConfig.set(DeploymentOptions.TARGET, "kubernetes-session");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'kubernetes.cluster-id'")
                .hasMessageContaining("TableConfig");
    }

    @Test
    void clusterIdOnlyOnTableConfigWhileTargetIsInRootIsRejected() {
        Configuration root = new Configuration();
        root.set(DeploymentOptions.TARGET, "kubernetes-session");
        TableConfig tableConfig = tableConfig(root);
        tableConfig.set("kubernetes.cluster-id", "mt-refresh");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'kubernetes.cluster-id'")
                .hasMessageContaining(ROOT_LAYER);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void blankClusterIdIsRejected(String clusterId) {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");
        tableConfig.set("kubernetes.cluster-id", clusterId);

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("requires 'kubernetes.cluster-id' to be configured");
    }

    @Test
    void unsetTargetIsRejected() {
        assertThatThrownBy(
                        () ->
                                RefreshJobTargetResolver.resolve(
                                        tableConfig(new Configuration()), false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported execution target detected: null");
    }

    @Test
    void unsetTargetNamesTheKeyAndTheRootLayer() {
        assertThatThrownBy(
                        () ->
                                RefreshJobTargetResolver.resolve(
                                        tableConfig(new Configuration()), false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("(read from 'execution.target' in the " + ROOT_LAYER + ")");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "local"})
    void emptyAndLocalTargetsAreRejected(String executionTarget) {
        final TableConfig tableConfig = tableConfigTargeting(executionTarget);

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, true))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'local' is not supported");
    }

    @Test
    void embeddedTargetIsRejected() {
        final TableConfig tableConfig = tableConfigTargeting("embedded");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, true))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Execution target 'embedded' is not supported")
                .hasMessageContaining("session cluster");
    }

    @Test
    void applicationTargetIsRejectedWithoutSupport() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-application");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Application-mode materialized table refresh is not supported in this environment")
                .hasMessageContaining("kubernetes-application");
    }

    @Test
    void applicationTargetIsResolvedWithSupportAndWithoutClusterId() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-application");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, true))
                .isEqualTo(new RefreshJobTarget("kubernetes-application", ""));
    }

    @Test
    void miniclusterTargetIsResolvedWithoutClusterId() {
        final TableConfig tableConfig = tableConfigTargeting("minicluster");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, false))
                .isEqualTo(new RefreshJobTarget("minicluster", ""));
    }

    @ParameterizedTest
    @ValueSource(strings = {"rest.address", "jobmanager.rpc.address"})
    void remoteTargetOnTableConfigIsResolvedWithItsRestAddressThere(String addressKey) {
        final TableConfig tableConfig = tableConfigTargeting("remote");
        tableConfig.set(addressKey, "refresh-cluster");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, false))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void restAddressOnlyInRootWhileRemoteTargetIsOnTableConfigIsRejected() {
        final Configuration root = new Configuration();
        root.set(RestOptions.ADDRESS, "program-cluster");
        final TableConfig tableConfig = tableConfig(root);
        tableConfig.set(DeploymentOptions.TARGET, "remote");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Execution target 'remote' requires 'rest.address' to be configured")
                .hasMessageContaining("TableConfig");
    }

    @Test
    void remoteTargetOnTableConfigNeedsNoRestAddressWithHighAvailability() {
        final TableConfig tableConfig = tableConfigTargeting("remote");
        tableConfig.set("high-availability.type", "zookeeper");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, false))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void blankRestAddressOnTableConfigIsRejected() {
        final TableConfig tableConfig = tableConfigTargeting("remote");
        tableConfig.set("rest.address", "  ");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("requires 'rest.address' to be configured");
    }

    @Test
    void remoteTargetInRootNeedsNoRestAddress() {
        final Configuration root = new Configuration();
        root.set(DeploymentOptions.TARGET, "remote");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig(root), false))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void highAvailabilityClusterIdOnlyInRootIsRejectedWhenTheTableConfigEnablesHighAvailability() {
        final Configuration root = new Configuration();
        root.set(HighAvailabilityOptions.HA_CLUSTER_ID, "/program-cluster");
        final TableConfig tableConfig = tableConfig(root);
        tableConfig.set(DeploymentOptions.TARGET, "remote");
        tableConfig.set("rest.address", "refresh-cluster");
        tableConfig.set("high-availability.type", "zookeeper");

        assertThatThrownBy(() -> RefreshJobTargetResolver.resolve(tableConfig, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'high-availability.cluster-id' must be set there too")
                .hasMessageContaining(ROOT_LAYER);
    }

    @Test
    void highAvailabilityOnTableConfigWithItsOwnClusterIdIsResolved() {
        final Configuration root = new Configuration();
        root.set(HighAvailabilityOptions.HA_CLUSTER_ID, "/program-cluster");
        final TableConfig tableConfig = tableConfig(root);
        tableConfig.set(DeploymentOptions.TARGET, "remote");
        tableConfig.set("rest.address", "refresh-cluster");
        tableConfig.set("high-availability.type", "zookeeper");
        tableConfig.set("high-availability.cluster-id", "/refresh-cluster");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, false))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void highAvailabilityOnlyInRootDoesNotRejectATargetOnTableConfig() {
        final Configuration root = new Configuration();
        root.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        root.set(HighAvailabilityOptions.HA_CLUSTER_ID, "/program-cluster");
        final TableConfig tableConfig = tableConfig(root);
        tableConfig.set(DeploymentOptions.TARGET, "remote");
        tableConfig.set("rest.address", "refresh-cluster");

        assertThat(RefreshJobTargetResolver.resolve(tableConfig, false))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void sameSessionClusterIsResolved() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");
        tableConfig.set("kubernetes.cluster-id", "A");

        assertThat(
                        RefreshJobTargetResolver.resolveOnRecordedCluster(
                                tableConfig,
                                TABLE,
                                new RefreshJobTarget("kubernetes-session", "A")))
                .isEqualTo(new RefreshJobTarget("kubernetes-session", "A"));
    }

    @Test
    void otherClusterIdIsRejected() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");
        tableConfig.set("kubernetes.cluster-id", "B");

        assertThatThrownBy(
                        () ->
                                RefreshJobTargetResolver.resolveOnRecordedCluster(
                                        tableConfig,
                                        TABLE,
                                        new RefreshJobTarget("kubernetes-session", "A")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "runs on execution.target 'kubernetes-session', kubernetes.cluster-id 'A'")
                .hasMessageContaining(
                        "submits refresh jobs to execution.target 'kubernetes-session', kubernetes.cluster-id 'B'")
                .hasMessageContaining(TABLE.toString());
    }

    @Test
    void otherTargetIsRejected() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");
        tableConfig.set("kubernetes.cluster-id", "A");

        assertThatThrownBy(
                        () ->
                                RefreshJobTargetResolver.resolveOnRecordedCluster(
                                        tableConfig, TABLE, new RefreshJobTarget("remote", "")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("runs on execution.target 'remote'")
                .hasMessageContaining(
                        "submits refresh jobs to execution.target 'kubernetes-session', kubernetes.cluster-id 'A'")
                .hasMessageContaining(TABLE.toString());
    }

    @Test
    void restAddressOfARemoteTargetIsNotCompared() {
        final Configuration root = new Configuration();
        root.set(DeploymentOptions.TARGET, "remote");
        root.setString("rest.address", "another-jobmanager");

        assertThat(
                        RefreshJobTargetResolver.resolveOnRecordedCluster(
                                tableConfig(root), TABLE, new RefreshJobTarget("remote", "")))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void legacyStandaloneClusterIdOfARemoteTargetIsNotCompared() {
        final Configuration root = new Configuration();
        root.set(DeploymentOptions.TARGET, "remote");

        assertThat(
                        RefreshJobTargetResolver.resolveOnRecordedCluster(
                                tableConfig(root),
                                TABLE,
                                new RefreshJobTarget("remote", "StandaloneClusterId")))
                .isEqualTo(new RefreshJobTarget("remote", ""));
    }

    @Test
    void recordedTargetWithoutClusterIdComparesOnlyTheTarget() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");
        tableConfig.set("kubernetes.cluster-id", "B");
        final RefreshJobTarget recordedTarget =
                RefreshJobTargetResolver.recordedTarget(
                        new ContinuousRefreshHandler("kubernetes-session", null, "job"));

        assertThat(recordedTarget).isEqualTo(new RefreshJobTarget("kubernetes-session", ""));
        assertThat(
                        RefreshJobTargetResolver.resolveOnRecordedCluster(
                                tableConfig, TABLE, recordedTarget))
                .isEqualTo(new RefreshJobTarget("kubernetes-session", "B"));
    }

    @Test
    void applicationClusterIsRejectedBeforeTheProgramsTargetIsResolved() {
        assertThatThrownBy(
                        () ->
                                RefreshJobTargetResolver.resolveOnRecordedCluster(
                                        tableConfig(new Configuration()),
                                        TABLE,
                                        new RefreshJobTarget("kubernetes-application", "app-1")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("own application cluster")
                .hasMessageContaining(
                        "execution.target 'kubernetes-application', kubernetes.cluster-id 'app-1'")
                .hasMessageContaining("SQL Gateway");
    }

    @Test
    void restPortOnTableConfigIsPinnedForARemoteTarget() {
        final TableConfig tableConfig = tableConfigTargeting("remote");
        tableConfig.set("rest.address", "refresh-cluster");
        tableConfig.set("rest.port", "9091");
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 8081);

        RefreshJobTargetResolver.pinUnrecordedCoordinates(tableConfig, "remote", configuration);

        assertThat(configuration.get(RestOptions.ADDRESS)).isEqualTo("refresh-cluster");
        assertThat(configuration.get(RestOptions.PORT)).isEqualTo(9091);
    }

    @Test
    void restPortIsInheritedWhenTheTableConfigSetsNone() {
        final TableConfig tableConfig = tableConfigTargeting("remote");
        tableConfig.set("rest.address", "refresh-cluster");
        final Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 9092);

        RefreshJobTargetResolver.pinUnrecordedCoordinates(tableConfig, "remote", configuration);

        assertThat(configuration.get(RestOptions.PORT)).isEqualTo(9092);
    }

    @Test
    void validationOfAnotherRecordedTargetRequiresNoRestAddress() {
        final TableConfig tableConfig = tableConfigTargeting("kubernetes-session");

        assertThatNoException()
                .isThrownBy(
                        () ->
                                RefreshJobTargetResolver.validateUnrecordedCoordinates(
                                        tableConfig, "remote"));
    }

    @Test
    void validationOfTheRemoteTableConfigTargetRequiresItsRestAddress() {
        final TableConfig tableConfig = tableConfigTargeting("remote");

        assertThatThrownBy(
                        () ->
                                RefreshJobTargetResolver.validateUnrecordedCoordinates(
                                        tableConfig, "remote"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("requires 'rest.address' to be configured");
    }

    @Test
    void validationIsSkippedWhenTheTargetComesFromTheRootLayer() {
        final Configuration root = new Configuration();
        root.set(DeploymentOptions.TARGET, "remote");
        root.set(HighAvailabilityOptions.HA_CLUSTER_ID, "/program-cluster");
        final TableConfig tableConfig = tableConfig(root);
        tableConfig.set("high-availability.type", "zookeeper");

        assertThatNoException()
                .isThrownBy(
                        () ->
                                RefreshJobTargetResolver.validateUnrecordedCoordinates(
                                        tableConfig, "remote"));
    }

    private static TableConfig tableConfigTargeting(String executionTarget) {
        final TableConfig tableConfig = tableConfig(new Configuration());
        tableConfig.set(DeploymentOptions.TARGET, executionTarget);
        return tableConfig;
    }

    private static TableConfig tableConfig(Configuration root) {
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(root);
        return tableConfig;
    }
}

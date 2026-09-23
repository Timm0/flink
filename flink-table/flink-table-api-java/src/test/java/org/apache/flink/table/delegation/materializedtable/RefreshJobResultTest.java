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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RefreshJobResult}. */
class RefreshJobResultTest {

    private static final Map<String, String> CLUSTER_INFO = Map.of("execution.target", "remote");

    @Test
    void testGetters() {
        RefreshJobResult result =
                new RefreshJobResult("remote", "cluster-1", "job-1", CLUSTER_INFO);

        assertThat(result.getExecutionTarget()).isEqualTo("remote");
        assertThat(result.getClusterId()).isEqualTo("cluster-1");
        assertThat(result.getJobId()).isEqualTo("job-1");
        assertThat(result.getClusterInfo()).isEqualTo(CLUSTER_INFO);
    }

    @Test
    void testClusterInfoIsDefensivelyCopied() {
        Map<String, String> mutable = new HashMap<>(CLUSTER_INFO);

        RefreshJobResult result = new RefreshJobResult("remote", "cluster-1", "job-1", mutable);
        mutable.put("added-after", "should-not-leak");

        assertThat(result.getClusterInfo()).containsOnlyKeys("execution.target");
    }

    @Test
    void testToStringContainsFields() {
        RefreshJobResult result =
                new RefreshJobResult("remote", "cluster-1", "job-1", CLUSTER_INFO);

        assertThat(result.toString()).contains("remote", "cluster-1", "job-1");
    }
}

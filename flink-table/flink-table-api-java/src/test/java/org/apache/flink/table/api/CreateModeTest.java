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

package org.apache.flink.table.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CreateModeTest {

    @Test
    void factoriesReturnTheirKind() {
        assertThat(CreateMode.createOrAlter().getKind()).isEqualTo(CreateMode.Kind.CREATE_OR_ALTER);
        assertThat(CreateMode.failIfExists().getKind()).isEqualTo(CreateMode.Kind.FAIL_IF_EXISTS);
    }

    @Test
    void summaryStringIsTheKindName() {
        assertThat(CreateMode.createOrAlter().asSummaryString()).isEqualTo("CREATE_OR_ALTER");
        assertThat(CreateMode.failIfExists()).hasToString("FAIL_IF_EXISTS");
    }

    @Test
    void modesAreValues() {
        assertThat(CreateMode.failIfExists())
                .isEqualTo(new CreateMode(CreateMode.Kind.FAIL_IF_EXISTS));
        assertThat(CreateMode.failIfExists())
                .hasSameHashCodeAs(new CreateMode(CreateMode.Kind.FAIL_IF_EXISTS));
        assertThat(CreateMode.failIfExists()).isNotEqualTo(CreateMode.createOrAlter());
    }
}

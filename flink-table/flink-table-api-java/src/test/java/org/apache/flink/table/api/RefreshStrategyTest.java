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

class RefreshStrategyTest {

    @Test
    void factoriesReturnTheirKind() {
        assertThat(RefreshStrategy.automatic().getKind()).isEqualTo(RefreshStrategy.Kind.AUTOMATIC);
        assertThat(RefreshStrategy.continuous().getKind())
                .isEqualTo(RefreshStrategy.Kind.CONTINUOUS);
        assertThat(RefreshStrategy.full().getKind()).isEqualTo(RefreshStrategy.Kind.FULL);
    }

    @Test
    void summaryStringIsTheKindName() {
        assertThat(RefreshStrategy.automatic().asSummaryString()).isEqualTo("AUTOMATIC");
        assertThat(RefreshStrategy.continuous().asSummaryString()).isEqualTo("CONTINUOUS");
        assertThat(RefreshStrategy.full().asSummaryString()).isEqualTo("FULL");
        assertThat(RefreshStrategy.full()).hasToString("FULL");
    }

    @Test
    void strategiesAreValues() {
        assertThat(RefreshStrategy.continuous())
                .isEqualTo(new RefreshStrategy(RefreshStrategy.Kind.CONTINUOUS));
        assertThat(RefreshStrategy.continuous())
                .hasSameHashCodeAs(new RefreshStrategy(RefreshStrategy.Kind.CONTINUOUS));
        assertThat(RefreshStrategy.continuous()).isNotEqualTo(RefreshStrategy.full());
    }
}

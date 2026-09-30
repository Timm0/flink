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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Interval.TimeUnit;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartMode}. */
class StartModeTest {

    @Test
    void timestampFactoriesRejectNull() {
        assertThatThrownBy(() -> StartMode.fromTimestamp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Start mode timestamp must not be null");
        assertThatThrownBy(() -> StartMode.resumeOrFromTimestamp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Start mode timestamp must not be null");
    }

    @Test
    void offsetMustBePositive() {
        assertThatThrownBy(() -> StartMode.fromNow(Duration.ZERO))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must be positive");
        assertThatThrownBy(() -> StartMode.resumeOrFromNow(Duration.ofSeconds(-1)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void offsetMustBeWholeSeconds() {
        assertThatThrownBy(() -> StartMode.fromNow(Duration.ofMillis(1500)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("whole number of seconds");
        assertThatThrownBy(() -> StartMode.fromNow(Duration.ofNanos(1)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("whole number of seconds");
    }

    @Test
    void offsetAmountMustFitAnInt() {
        assertThat(StartMode.fromNow(Duration.ofSeconds(Integer.MAX_VALUE)).getInterval())
                .isEqualTo(Interval.of(Integer.MAX_VALUE, TimeUnit.SECOND));
        assertThatThrownBy(() -> StartMode.fromNow(Duration.ofSeconds(Integer.MAX_VALUE + 1L)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "at most 2147483647 SECOND, the largest unit that divides it exactly");
        assertThatThrownBy(() -> StartMode.fromNow(Duration.ofDays(Integer.MAX_VALUE + 1L)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("at most 2147483647 DAY");
    }

    @Test
    void offsetFactoriesRejectNull() {
        assertThatThrownBy(() -> StartMode.fromNow(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Start mode offset must not be null");
        assertThatThrownBy(() -> StartMode.resumeOrFromNow(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Start mode offset must not be null");
    }
}

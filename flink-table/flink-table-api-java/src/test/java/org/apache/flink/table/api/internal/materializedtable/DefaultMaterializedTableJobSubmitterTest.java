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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultMaterializedTableJobSubmitter}. */
class DefaultMaterializedTableJobSubmitterTest {

    @Test
    void testWorkflowSchedulingIsUnsupported() {
        assertThat(newSubmitter().getRefreshWorkflowContext()).isEmpty();
    }

    @Test
    void testApplicationTargetIsRejected() {
        assertThatThrownBy(
                        () ->
                                newSubmitter()
                                        .submitRefreshJob(
                                                "kubernetes-application",
                                                new Configuration(),
                                                "INSERT INTO t SELECT * FROM s"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Application-mode materialized table refresh is not supported in this environment.");
    }

    @Test
    void testEmbeddedTargetIsRejected() {
        assertThatThrownBy(
                        () ->
                                newSubmitter()
                                        .submitRefreshJob(
                                                "embedded",
                                                new Configuration(),
                                                "INSERT INTO t SELECT * FROM s"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Application-mode materialized table refresh is not supported in this environment.");
    }

    @Test
    void testControllingAnUnknownJobIsRejected() {
        ContinuousRefreshHandler handler =
                new ContinuousRefreshHandler("remote", "cluster-id", "job-id");

        String expectedMessage =
                "Controlling a refresh job started by another session is not supported in this environment.";

        assertThatThrownBy(() -> newSubmitter().getJobStatus(handler))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> newSubmitter().cancelJob(handler))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(expectedMessage);
        assertThatThrownBy(() -> newSubmitter().stopJobWithSavepoint(handler))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void testInterruptedWaitRestoresTheInterruptFlag() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    DefaultMaterializedTableJobSubmitter.awaitJobClientResult(
                                            new CompletableFuture<>(), "Failed to cancel."))
                    .isInstanceOf(TableException.class)
                    .hasMessage("Failed to cancel.")
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private static DefaultMaterializedTableJobSubmitter newSubmitter() {
        return new DefaultMaterializedTableJobSubmitter(null, null, null);
    }
}

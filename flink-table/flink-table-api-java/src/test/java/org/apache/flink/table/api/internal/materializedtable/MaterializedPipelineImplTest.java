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

import org.apache.flink.table.api.CreateMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MaterializedPipelineImpl}. */
class MaterializedPipelineImplTest {

    private TableEnvironmentMock tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironmentMock.getStreamingInstance();
        tEnv.createTable(
                "source",
                TableDescriptor.forConnector("fake")
                        .schema(Schema.newBuilder().column("f0", DataTypes.INT()).build())
                        .build());
    }

    @Test
    void executeRejectsAStartModeWithoutItsRequiredTimestamp() {
        final StartMode withoutTimestamp = StartMode.of(StartModeKind.FROM_TIMESTAMP);
        final String expectedMessage =
                "Start mode FROM_TIMESTAMP requires a timestamp. Create it with StartMode.fromTimestamp(Instant) or StartMode.resumeOrFromTimestamp(Instant).";

        final MaterializedPipelineImpl pipeline =
                materializeInto(MaterializedTableDescriptor.newBuilder().build());
        final MaterializedPipelineImpl pipelineWithDeclaredStartMode =
                materializeInto(
                        MaterializedTableDescriptor.newBuilder()
                                .startMode(withoutTimestamp)
                                .build());

        assertThatThrownBy(() -> pipeline.execute(withoutTimestamp))
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);
        assertThatThrownBy(pipelineWithDeclaredStartMode::execute)
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);
    }

    @Test
    void startModeGivenToExecuteOverridesTheDeclaredOne() {
        final MaterializedPipelineImpl pipeline =
                materializeInto(
                        MaterializedTableDescriptor.newBuilder()
                                .startMode(StartMode.resumeOrFromNow())
                                .build());

        assertThat(pipeline.buildOperation(CreateMode.createOrAlter(), StartMode.fromNow()))
                .isInstanceOfSatisfying(
                        CreateMaterializedTableOperation.class,
                        operation ->
                                assertThat(operation.getCatalogMaterializedTable().getStartMode())
                                        .contains(StartMode.fromNow()));
    }

    private MaterializedPipelineImpl materializeInto(MaterializedTableDescriptor descriptor) {
        return (MaterializedPipelineImpl) tEnv.from("source").materializeInto("mt", descriptor);
    }
}

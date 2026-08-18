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

package org.apache.flink.table.gateway.service.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.internal.ExecutableOperationContextImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.resource.ResourceManager;

import java.util.List;

/**
 * Backs {@link MaterializedTableLifecycle.Context} with a SQL Gateway session.
 *
 * <p>The counterpart to the Table API's own context: the lifecycle itself is shared, and only the
 * two ways of reaching a session differ. Everything but statement execution is the session state
 * the executor already hands to self-executing operations.
 */
@Internal
public class GatewayLifecycleContext extends ExecutableOperationContextImpl
        implements MaterializedTableLifecycle.Context {

    private final OperationExecutor operationExecutor;

    public GatewayLifecycleContext(OperationExecutor operationExecutor) {
        super(operationExecutor.getExecutableOperationContext());
        this.operationExecutor = operationExecutor;
    }

    @Override
    public TableResultInternal executeStatement(String statement, Configuration executionConfig) {
        // Go straight to a TableEnvironment rather than through OperationExecutor.executeStatement:
        // that path wraps the result in a ResultFetcher, which drops the JobClient and leaves only
        // a
        // job id string. The lifecycle wants the client, so it can control a job it just submitted
        // without re-attaching to the cluster.
        final ResourceManager resourceManager =
                operationExecutor.getSessionContext().getSessionState().resourceManager.copy();
        final TableEnvironmentInternal tableEnv =
                operationExecutor.getTableEnvironment(resourceManager, executionConfig);

        final List<Operation> operations = tableEnv.getParser().parse(statement);
        if (operations.size() != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected a single statement for a materialized table refresh, got %d: %s",
                            operations.size(), statement));
        }
        return tableEnv.executeInternal(operations.get(0));
    }
}

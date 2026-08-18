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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.Operation;

import java.util.List;
import java.util.function.Function;

/**
 * Backs {@link MaterializedTableLifecycle.Context} with a {@link TableEnvironment}.
 *
 * <p>Everything the lifecycle needs from the session other than statement execution already exists
 * on {@link ExecutableOperation.Context}, so this inherits that half and adds only the ability to
 * run a refresh statement under a different configuration.
 */
@Internal
class TableEnvironmentLifecycleContext extends ExecutableOperationContextImpl
        implements MaterializedTableLifecycle.Context {

    private final Function<Configuration, TableEnvironmentInternal> childEnvironmentFactory;

    TableEnvironmentLifecycleContext(
            ExecutableOperation.Context operationContext,
            Function<Configuration, TableEnvironmentInternal> childEnvironmentFactory) {
        super(operationContext);
        this.childEnvironmentFactory = childEnvironmentFactory;
    }

    @Override
    public TableResultInternal executeStatement(String statement, Configuration executionConfig) {
        // A refresh job runs under its own configuration — a different runtime mode, job name,
        // checkpoint interval or restore path — so it needs its own environment rather than a
        // mutated view of this one. The catalog, modules and resources are shared, so the
        // materialized table being refreshed is visible to it.
        final TableEnvironmentInternal childEnvironment =
                childEnvironmentFactory.apply(executionConfig);

        final List<Operation> operations = childEnvironment.getParser().parse(statement);
        if (operations.size() != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected a single statement for a materialized table refresh, got %d: %s",
                            operations.size(), statement));
        }
        return childEnvironment.executeInternal(operations.get(0));
    }
}

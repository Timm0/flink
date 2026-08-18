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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.MaterializedTableLifecycleFactoryUtil;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperation;

import javax.annotation.Nullable;

import java.net.URLClassLoader;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Adapts the SQL Gateway to the shared {@link MaterializedTableLifecycle}.
 *
 * <p>All the materialized table semantics that used to live here — the ordering of catalog writes
 * against job submission, the compensation when submission fails, savepoint-based suspend and
 * resume, workflow handling for full mode — moved into the lifecycle, so the gateway and a plain
 * {@code TableEnvironment} share one implementation. What remains is translation: a gateway session
 * in, a {@link ResultFetcher} out.
 *
 * <p>The lifecycle is discovered through the SPI rather than imported, because {@code
 * flink-sql-gateway} depends on the planner only at test scope. That indirection is also what lets
 * a vendor drop in their own implementation without touching the gateway.
 */
@Internal
public class MaterializedTableManager {

    private final MaterializedTableLifecycle lifecycle;

    public MaterializedTableManager(
            Configuration configuration, URLClassLoader userCodeClassLoader) {
        this.lifecycle =
                MaterializedTableLifecycleFactoryUtil.createMaterializedTableLifecycle(
                        configuration, userCodeClassLoader);
    }

    public void open() throws Exception {
        lifecycle.open();
    }

    public void close() throws Exception {
        lifecycle.close();
    }

    public ResultFetcher callMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            MaterializedTableOperation op) {
        TableResultInternal result =
                asGatewayFailure(
                        () ->
                                lifecycle.execute(
                                        new GatewayLifecycleContext(operationExecutor), op));
        return ResultFetcher.fromTableResult(handle, result, false);
    }

    public ResultFetcher refreshMaterializedTable(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            ObjectIdentifier materializedTableIdentifier,
            Map<String, String> staticPartitions,
            Map<String, String> dynamicOptions,
            boolean isPeriodic,
            @Nullable String scheduleTime) {
        TableResultInternal result =
                asGatewayFailure(
                        () ->
                                lifecycle.refresh(
                                        new GatewayLifecycleContext(operationExecutor),
                                        materializedTableIdentifier,
                                        staticPartitions,
                                        dynamicOptions,
                                        isPeriodic,
                                        scheduleTime));
        return ResultFetcher.fromTableResult(handle, result, false);
    }

    /**
     * Preserves the gateway's exception contract.
     *
     * <p>The lifecycle cannot throw a gateway type, so execution failures surface as {@link
     * TableException}. Callers of the gateway have always seen {@link SqlExecutionException} for
     * these, so translate. {@link org.apache.flink.table.api.ValidationException} is a sibling of
     * {@link TableException}, not a subclass, so it still propagates untouched — exactly as before.
     */
    private static TableResultInternal asGatewayFailure(
            Supplier<TableResultInternal> lifecycleCall) {
        try {
            return lifecycleCall.get();
        } catch (TableException e) {
            throw toGatewayException(e);
        }
    }

    /**
     * Translates a whole chain of {@link TableException}s, not just the outermost.
     *
     * <p>The lifecycle nests them — an inner "already suspended" inside an outer "failed to
     * suspend" — and callers assert on the root cause. Converting only the top would leave a {@link
     * TableException} at the bottom of a chain that used to be gateway exceptions throughout.
     * Causes of any other type are left alone.
     */
    private static SqlExecutionException toGatewayException(TableException e) {
        Throwable cause = e.getCause();
        Throwable translatedCause =
                cause instanceof TableException
                        ? toGatewayException((TableException) cause)
                        : cause;
        return new SqlExecutionException(e.getMessage(), translatedCause);
    }
}

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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.StartMode;

/**
 * A pending declaration of one materialized table, created by {@link Table#materializeInto(String,
 * MaterializedTableDescriptor)}.
 */
@PublicEvolving
public interface MaterializedPipeline {

    /**
     * The fully qualified identifier this declaration will create or alter.
     *
     * <p>Resolved against the current catalog and database at {@link Table#materializeInto}.
     */
    ObjectIdentifier getIdentifier();

    /**
     * Submits this declaration with the semantics of {@code CREATE OR ALTER MATERIALIZED TABLE}
     * without {@code START_MODE}: creates the materialized table, or alters an existing one, and
     * records the default start mode ({@code materialized-table.default-start-mode}). A changed
     * freshness, comment or partition keys has no effect on an existing materialized table. Fails
     * if a regular table, a view, or a temporary table or view exists at the path.
     *
     * <p>If the materialized table exists and its continuous refresh job is running, the job is
     * stopped with a savepoint and a new refresh job is started, which does not restore that
     * savepoint. This also happens if the declaration is unchanged. If writing the changes fails
     * after the stop, the materialized table is left suspended with its previous definition.
     *
     * <p>A suspended materialized table is altered and stays suspended, and its stored savepoint is
     * cleared, so a later {@code RESUME} starts the refresh job without state. A materialized table
     * in full refresh mode is altered without changing its refresh workflow.
     *
     * <p>Returns once the catalog entry is written and, if a continuous refresh job is started,
     * once that job has been submitted.
     *
     * <p>Equivalent to {@code execute(CreateMode.createOrAlter())}.
     */
    MaterializedTable execute();

    /**
     * Submits this declaration with the given start mode, i.e. with the semantics of {@code CREATE
     * OR ALTER MATERIALIZED TABLE ... START_MODE = ...}. It behaves like {@link #execute()}, but
     * records the given start mode instead of the default one. The start mode belongs to this call
     * only, a later {@link #execute()} records the default start mode again.
     *
     * <p>Equivalent to {@code execute(CreateMode.createOrAlter(), startMode)}.
     *
     * @throws ValidationException if the start mode is {@code FROM_TIMESTAMP} or {@code
     *     RESUME_OR_FROM_TIMESTAMP} without a timestamp; {@link StartMode#fromTimestamp} and {@link
     *     StartMode#resumeOrFromTimestamp} always set one
     */
    MaterializedTable execute(StartMode startMode);

    /**
     * Submits this declaration in the given mode and records the default start mode ({@code
     * materialized-table.default-start-mode}). With {@link CreateMode#failIfExists()}, this has the
     * semantics of {@code CREATE MATERIALIZED TABLE} without {@code START_MODE}. See {@link
     * #execute()}.
     */
    MaterializedTable execute(CreateMode mode);

    /**
     * Submits this declaration in the given mode with the given start mode, i.e. with the semantics
     * of {@code CREATE [OR ALTER] MATERIALIZED TABLE ... START_MODE = ...}. See {@link #execute()}.
     *
     * @throws ValidationException if the start mode is {@code FROM_TIMESTAMP} or {@code
     *     RESUME_OR_FROM_TIMESTAMP} without a timestamp; {@link StartMode#fromTimestamp} and {@link
     *     StartMode#resumeOrFromTimestamp} always set one
     */
    MaterializedTable execute(CreateMode mode, StartMode startMode);
}

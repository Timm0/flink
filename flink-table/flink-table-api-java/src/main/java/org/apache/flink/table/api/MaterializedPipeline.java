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
public interface MaterializedPipeline extends Explainable<MaterializedPipeline> {

    /**
     * The fully qualified identifier this declaration will create or alter.
     *
     * <p>Resolved against the current catalog and database at {@link Table#materializeInto}.
     */
    ObjectIdentifier getIdentifier();

    /**
     * Submits this declaration with {@link CreateMode#createOrAlter()} and the default start mode
     * ({@code materialized-table.default-start-mode}), i.e. with the semantics of {@code CREATE OR
     * ALTER MATERIALIZED TABLE} without {@code START_MODE}.
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
     * <p>Equivalent to {@code execute(CreateMode.createOrAlter())}. Returns once the catalog entry
     * is written and, if a continuous refresh job is started, once that job has been submitted.
     */
    MaterializedTable execute();

    /**
     * Submits this declaration with {@link CreateMode#createOrAlter()} and the given start mode,
     * i.e. with the semantics of {@code CREATE OR ALTER MATERIALIZED TABLE ... START_MODE = ...}.
     */
    MaterializedTable execute(StartMode startMode);

    /**
     * Submits this declaration in the given mode, with the default start mode. See {@link
     * #execute()} for what {@link CreateMode#createOrAlter()} does to an existing materialized
     * table.
     */
    MaterializedTable execute(CreateMode mode);

    /**
     * Submits this declaration in the given mode, with the given start mode. The start mode belongs
     * to this call only. See {@link #execute()} for what {@link CreateMode#createOrAlter()} does to
     * an existing materialized table.
     */
    MaterializedTable execute(CreateMode mode, StartMode startMode);
}

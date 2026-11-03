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
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Optional;

/**
 * A handle on an existing materialized table, returned by {@link MaterializedPipeline#execute()}
 * and {@link TableEnvironment#fromMaterializedTable(String)}.
 */
@PublicEvolving
public interface MaterializedTable extends Explainable<MaterializedTable> {

    /** The fully qualified identifier of the materialized table. */
    ObjectIdentifier getIdentifier();

    /**
     * ACTIVATED, SUSPENDED or INITIALIZING.
     *
     * <p>ACTIVATED means the refresh pipeline was started (a continuous refresh job submitted, or a
     * full-refresh workflow scheduled), not that it is running. It is read from the catalog on
     * every call.
     */
    RefreshStatus getRefreshStatus();

    /**
     * Original text of the materialized table definition that preserves the original formatting.
     */
    String getOriginalQuery();

    /** Expanded text of the original materialized table definition with resolved identifiers. */
    String getExpandedQuery();

    /**
     * The freshness in effect, which may differ from the declared one where the engine normalizes
     * it.
     */
    IntervalFreshness getFreshness();

    /** CONTINUOUS or FULL. */
    RefreshMode getRefreshMode();

    /**
     * Display-only summary of the refresh handler. The refresh job of a CONTINUOUS table, or the
     * scheduled workflow of a FULL table. Empty if the catalog records none. It is read from the
     * catalog on every call.
     */
    Optional<String> getRefreshHandlerDescription();

    /**
     * Reads this table's storage, as {@link TableEnvironment#from(String)} does with {@link
     * #getIdentifier()}. Use it to query the table or to build downstream tables on it.
     *
     * @throws ValidationException if no table exists at {@link #getIdentifier()}
     */
    Table toTable();

    /**
     * The query that defined this table when this handle was created, for deriving a modified
     * definition, e.g. with {@link Table#materializeInto(String, MaterializedTableDescriptor)}.
     *
     * <p>The stored query text is planned when this method is called. Every object it references
     * must be resolvable in this environment under the name it had when the table was declared,
     * including the catalog's name.
     *
     * @throws ValidationException if the stored query cannot be planned
     */
    Table toQuery();

    /**
     * This table's definition when this handle was created, as a descriptor for re-declaring it,
     * e.g. with {@link Table#materializeInto(String, MaterializedTableDescriptor)}.
     *
     * <p>Reflects the resolved state: the full schema, the options, distribution, partition keys
     * and comment as stored, the freshness, the refresh mode in effect as {@link
     * RefreshStrategy#continuous()} or {@link RefreshStrategy#full()}, and the recorded start mode.
     * Re-declaring with this descriptor records that start mode again. Use {@code
     * MaterializedPipeline#execute(StartMode)} to override it instead.
     */
    MaterializedTableDescriptor toDescriptor();
}

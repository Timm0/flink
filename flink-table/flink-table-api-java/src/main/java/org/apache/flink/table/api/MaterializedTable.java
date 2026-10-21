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
 * A handle on an existing materialized table, returned by {@link MaterializedPipeline#execute()}.
 */
@PublicEvolving
public interface MaterializedTable {

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
}

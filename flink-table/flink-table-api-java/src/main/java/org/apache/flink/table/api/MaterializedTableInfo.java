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

import java.util.Optional;

/**
 * A snapshot of a materialized table's definition and refresh state, as the catalog holds it.
 *
 * <p>Read once per {@link MaterializedTable#info()} call, so the values within one instance are
 * consistent with each other.
 */
@PublicEvolving
public interface MaterializedTableInfo {

    /** {@code ACTIVATED}, {@code SUSPENDED} or {@code INITIALIZING}. */
    RefreshStatus getRefreshStatus();

    /**
     * Original text of the materialized table definition, preserving the formatting it was declared
     * with.
     */
    String getOriginalQuery();

    /** Expanded text of the materialized table definition, with resolved identifiers. */
    String getExpandedQuery();

    /**
     * The freshness in effect, which may differ from the declared one where the engine normalises
     * it.
     */
    IntervalFreshness getFreshness();

    /** {@code CONTINUOUS} or {@code FULL}. */
    RefreshMode getRefreshMode();

    /** Display-only summary of the handle behind the refresh — a job or a schedule. */
    Optional<String> getRefreshHandlerDescription();
}

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

import java.util.Map;
import java.util.Optional;

/**
 * A handle to a materialized table that exists in a catalog.
 *
 * <p>Obtained from {@link Materialization#execute()} or from {@link
 * TableEnvironment#materializedTable(String)}. The handle carries no cached state: every method
 * reads or acts on the catalog and the cluster at the moment it is called, so a handle stays valid
 * across suspends, resumes and refreshes.
 */
@PublicEvolving
public interface MaterializedTable {

    ObjectIdentifier getIdentifier();

    /** This materialized table as a readable query, for deriving downstream tables. */
    Table asTable();

    /** A consistent snapshot of this table's definition and refresh state. */
    MaterializedTableInfo info();

    /**
     * The continuous refresh job, if one is running.
     *
     * <p>Not limited to jobs this session submitted: a job started by an earlier session is
     * reachable too, resolved from the table's persisted refresh handler.
     */
    Optional<RefreshJob> getRefreshJob();

    /** Equivalent to {@code ALTER MATERIALIZED TABLE ... SUSPEND}. */
    void suspend();

    /**
     * Equivalent to {@code ALTER MATERIALIZED TABLE ... SUSPEND}.
     *
     * @param ignoreIfSuspended when true, returns quietly if the table is already suspended instead
     *     of failing
     */
    void suspend(boolean ignoreIfSuspended);

    /** Equivalent to {@code ALTER MATERIALIZED TABLE ... RESUME}. */
    void resume();

    /**
     * Equivalent to {@code ALTER MATERIALIZED TABLE ... RESUME}.
     *
     * @param ignoreIfActive when true, returns quietly if the table is already active instead of
     *     failing
     */
    void resume(boolean ignoreIfActive);

    /** Equivalent to {@code ALTER MATERIALIZED TABLE ... RESUME WITH (...)}. */
    void resume(Map<String, String> dynamicOptions);

    /**
     * Equivalent to {@code ALTER MATERIALIZED TABLE ... RESUME WITH (...)}.
     *
     * @param ignoreIfActive when true, returns quietly if the table is already active instead of
     *     failing
     */
    void resume(Map<String, String> dynamicOptions, boolean ignoreIfActive);

    /**
     * Equivalent to {@code ALTER MATERIALIZED TABLE ... REFRESH}.
     *
     * <p>Returns as soon as the refresh job is submitted. Call {@link RefreshJob#await()} on the
     * result to wait for it.
     */
    RefreshJob refresh();

    /** Equivalent to {@code ALTER MATERIALIZED TABLE ... REFRESH PARTITION (...)}. */
    RefreshJob refresh(Map<String, String> partitionSpec);

    /** Equivalent to {@code DROP MATERIALIZED TABLE}. */
    void drop();

    /**
     * Equivalent to {@code DROP MATERIALIZED TABLE [IF EXISTS]}.
     *
     * @param ignoreIfNotExists when true, returns quietly if the table does not exist
     */
    void drop(boolean ignoreIfNotExists);
}

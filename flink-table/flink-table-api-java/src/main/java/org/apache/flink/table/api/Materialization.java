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

/**
 * A materialized table declaration that has not been submitted yet.
 *
 * <p>Returned by {@link Table#materializeAs(String, MaterializedTableDescriptor)}. Nothing reaches
 * the catalog and no refresh job starts until {@link #execute()} is called, so a declaration can be
 * built up, passed around and submitted conditionally.
 */
@PublicEvolving
public interface Materialization {

    /** The resolved target, qualified against the session's current catalog and database. */
    ObjectIdentifier getIdentifier();

    /**
     * Submits this declaration with {@link CreateMode#CREATE_OR_UPDATE}, which is idempotent: a
     * declaration identical to what is already deployed changes nothing and leaves a running
     * refresh job untouched.
     */
    MaterializedTable execute();

    /** Submits this declaration in the given mode. */
    MaterializedTable execute(CreateMode mode);
}

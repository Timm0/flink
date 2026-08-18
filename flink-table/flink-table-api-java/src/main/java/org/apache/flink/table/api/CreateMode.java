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

/** How a {@link Materialization} treats whatever already exists at its path. */
@PublicEvolving
public enum CreateMode {

    /**
     * Create the table, or alter an existing materialized table to match this declaration. The
     * default.
     *
     * <p>Idempotent: re-running an unchanged declaration changes nothing and leaves a running
     * refresh job untouched.
     */
    CREATE_OR_UPDATE,

    /** Create the table; fail if anything already exists at the path. */
    CREATE_OR_FAIL
}

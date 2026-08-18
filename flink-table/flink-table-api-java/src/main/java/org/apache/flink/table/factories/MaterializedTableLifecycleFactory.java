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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;

/**
 * Creates a {@link MaterializedTableLifecycle}.
 *
 * <p>Discovered through {@link FactoryUtil#discoverFactory} by the identifier configured under
 * {@code materialized-table.lifecycle.type}. When that option is unset, Flink's own implementation
 * is used, so materialized tables work without any configuration.
 *
 * @see MaterializedTableLifecycleFactoryUtil
 */
@Internal
public interface MaterializedTableLifecycleFactory extends Factory {

    /** Identifier of Flink's own implementation, and the default when the option is unset. */
    String DEFAULT_IDENTIFIER = "default";

    /**
     * Creates a lifecycle instance. Called once per session; the caller owns opening and closing.
     *
     * @param configuration the full session configuration — a vendor implementation reads its own
     *     options from here
     * @param userClassLoader used to deserialize persisted refresh handlers and to discover a
     *     configured workflow scheduler
     */
    MaterializedTableLifecycle createMaterializedTableLifecycle(
            ReadableConfig configuration, ClassLoader userClassLoader);
}

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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;

/** Utility for discovering and creating a {@link MaterializedTableLifecycle}. */
@Internal
public class MaterializedTableLifecycleFactoryUtil {

    public static final ConfigOption<String> MATERIALIZED_TABLE_LIFECYCLE_TYPE =
            ConfigOptions.key("materialized-table.lifecycle.type")
                    .stringType()
                    .defaultValue(MaterializedTableLifecycleFactory.DEFAULT_IDENTIFIER)
                    .withDescription(
                            "The implementation that executes the materialized table lifecycle "
                                    + "(create, suspend, resume, refresh, drop). Defaults to Flink's "
                                    + "own implementation; vendors whose control plane owns job "
                                    + "submission and scheduling register their own identifier here.");

    private MaterializedTableLifecycleFactoryUtil() {
        // no instantiation
    }

    /**
     * Discovers the configured lifecycle factory and creates an instance.
     *
     * <p>Unlike {@link WorkflowSchedulerFactoryUtil#createWorkflowScheduler}, this never returns
     * {@code null}: a materialized table cannot do anything useful without a lifecycle, so a
     * missing implementation is an error rather than an optional feature being switched off.
     *
     * <p>Call this lazily, when a materialized table operation actually arrives. The default
     * implementation ships in the planner, which in a distribution sits behind the planner loader —
     * discovering it eagerly would make every {@code TableEnvironment} pay for it.
     */
    public static MaterializedTableLifecycle createMaterializedTableLifecycle(
            ReadableConfig configuration, ClassLoader classLoader) {
        final String identifier = configuration.get(MATERIALIZED_TABLE_LIFECYCLE_TYPE);

        final MaterializedTableLifecycleFactory factory;
        try {
            factory =
                    FactoryUtil.discoverFactory(
                            classLoader, MaterializedTableLifecycleFactory.class, identifier);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Could not find a materialized table lifecycle for identifier '%s'. "
                                    + "Materialized tables require the Flink planner on the classpath, "
                                    + "or a vendor implementation registered under this identifier via '%s'.",
                            identifier, MATERIALIZED_TABLE_LIFECYCLE_TYPE.key()),
                    t);
        }

        try {
            return factory.createMaterializedTableLifecycle(configuration, classLoader);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("Error creating materialized table lifecycle '%s'.", identifier),
                    t);
        }
    }
}

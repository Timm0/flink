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

package org.apache.flink.table.planner.loader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.MaterializedTableLifecycleFactory;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;

import java.util.Set;

/**
 * Delegate of {@link MaterializedTableLifecycleFactory}.
 *
 * <p>Without this, a distribution could not run materialized tables at all: the implementation
 * ships inside the planner fat jar, which is only reachable through the planner loader's
 * classloader.
 *
 * <p>Unlike its sibling delegates, this one resolves the planner factory on first use rather than
 * in its constructor. {@link java.util.ServiceLoader} instantiates every provider listed in the
 * services file while looking for any one of them, so a constructor that can fail would take {@code
 * ExecutorFactory} and {@code PlannerFactory} discovery down with it — for instance against a
 * planner jar built before materialized tables moved into it. A missing materialized table
 * implementation must cost you materialized tables, not the planner. The identifier is a constant
 * for the same reason: discovery must not need the planner loaded.
 */
@Internal
public class DelegateMaterializedTableLifecycleFactory
        implements MaterializedTableLifecycleFactory {

    private volatile MaterializedTableLifecycleFactory delegate;

    @Override
    public String factoryIdentifier() {
        return DEFAULT_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return getDelegate().requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return getDelegate().optionalOptions();
    }

    @Override
    public MaterializedTableLifecycle createMaterializedTableLifecycle(
            ReadableConfig configuration, ClassLoader userClassLoader) {
        return getDelegate().createMaterializedTableLifecycle(configuration, userClassLoader);
    }

    private MaterializedTableLifecycleFactory getDelegate() {
        MaterializedTableLifecycleFactory current = delegate;
        if (current == null) {
            synchronized (this) {
                current = delegate;
                if (current == null) {
                    current = PlannerModule.getInstance().loadMaterializedTableLifecycleFactory();
                    delegate = current;
                }
            }
        }
        return current;
    }
}

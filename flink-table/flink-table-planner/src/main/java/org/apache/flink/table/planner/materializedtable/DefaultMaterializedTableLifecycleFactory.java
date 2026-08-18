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

package org.apache.flink.table.planner.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.MaterializedTableLifecycleFactory;
import org.apache.flink.table.materializedtable.MaterializedTableLifecycle;

import java.util.Collections;
import java.util.Set;

/** Factory for Flink's own {@link DefaultMaterializedTableLifecycle}. */
@Internal
public class DefaultMaterializedTableLifecycleFactory implements MaterializedTableLifecycleFactory {

    @Override
    public String factoryIdentifier() {
        return DEFAULT_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public MaterializedTableLifecycle createMaterializedTableLifecycle(
            ReadableConfig configuration, ClassLoader userClassLoader) {
        // the workflow scheduler is discovered from the full session configuration, because it is a
        // separate top-level plugin rather than an option of this one
        return new DefaultMaterializedTableLifecycle(
                Configuration.fromMap(configuration.toMap()), userClassLoader);
    }
}

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

package org.apache.flink.table.planner.materializedtable.workflow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.WorkflowSchedulerFactory;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.Collections;
import java.util.Set;

/**
 * Factory for {@link InProcessWorkflowScheduler}, registered under {@code in-process}.
 *
 * <p>Deliberately a different identifier from the SQL Gateway's {@code embedded} scheduler: the
 * gateway has both on its classpath and {@link
 * org.apache.flink.table.factories.FactoryUtil#discoverFactory} rejects an ambiguous identifier.
 */
@Internal
public class InProcessWorkflowSchedulerFactory implements WorkflowSchedulerFactory {

    public static final String IDENTIFIER = "in-process";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
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
    public WorkflowScheduler<?> createWorkflowScheduler(Context context) {
        return new InProcessWorkflowScheduler();
    }
}

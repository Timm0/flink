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
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/** {@link RefreshHandlerSerializer} for {@link InProcessRefreshHandler}. */
@Internal
public class InProcessRefreshHandlerSerializer
        implements RefreshHandlerSerializer<InProcessRefreshHandler> {

    public static final InProcessRefreshHandlerSerializer INSTANCE =
            new InProcessRefreshHandlerSerializer();

    @Override
    public byte[] serialize(InProcessRefreshHandler refreshHandler) throws IOException {
        return InstantiationUtil.serializeObject(refreshHandler);
    }

    @Override
    public InProcessRefreshHandler deserialize(byte[] serializedBytes, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        return InstantiationUtil.deserializeObject(serializedBytes, cl);
    }
}

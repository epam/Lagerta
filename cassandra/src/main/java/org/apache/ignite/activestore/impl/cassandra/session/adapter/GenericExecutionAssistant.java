/*
 * Copyright (c) 2017. EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.activestore.impl.cassandra.session.adapter;

import org.apache.ignite.cache.store.cassandra.session.ExecutionAssistant;

/**
 * Assistant used for execute query operations.
 */
public abstract class GenericExecutionAssistant<T> extends GenericAssistant implements ExecutionAssistant<T> {
    /**
     * Creates assistant with given settings.
     *
     * @param tableExistenceRequired is existing table needed
     * @param operationName name of operation
     */
    public GenericExecutionAssistant(boolean tableExistenceRequired, String operationName) {
        super(tableExistenceRequired, operationName);
    }
}

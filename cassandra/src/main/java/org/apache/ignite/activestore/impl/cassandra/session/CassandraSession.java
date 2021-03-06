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

package org.apache.ignite.activestore.impl.cassandra.session;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.cache.store.cassandra.session.BatchExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.session.BatchLoaderAssistant;
import org.apache.ignite.cache.store.cassandra.session.ExecutionAssistant;

/**
 * Wrapper around Cassandra driver session, to automatically handle: <ul> <li>Keyspace and table absence exceptions</li>
 * <li>Timeout exceptions</li> <li>Batch operations</li> </ul>
 */
public interface CassandraSession extends Closeable {
    /**
     * Execute single synchronous operation against Cassandra  database.
     *
     * @param assistant execution assistance to perform the main operation logic.
     * @param <V> type of the result returned from operation.
     * @return result of the operation.
     */
    public <V> V execute(ExecutionAssistant<V> assistant);

    /**
     * Executes batch asynchronous operation against Cassandra database.
     *
     * @param assistant execution assistance to perform the main operation logic.
     * @param data data which should be processed in batch operation.
     * @param <R> type of the result returned from batch operation.
     * @param <V> type of the value used in batch operation.
     * @return result of the operation.
     */
    public <R, V> R execute(BatchExecutionAssistant<R, V> assistant, Iterable<? extends V> data);

    /**
     * Executes batch asynchronous operation to load bunch of records specified by CQL statement from Cassandra
     * database
     *
     * @param assistant execution assistance to perform the main operation logic.
     */
    public void execute(BatchLoaderAssistant assistant);

    public void execute(TransactionExecutionAssistance assistant, Map<String, Collection<Cache.Entry<?, ?>>> mutations);

    public void executeAllRows(ExecutionAssistant<Void> assistant);

    public void truncate(String keyspace, String table);
}

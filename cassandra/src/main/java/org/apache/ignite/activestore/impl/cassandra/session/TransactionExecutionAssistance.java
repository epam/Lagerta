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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import javax.cache.Cache;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

public interface TransactionExecutionAssistance {
    /**
     * Indicates if Cassandra tables existence is required for this batch operation.
     *
     * @return {@code true} true if table existence required.
     */
    public boolean tableExistenceRequired();

    /**
     * Persistence settings to use for operation.
     *
     * @return persistence settings.
     */
    public KeyValuePersistenceSettings getPersistenceSettings(String cache);

    /**
     * Returns unbind CLQ statement for to be executed inside batch operation.
     *
     * @return Unbind CQL statement.
     */
    public String getStatement(String cache);

    /**
     * Binds prepared statement to current Cassandra session.
     *
     * @param cache cache name for statement binding.
     * @param statement Statement.
     * @return Bounded statement.
     */
    public BoundStatement bindStatement(String cache, PreparedStatement statement, Cache.Entry entry);
}

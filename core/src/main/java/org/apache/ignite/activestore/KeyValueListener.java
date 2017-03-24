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

package org.apache.ignite.activestore;

import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;
import java.util.Map;

/**
 * @author Aleksandr_Meterko
 * @since 10/26/2016
 */
public interface KeyValueListener {

    /**
     * Writes changed key-value pairs from different caches transactionally.
     *
     * @param transactionId id to address current transaction.
     * @param updates mapping of cache name to collection of changed key-value pairs.
     * @throws CacheWriterException in case of errors during writing.
     */
    void writeTransaction(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) throws CacheWriterException;

    /**
     * Writes empty transaction that was missed before because of id sequencer fault.
     *
     * @param transactionId id to address current transaction.
     */
    void writeGapTransaction(long transactionId);
}

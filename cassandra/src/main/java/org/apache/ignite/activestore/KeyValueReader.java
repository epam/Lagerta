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

import java.util.Map;
import javax.cache.integration.CacheLoaderException;

/**
 * @author Aleksandr_Meterko
 * @since 10/26/2016
 */
public interface KeyValueReader {

    /**
     * Loads single entry from storage.
     *
     * @param key of cache entry.
     * @param cacheName name of cache to which data will be loaded.
     * @param path metadatas where value for the key should be looked.
     * @param <V> type of value for this cache.
     * @return value for specified key or null if nothing found.
     * @throws CacheLoaderException in case of errors during loading.
     */
    <K, V> V load(K key, String cacheName, Iterable<Metadata> path) throws CacheLoaderException;

    /**
     * Loads values for all keys for this cache.
     *
     * @param keys to be loaded.
     * @param cacheName name of cache to which data will be loaded.
     * @param path metadatas where value for the key should be looked.
     * @param <K> type of key for this cache.
     * @param <V> type of value for this cache.
     * @return map of loaded key-value pairs.
     * @throws CacheLoaderException in case of errors during loading.
     */
    <K, V> Map<K, V> loadAll(Iterable<? extends K> keys, String cacheName, Iterable<Metadata> path)
        throws CacheLoaderException;

}

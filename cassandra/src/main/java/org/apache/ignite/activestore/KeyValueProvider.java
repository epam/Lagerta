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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Provides logic for saving and loading Ignite key-value pairs from caches to or from underlying storage.
 */
public interface KeyValueProvider {
    /**
     * Writes changed key-value pairs from different caches transactionally.
     *
     * @param transactionId id to address current transaction.
     * @param updates mapping of cache name to collection of changed key-value pairs.
     * @param metadata snapshot to which we should write this data.
     * @throws CacheWriterException in case of errors during writing.
     */
    void write(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates, Metadata metadata)
        throws CacheWriterException;

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

    /**
     * Executes specified action on all keys from specified metadata and cache.
     *
     * @param cacheName to look keys.
     * @param metadata snapshot where we will check keys.
     * @param action to be performed on found keys.
     * @throws CacheLoaderException in case of errors during loading.
     */
    void fetchAllKeys(String cacheName, Metadata metadata, IgniteInClosure<Object> action) throws CacheLoaderException;

    /**
     * Executes specified action on all key-value pairs from specified metadata and cache.
     *
     * @param cacheName to look keys.
     * @param metadata snapshot where we will check keys.
     * @param action to be performed on found keys.
     */
    void fetchAllKeyValues(String cacheName, Metadata metadata, IgniteInClosure<Cache.Entry<Object, Object>> action);

    /**
     * Analyzes provided metadatas and to specify metadatas which involve specific caches.
     *
     * @param metadatas to analyze.
     * @return mapping of cache name to those metadatas from input which contain entry for this cache name.
     */
    Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas);
}

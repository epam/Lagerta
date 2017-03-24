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

package org.apache.ignite.activestore.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * In-memory implementation of provider.
 */
public class InMemoryKeyValueProvider implements KeyValueProvider {
    /** {@inheritDoc} */
    @Override public <K, V> V load(K key, String cacheName, Iterable<Metadata> path) throws CacheLoaderException {
        for (Metadata metadata : path) {
            Object result = InMemoryStorage.getMap(cacheName, metadata).get(key);
            if (result != null) {
                return ActiveCacheStore.TOMBSTONE.equals(result) ? null : (V)result;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Map<K, V> loadAll(Iterable<? extends K> keys, String cacheName, Iterable<Metadata> path)
            throws CacheLoaderException {
        Map<K, V> result = new HashMap<>();
        for (Metadata metadata : path) {
            Map map = loadAll(keys, cacheName, metadata);
            List<K> next = new ArrayList<>();
            for (K key : keys) {
                Object value = map.get(key);
                if (value == null) {
                    next.add(key);
                }
                else {
                    result.put(key, ActiveCacheStore.TOMBSTONE.equals(value) ? null : (V)value);
                }
            }
            if (next.isEmpty()) {
                break;
            }
            else {
                keys = next;
            }
        }
        return result;
    }

    private Map loadAll(Iterable<?> keys, String cacheName, Metadata metadata) {
        Map fromStore = InMemoryStorage.getMap(cacheName, metadata);
        Map result = new HashMap();
        for (Object key : keys) {
            Object value = fromStore.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override public void write(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates,
                                Metadata metadata) throws CacheWriterException {
        for (Map.Entry<String, Collection<Cache.Entry<?, ?>>> entry : updates.entrySet()) {
            Map map = InMemoryStorage.getMap(entry.getKey(), metadata);
            for (Cache.Entry<?, ?> entry1 : entry.getValue()) {
                map.put(entry1.getKey(), entry1.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void fetchAllKeys(String cacheName, Metadata metadata,
        IgniteInClosure<Object> action) throws CacheLoaderException {
        Map map = InMemoryStorage.getMap(cacheName, metadata);
        for (Object key : map.keySet()) {
            action.apply(key);
        }
    }

    /** {@inheritDoc} */
    @Override public void fetchAllKeyValues(String cacheName, Metadata metadata,
        IgniteInClosure<Cache.Entry<Object, Object>> action) {
        Map<?, ?> map = InMemoryStorage.getMap(cacheName, metadata);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            action.apply(new CacheEntryImpl<>(entry.getKey(), entry.getValue()));
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas) {
        Lazy<String, List<Metadata>> result = new Lazy<>(new IgniteClosure<String, List<Metadata>>() {
            @Override public List<Metadata> apply(String s) {
                return new ArrayList<>();
            }
        });
        for (Metadata metadata : metadatas) {
            Set<String> changedCaches = InMemoryStorage.findChangedKeysByCache(metadata).keySet();
            for (String changedCache : changedCaches) {
                result.get(changedCache).add(metadata);
            }
        }
        return result.toMap();
    }
}

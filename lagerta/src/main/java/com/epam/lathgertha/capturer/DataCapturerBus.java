/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.capturer;

import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.transactions.Transaction;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration point of active cache store and Ignite. Links persistence storage with caches using {@link CacheStore}
 * mechanism.
 */
@SuppressWarnings({"unchecked", "unused"})
public class DataCapturerBus<K, V> implements CacheStore<K, V> {
    /**
     * Mark for deleted key-values. UUID version 5 for DNS namespace "deleted.ignite.apache.org"
     */
    public static final UUID TOMBSTONE = UUID.fromString("45ffae47-3193-5910-84a2-048fe65735d9");

    /**
     * Name of session property which contains information about caches changed during transaction.
     */
    private static final String CACHES_PROPERTY_NAME = "CACHES_PROPERTY_NAME";

    /**
     * Name of session property which contains information about entries changed in specific cache during transaction.
     */
    private static final String BUFFER_PROPERTY_NAME = "BUFFER_PROPERTY_NAME";

    /** */
    private static final String ON_DR_FLAG_PROPERTY_NAME = "ON_DR_FLAG_PROPERTY_NAME";

    /**
     * Auto-injected store session.
     */
    @CacheStoreSessionResource
    CacheStoreSession session;

    /**
     * Auto-injected cache name.
     */
    @CacheNameResource
    String cacheName;

    /**
     * List of all listeners.
     */
    @SpringResource(resourceName = "key-value-listeners")
    List<ModificationListener> allListeners;

    /**
     * id sequence provider.
     */
    @SpringResource(resourceName = "id-sequencer")
    IdSequencer sequencer;

    /**
     * bean to load data from persistent store
     */
    @SpringResource(resourceName = "cache-loader")
    DataCapturerLoader loader;

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadCache(IgniteBiInClosure<K, V> clo, Object... args) throws CacheLoaderException {
        /* No-op. */
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V load(K key) throws CacheLoaderException {
        return loader.load(cacheName, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        return loader.loadAll(cacheName, keys);
    }

    /**
     * Gets buffer which holds changes for specific cache made during transaction.
     *
     * @return buffer.
     */
    private Collection<Cache.Entry<?, ?>> getBuffer() {
        Map<Object, Object> properties = session.properties();
        Set<String> caches = (Set<String>) properties.get(CACHES_PROPERTY_NAME);
        if (caches == null) {
            properties.put(CACHES_PROPERTY_NAME, caches = new HashSet<>());
            properties.put(BUFFER_PROPERTY_NAME, new HashMap<String, Map>());
        }
        Map<String, Collection<Cache.Entry<?, ?>>> buffer = (Map<String, Collection<Cache.Entry<?, ?>>>) properties.get(BUFFER_PROPERTY_NAME);
        if (caches.add(cacheName)) {
            Collection<Cache.Entry<?, ?>> cacheBuffer = new ArrayList<>();
            buffer.put(cacheName, cacheBuffer);
            return cacheBuffer;
        } else {
            return buffer.get(cacheName);
        }
    }

    private void notifyListeners(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        for (ModificationListener listener : allListeners) {
            listener.handle(transactionId, updates);
        }
    }

    /**
     * Performs preparation for or actual write to persistence storage.
     *
     * @param key   in cache.
     * @param value for that key.
     */
    private void put(Object key, Object value) {
        Transaction transaction = session.transaction();
        if (transaction == null) {
            Collection<Cache.Entry<?, ?>> entries = Collections.singletonList(new CacheEntryImpl(key, value));
            notifyListeners(nextTransactionId(), Collections.singletonMap(cacheName, entries));
        } else {
            getBuffer().add(new CacheEntryImpl<>(key, value));
        }
    }

    /**
     * Performs preparation for or actual write to persistence storage.
     *
     * @param entries changed in cache.
     */
    private void putAll(Collection<Cache.Entry<?, ?>> entries) {
        Transaction transaction = session.transaction();
        if (transaction == null) {
            notifyListeners(nextTransactionId(), Collections.singletonMap(cacheName, entries));
        } else {
            Collection<Cache.Entry<?, ?>> cacheBuffer = getBuffer();
            entries.forEach(cacheBuffer::add);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        put(entry.getKey(), entry.getValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
        putAll((Collection) entries);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Object key) throws CacheWriterException {
        put(key, TOMBSTONE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAll(Collection<?> keys) throws CacheWriterException {
        putAll(keys
                .stream()
                .map(key -> new CacheEntryImpl<Object, Object>(key, TOMBSTONE))
                .collect(Collectors.toList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionEnd(boolean commit) throws CacheWriterException {
        Transaction transaction = session.transaction();
        if (transaction == null) {
            return;
        }
        Map<Object, Object> properties = session.properties();
        if (!commit) {
            Map bigBuffer = (Map) properties.get(BUFFER_PROPERTY_NAME);
            if (bigBuffer != null) {
                bigBuffer.remove(cacheName);
            }
        }
        Set<String> caches = (Set<String>) properties.get(CACHES_PROPERTY_NAME);
        if (caches != null && caches.remove(cacheName) && caches.isEmpty()) {
            Map<String, Collection<Cache.Entry<?, ?>>> buffer =
                    (Map<String, Collection<Cache.Entry<?, ?>>>) properties.get(BUFFER_PROPERTY_NAME);
            notifyListeners(nextTransactionId(), buffer);
        }
    }

    private long nextTransactionId() {
        return sequencer.getNextId();
    }

}

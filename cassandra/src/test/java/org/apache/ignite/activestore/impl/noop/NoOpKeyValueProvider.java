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

package org.apache.ignite.activestore.impl.noop;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * @author Evgeniy_Ignatiev
 * @since 10/4/2016 6:41 PM
 */
public class NoOpKeyValueProvider implements KeyValueProvider {
    @Override
    public <K, V> V load(K key, String cacheName, Iterable<Metadata> path) throws CacheLoaderException {
        return null;
    }

    @Override
    public <K, V> Map<K, V> loadAll(Iterable<? extends K> keys, String cacheName,
        Iterable<Metadata> path) throws CacheLoaderException {
        return Collections.emptyMap();
    }

    @Override
    public void write(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates,
                      Metadata metadata) throws CacheWriterException {
    }

    @Override
    public void fetchAllKeys(String cacheName, Metadata metadata,
        IgniteInClosure<Object> action) throws CacheLoaderException {
        //do nothing;
    }

    @Override
    public void fetchAllKeyValues(String cacheName, Metadata metadata,
        IgniteInClosure<Cache.Entry<Object, Object>> action) {
        //do nothing;
    }

    @Override
    public Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas) {
        return Collections.emptyMap();
    }
}


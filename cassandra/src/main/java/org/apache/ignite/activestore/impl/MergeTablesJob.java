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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Job which performs merge operation over snapshots in one cache.
 */
class MergeTablesJob extends ComputeJobAdapter {
    /**
     * Name of cache which is merged.
     */
    private final String cacheName;

    /**
     * Metadatas which should be merged.
     */
    private final List<Metadata> metadatas;

    /**
     * Replacement for previously merged metadatas.
     */
    private final Metadata destination;

    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Auto-injected logger.
     */
    @LoggerResource
    private transient IgniteLogger log;

    /**
     * Auto-injected provider.
     */
    @Inject
    private KeyValueProvider keyValueProvider;

    /**
     * Auto-injected id generator.
     */
    @Inject
    private IdSequencer idSequencer;

    /**
     * Creates instance of job with given settings.
     *
     * @param cacheName Ignite cache name.
     * @param metadatas to be replaced.
     * @param destination replacement for metadatas.
     */
    public MergeTablesJob(String cacheName, List<Metadata> metadatas, Metadata destination) {
        this.cacheName = cacheName;
        this.metadatas = metadatas;
        this.destination = destination;
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        Injection.inject(this, ignite);
        final Map<Object, Cache.Entry<?, ?>> mergedData = new HashMap<>();
        for (Metadata metadata : metadatas) {
            keyValueProvider.fetchAllKeyValues(cacheName, metadata, new IgniteInClosure<Cache.Entry<Object, Object>>() {
                @Override public void apply(Cache.Entry<Object, Object> entry) {
                    Object key = entry.getKey();
                    if (!mergedData.containsKey(key)) {
                        mergedData.put(key, new CacheEntryImpl<>(key, entry.getValue()));
                    }
                }
            });
        }
        keyValueProvider.write(idSequencer.getNextId(), Collections.singletonMap(cacheName, mergedData.values()), destination);
        return null;
    }
}

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

import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Job which performs given action on all keys which are contained in storage for given cache and snapshot.
 */
class ProcessAllKeysJob extends ComputeJobAdapter {
    /**
     * Name of cache for which keys will be loaded.
     */
    private final String cacheName;

    /**
     * Snapshot to analyze for keys.
     */
    private final Metadata metadata;

    /**
     * Action which will be performed on found keys. Processes mapping of cache name to set of keys.
     */
    private final IgniteBiInClosure<String, Set<Object>> action;

    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Auto-injected provider.
     */
    @Inject
    private KeyValueProvider keyValueProvider;

    /**
     * Auto-injected logger.
     */
    @LoggerResource
    private transient IgniteLogger log;

    /**
     * Creates instance of job with given settings.
     *
     * @param cacheName Ignite cache name.
     * @param metadata to get keys from.
     * @param action to be performed.
     */
    public ProcessAllKeysJob(String cacheName, Metadata metadata, IgniteBiInClosure<String, Set<Object>> action) {
        this.cacheName = cacheName;
        this.metadata = metadata;
        this.action = action;
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        Injection.inject(this, ignite);
        final Set<Object> keys = new HashSet<>();
        keyValueProvider.fetchAllKeys(cacheName, metadata, new IgniteInClosure<Object>() {
            @Override public void apply(Object key) {
                keys.add(key);
            }
        });
        if (!keys.isEmpty()) {
            action.apply(cacheName, keys);
        }
        return null;
    }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Platform-agnostic implementation of {@link KeyValueManager}.
 */
public class KeyValueManagerImpl implements KeyValueManager {
    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Auto-injected logger.
     */
    @LoggerResource
    private IgniteLogger log;

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

    /** {@inheritDoc} */
    @Override public void createSnapshot(Metadata newSnapshot) {
        /* no-opt */
    }

    /** {@inheritDoc} */
    @Override public void invalidate(Iterable<Metadata> subPath, final IgniteBiInClosure<String, Set<Object>> action) {
        Map<String, List<Metadata>> names = getSnapshotsByCache(subPath);
        if (!names.isEmpty()) {
            ignite.compute().execute(new ComputeTaskSplitAdapter<Map<String, List<Metadata>>, Void>() {
                /** {@inheritDoc} */
                @Override protected Collection<? extends ComputeJob> split(int gridSize,
                    Map<String, List<Metadata>> byCache) throws IgniteException {
                    List<ComputeJob> result = new ArrayList<>();
                    for (Map.Entry<String, List<Metadata>> entry : byCache.entrySet()) {
                        String cacheName = entry.getKey();
                        for (Metadata metadata : entry.getValue()) {
                            result.add(new ProcessAllKeysJob(cacheName, metadata, action));
                        }
                    }
                    return result;
                }

                /** {@inheritDoc} */
                @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
                    return null;
                }
            }, names);
        }
    }

    /** {@inheritDoc} */
    @Override public void merge(Iterable<Metadata> sourcePath, final Metadata destination) {
        Map<String, List<Metadata>> names = getSnapshotsByCache(sourcePath);
        if (!names.isEmpty()) {
            ignite.compute().execute(new ComputeTaskSplitAdapter<Map<String, List<Metadata>>, Void>() {
                /** {@inheritDoc} */
                @Override protected Collection<? extends ComputeJob> split(int gridSize,
                    Map<String, List<Metadata>> names) throws IgniteException {
                    List<ComputeJob> result = new ArrayList<>(names.size());
                    for (Map.Entry<String, List<Metadata>> entry : names.entrySet()) {
                        result.add(new MergeTablesJob(entry.getKey(), entry.getValue(), destination));
                    }
                    return result;
                }

                /** {@inheritDoc} */
                @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
                    return null;
                }
            }, names);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas) {
        return keyValueProvider.getSnapshotsByCache(metadatas);
    }

    /** {@inheritDoc} */
    @Override public void backup(String cacheName, Metadata metadata, final Exporter.Writer writer) throws IOException {
        final IOException[] ioException = new IOException[1];
        try {
            keyValueProvider.fetchAllKeyValues(cacheName, metadata, new IgniteInClosure<Cache.Entry<Object, Object>>() {
                /** {@inheritDoc} */
                @Override public void apply(Cache.Entry<Object, Object> entry) {
                    try {
                        writer.write(entry.getKey(), entry.getValue());
                    }
                    catch (IOException e) {
                        ioException[0] = e;
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        catch (RuntimeException e) {
            if (ioException[0] != null) {
                throw ioException[0];
            }
            else {
                throw e;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<Map<Object, Object>> getDataWriter(final String cacheName,
        final Metadata metadata) {
        return Injection.inject(new IgniteInClosure<Map<Object, Object>>() {
            /**
             * Auto-injected provider.
             */
            @Inject
            private transient KeyValueProvider keyValueProvider;

            /** {@inheritDoc} */
            @Override public void apply(Map<Object, Object> map) {
                Collection<Cache.Entry<?, ?>> updates = new ArrayList<>(map.size());
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    updates.add(new CacheEntryImpl<>(entry.getKey(), entry.getValue()));
                }
                keyValueProvider.write(idSequencer.getNextId(), Collections.singletonMap(cacheName, updates), metadata);
            }
        }, ignite);
    }
}

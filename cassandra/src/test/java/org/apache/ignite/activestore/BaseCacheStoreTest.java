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

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import com.google.common.collect.Sets;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.rules.TestResourceFactory;
import org.apache.ignite.activestore.rules.TestResources;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

/**
 * Base class for all tests.
 */
public class BaseCacheStoreTest {
    /**
     * Prefix for all cache names created withing this test.
     */
    private static final String CACHE_PREFIX = "test_cache_";
    /** */
    @ClassRule
    public static TestResources resource = TestResourceFactory.getResource();
    /**
     * Number of created cache.
     */
    private static int CURRENT_CACHE_NUM = 0;
    /**
     * Name of first cache.
     */
    protected String firstCache;
    /**
     * Name of second cache.
     */
    protected String secondCache;

    /** */
    public static <K, V> IgniteCache<K, V> createCache(String cacheName, CacheAtomicityMode atomicityMode,
        CacheMode cacheMode, boolean writeBehindEnabled, int flushFreq,
        CacheConfiguration<K, V> base) {
        return resource.createCache(cacheName, atomicityMode, cacheMode, writeBehindEnabled, flushFreq, base);
    }

    /** */
    public static <K, V> IgniteCache<K, V> createCache(String cacheName, CacheAtomicityMode atomicityMode,
        CacheMode cacheMode, boolean writeBehindEnabled, int flushFreq) {
        return createCache(cacheName, atomicityMode, cacheMode, writeBehindEnabled, flushFreq, null);
    }

    /** */
    @Before
    public void setUpControllers() {
        resource.deployServices();
        firstCache = createCacheName();
        secondCache = createCacheName();
    }

    /**
     * Generate unique name for cache.
     */
    private String createCacheName() {
        int currentCache = CURRENT_CACHE_NUM++;
        return CACHE_PREFIX + currentCache;
    }

    /** */
    @After
    public void tearDownControllers() {
        resource.cancelServices();
        resource.destroyCachesIfExist(firstCache, secondCache);
        resource.callSpecificTearDown();
    }

    /**
     * Reads value from persistence storage.
     */
    protected Object readKV(Object key, String name, Metadata metadata) {
        return resource.getKVProvider().load(key, name, Collections.singletonList(metadata));
    }

    /**
     * Writes key-value to persistent store.
     */
    protected void writeKV(Object key, Object value, String cacheName, Metadata metadata) {
        Collection<Cache.Entry<?, ?>> entries = Collections.<Cache.Entry<?, ?>>singletonList(new CacheEntryImpl<>(key, value));
        resource.getKVProvider().write(resource.nextTransactionId(), Collections.singletonMap(cacheName, entries), metadata);
    }

    private void sleep() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }
    }

    /** */
    protected Metadata createSnapshotInCompute(final String label) {
        sleep();
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                resource.createSnapshot(label);
            }
        });
        return resource.head();
    }

    /** */
    protected Metadata rollbackInCompute(final String label) {
        sleep();
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                resource.rollback(label);
            }
        });
        return resource.head();
    }

    /** */
    protected Metadata mergeInCompute(final Metadata from, final Metadata to) {
        sleep();
        return resource.ignite().compute().call(new IgniteCallable<Metadata>() {
            @Override public Metadata call() throws Exception {
                resource.merge(from.getDefaultLabel(), to.getDefaultLabel());
                return resource.metadataTree().findByLabel(String.valueOf(from.getTimestamp()));
            }
        });
    }

    /** */
    protected Tree getTreeInCompute() {
        sleep();
        return resource.ignite().compute().call(new IgniteCallable<Tree>() {
            @Override public Tree call() throws Exception {
                return resource.metadataTree();
            }
        });
    }

    /** */
    protected void backupInCompute(final String destination) {
        sleep();
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                resource.backup(URI.create(destination));
            }
        });
    }

    /** */
    protected void incrementalBackupInCompute(final String destination, final String baseLabel) {
        sleep();
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                resource.incrementalBackup(URI.create(destination), baseLabel);
            }
        });
    }

    /** */
    protected void restoreInCompute(final String source) {
        sleep();
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                resource.restore(URI.create(source));
            }
        });
    }

    /** */
    protected Map<Object, Object> readFromCache(final String cacheName, final String... keys) {
        return resource.ignite().compute().call(new IgniteCallable<Map<Object, Object>>() {
            @Override public Map<Object, Object> call() throws Exception {
                IgniteCache<Object, Object> cache = Ignition.localIgnite().cache(cacheName);
                return cache.getAll(Sets.newHashSet(keys));
            }
        });
    }
}


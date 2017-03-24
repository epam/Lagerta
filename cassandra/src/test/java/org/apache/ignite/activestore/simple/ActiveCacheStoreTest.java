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

package org.apache.ignite.activestore.simple;

import java.util.Map;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.BaseCacheStoreTest;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Test;

/**
 * Basic test for active store-based tests on different caches.
 */
public abstract class ActiveCacheStoreTest extends BaseCacheStoreTest {
    /** */
    @Test
    public void testRead() {
        initializeKV();

        Object[] result = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> cache = Ignition.localIgnite().cache(firstCache);
                return new Object[] {
                    cache.get("a"),
                    cache.get("b"),
                    cache.get("c"),
                    cache.get("d"),
                    cache.get("e")
                };
            }
        });

        Assert.assertEquals("1", result[0]);
        Assert.assertEquals("3", result[1]);
        Assert.assertEquals("4", result[2]);
        Assert.assertNull(result[3]);
        Assert.assertNull(result[4]);
    }

    /** */
    @Test
    public void testReadAll() {
        initializeKV();

        Map result = readFromCache(firstCache, "a", "b", "c", "d", "e");

        Assert.assertEquals("1", result.get("a"));
        Assert.assertEquals("3", result.get("b"));
        Assert.assertEquals("4", result.get("c"));
        Assert.assertNull(result.get("d"));
        Assert.assertNull(result.get("e"));
    }

    /** */
    private void initializeKV() {
        Metadata snapshot1 = resource.head();
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "2", firstCache, snapshot1);
        createSnapshotInCompute("laterSnapshot");
        Metadata snapshot2 = resource.head();
        writeKV("b", "3", firstCache, snapshot2);
        writeKV("c", "4", firstCache, snapshot2);
        writeKV("d", ActiveCacheStore.TOMBSTONE, firstCache, snapshot2);
    }

    /** */
    @Test
    public void testReadInTransaction() {
        initializeKV();

        Object[] result = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @Override public Object[] call() throws Exception {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                Object[] objects;
                try (Transaction transaction = ignite.transactions().txStart()) {
                    objects = new Object[] {
                        cache.get("a"),
                        cache.get("b"),
                        cache.get("c"),
                        cache.get("d"),
                        cache.get("e")
                    };
                    transaction.commit();
                }
                return objects;
            }
        });

        Assert.assertEquals("1", result[0]);
        Assert.assertEquals("3", result[1]);
        Assert.assertEquals("4", result[2]);
        Assert.assertNull(result[3]);
        Assert.assertNull(result[4]);
    }

    /** */
    @Test
    public void testWrite() throws InterruptedException {
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                cache.put("a", "5");
                cache.put("b", "6");

                try (Transaction transaction = ignite.transactions().txStart()) {
                    cache.put("c", "7");
                    cache.remove("a");
                    cache.put("d", "8");
                    transaction.commit();
                }
            }
        });

        Map result = readFromCache(firstCache, "a", "b", "c", "d");

        Assert.assertNull(result.get("a"));
        Assert.assertEquals("6", result.get("b"));
        Assert.assertEquals("7", result.get("c"));
        Assert.assertEquals("8", result.get("d"));
    }

    /** */
    @Test
    public void testClear() {
        Metadata head = resource.head();
        writeKV("a", "1", firstCache, head);
        writeKV("b", "1", firstCache, head);

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignition.localIgnite().cache(firstCache).get("a");
            }
        });

        createSnapshotInCompute("laterSnapshot");

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    cache.put("a", "2");
                    cache.put("b", "2");
                    transaction.commit();
                }
            }
        });

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignition.localIgnite().cache(firstCache).clear();
            }
        });

        Map result = readFromCache(firstCache, "a", "b");

        Assert.assertEquals("2", result.get("a"));
        Assert.assertEquals("2", result.get("b"));
    }

    @Test
    public void deleteAll() {
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    cache.put("a", "1");
                    cache.put("b", "2");
                    cache.put("c", "3");
                    transaction.commit();
                }
            }
        });
        Map<Object, Object> cacheResult = readFromCache(firstCache, "a", "b", "c");
        Assert.assertEquals("1", cacheResult.get("a"));
        Assert.assertEquals("2", cacheResult.get("b"));
        Assert.assertEquals("3", cacheResult.get("c"));

        createSnapshotInCompute("newHead");

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    cache.removeAll(Sets.newHashSet("a", "b", "c"));
                    transaction.commit();
                }
            }
        });

        cacheResult = readFromCache(firstCache, "a", "b", "c");
        Assert.assertNull(cacheResult.get("a"));
        Assert.assertNull(cacheResult.get("b"));
        Assert.assertNull(cacheResult.get("c"));
    }

}

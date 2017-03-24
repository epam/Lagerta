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

package org.apache.ignite.activestore.complex;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.BaseCacheStoreTest;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests active store calls on eviction from cache.
 */
public class EvictionPolicyTest extends BaseCacheStoreTest {
    /** */
    @Before
    public void createCaches() {
        CacheConfiguration configuration = new CacheConfiguration<>();
        configuration.setEvictionPolicy(new FifoEvictionPolicy(5));
        createCache(firstCache, TRANSACTIONAL, PARTITIONED, false, 0, configuration);

        configuration = new CacheConfiguration<>();
        configuration.setEvictionPolicy(new FifoEvictionPolicy(101));
        createCache(secondCache, TRANSACTIONAL, PARTITIONED, false, 0, configuration);
    }

    /** */
    @Test
    public void testEviction() throws InterruptedException {
        resource.ignite().compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("a", 1);
                    a.put("b", 1);
                    a.put("c", 1);
                    transaction.commit();
                }
            }
        });

        Metadata head = resource.head();
        writeKV("a", 2, firstCache, head);
        writeKV("b", 2, firstCache, head);

        Map one = readFromCache(firstCache, "a", "b", "c");

        resource.ignite().compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    for (int i = 0; i < 100; i++) {
                        a.put(i, 1);
                    }
                    transaction.commit();
                }
            }
        });

        Map another = readFromCache(firstCache, "a", "b", "c");

        Assert.assertEquals(1, one.get("a"));
        Assert.assertEquals(1, one.get("b"));
        Assert.assertEquals(1, one.get("c"));

        Assert.assertEquals(2, another.get("a"));
        Assert.assertEquals(2, another.get("b"));
        Assert.assertEquals(1, another.get("c"));
    }

    /** */
    @Test
    public void testExpiryReadFromOutdatedPath() throws InterruptedException {
        Metadata head = resource.head();
        final int size = 100;
        for (int i = 0; i < size; i++) {
            writeKV(i, 1, secondCache, head);
        }

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                IgniteCache<Object, Object> a = Ignition.localIgnite().cache(secondCache);
                for (int i = 0; i < size; i++) {
                    a.get(i);
                }
            }
        });

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                resource.createSnapshot("snapshotFromClosure");
            }
        });

        resource.ignite().compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(secondCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    for (int i = 0; i < size; i++) {
                        a.put(i, 2);
                    }
                    transaction.commit();
                }

                a.put(-1, -1);
                a.put(-2, -1);
            }
        });

        Object[] actual = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> a = Ignition.localIgnite().cache(secondCache);
                Object[] result = new Object[size];
                for (int i = 0; i < size; i++) {
                    result[i] = a.get(i);
                }
                return result;
            }
        });

        for (int i = 0; i < size; i++) {
            Assert.assertEquals(2, actual[i]);
        }
    }
}

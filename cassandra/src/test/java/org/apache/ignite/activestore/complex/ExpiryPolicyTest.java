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

import java.util.concurrent.TimeUnit;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.BaseCacheStoreTest;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests active store calls on expiration of data in cache.
 */
public class ExpiryPolicyTest extends BaseCacheStoreTest {
    /** */
    @Before
    public void createCaches() {
        CacheConfiguration configuration = new CacheConfiguration<>();
        ModifiedExpiryPolicy expiryPolicy = new ModifiedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 100));
        configuration.setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicy));

        createCache(firstCache, TRANSACTIONAL, PARTITIONED, false, 0, configuration);
        createCache(secondCache, TRANSACTIONAL, PARTITIONED, false, 0, configuration);
    }

    /** */
    @Test
    public void testExpiry() throws InterruptedException {
        resource.ignite().compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                IgniteCache<Object, Object> b = ignite.cache(secondCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("a", 1);
                    a.put("b", 1);
                    b.put("a", 1);
                    b.put("b", 1);
                    transaction.commit();
                }
            }
        });

        Metadata head = resource.head();
        writeKV("a", 2, firstCache, head);
        writeKV("b", 2, firstCache, head);

        Object[] one = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> a = Ignition.localIgnite().cache(firstCache);
                IgniteCache<Object, Object> b = Ignition.localIgnite().cache(secondCache);
                return new Object[] {
                    a.get("a"),
                    a.get("b"),
                    b.get("a"),
                    b.get("b")
                };
            }
        });

        sleep(200);

        Object[] another = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> a = Ignition.localIgnite().cache(firstCache);
                IgniteCache<Object, Object> b = Ignition.localIgnite().cache(secondCache);
                return new Object[] {
                    a.get("a"),
                    a.get("b"),
                    b.get("a"),
                    b.get("b")
                };
            }
        });

        Assert.assertEquals(1, one[0]);
        Assert.assertEquals(1, one[1]);
        Assert.assertEquals(1, one[2]);
        Assert.assertEquals(1, one[3]);

        Assert.assertEquals(2, another[0]);
        Assert.assertEquals(2, another[1]);
        Assert.assertEquals(1, another[2]);
        Assert.assertEquals(1, another[3]);
    }

    /** */
    @Test
    public void testExpiryReadFromOutdatedPath() throws InterruptedException {
        Metadata head = resource.head();
        final int size = 100;
        for (int i = 0; i < size; i++) {
            writeKV(i, 1, firstCache, head);
        }

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                IgniteCache<Object, Object> a = Ignition.localIgnite().cache(firstCache);
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
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    for (int i = 0; i < size; i++) {
                        a.put(i, 2);
                    }
                    transaction.commit();
                }
            }
        });

        sleep(200);

        Object[] actual = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> a = Ignition.localIgnite().cache(firstCache);
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

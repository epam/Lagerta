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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Test;

import static java.lang.Thread.sleep;

/**
 * Base class for tests on transactional caches with write behind.
 */
public abstract class TransactionalWriteBehindACSTest extends WriteBehindACSTest {
    /** */
    @Test
    public void testRollbackTransaction() throws InterruptedException {
        Metadata oldHead = resource.head();

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                cache.put("a", "5");
                cache.put("b", "6");
            }
        });

        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                IgniteCache<Object, Object> cache = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    cache.put("c", cache.get("a") + "7");
                    cache.put("d", "8");
                    transaction.rollback();
                }
            }
        });

        Map result = readFromCache(firstCache, "a", "b", "c", "d");

        sleep(3000);

        Assert.assertEquals("5", result.get("a"));
        Assert.assertEquals("6", result.get("b"));
        Assert.assertNull(result.get("c"));
        Assert.assertNull(result.get("d"));

        Assert.assertEquals("5", readKV("a", firstCache, oldHead));
        Assert.assertEquals("6", readKV("b", firstCache, oldHead));
        Assert.assertNull(readKV("c", firstCache, oldHead));
        Assert.assertNull(readKV("d", firstCache, oldHead));
    }
}

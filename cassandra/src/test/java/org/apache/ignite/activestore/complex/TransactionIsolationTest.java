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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.BaseCacheStoreTest;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests active store with different transaction isolation level.
 */
@Ignore
public class TransactionIsolationTest extends BaseCacheStoreTest {
    /** */
    private static CyclicBarrier[] BARRIERS;

    /** */
    private static void await(int i) {
        if (BARRIERS[i] != null) {
            try {
                BARRIERS[i].await();
            }
            catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** */
    @Before
    public void createCaches() {
        CacheConfiguration configuration = new CacheConfiguration<>();
        configuration.setEvictionPolicy(new FifoEvictionPolicy(1));
        createCache(firstCache, CacheAtomicityMode.TRANSACTIONAL, CacheMode.PARTITIONED, false, 0, configuration);
    }

    /** */
    @Test
    public void testDirtyReadSimple1() {              //      N--N
        testDirtyReadSimple(OPTIMISTIC, READ_COMMITTED, 1, 1, 1, 3, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadSimple2() {              //       N--N
        testDirtyReadSimple(OPTIMISTIC, REPEATABLE_READ, 1, 1, 1, 3, 3, 3);
    }

    /** */
    @Test(expected = IgniteException.class)
    public void testDirtyReadSimple3() {
        testDirtyReadSimple(OPTIMISTIC, SERIALIZABLE, 1, 1, 1, 1, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadSimple4() {               //      N--N
        testDirtyReadSimple(PESSIMISTIC, READ_COMMITTED, 1, 1, 1, 3, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadSimple5() {
        testDirtyReadSimple(PESSIMISTIC, REPEATABLE_READ, 1, 1, 1, 1, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadSimple6() {
        testDirtyReadSimple(PESSIMISTIC, SERIALIZABLE, 1, 1, 1, 1, 3, 3);
    }

    /** */
    private void testDirtyReadSimple(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        Object... expected
    ) {
        //--- Initial state ----------------------------------------
        final Metadata oldHead = resource.head();
        writeKV("a", 1, firstCache, oldHead);
        writeKV("x", 1, firstCache, oldHead);

        Object[] call = resource.ignite().compute().call(new IgniteCallable<Object[]>() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                Object[] result = new Object[6];
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[0] = a.get("x");
                    transaction.commit();
                }
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[1] = a.get("a");
                    a.put("b", 2);
                    a.put("c", 2);
                    a.put("d", 2);
                    a.put("e", 2);
                    writeKV("a", 3, firstCache, oldHead);
                    writeKV("x", 3, firstCache, oldHead);
                    result[2] = a.get("a");
                    result[3] = a.get("x");
                    transaction.commit();
                }
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[4] = a.get("a");
                    result[5] = a.get("x");
                    transaction.commit();
                }
                return result;
            }
        });

        Assert.assertEquals(expected[0], call[0]); //x
        Assert.assertEquals(expected[1], call[1]); //a
        Assert.assertEquals(expected[2], call[2]); //a
        Assert.assertEquals(expected[3], call[3]); //x
        Assert.assertEquals(expected[4], call[4]); //a
        Assert.assertEquals(expected[5], call[5]); //x
    }

    /** */
    @Test
    public void testDirtyReadConcurrent1() {
        testDirtyReadConcurrent(OPTIMISTIC, READ_COMMITTED, 1, 1, 3, 3, 3, 3, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadConcurrent2() {           //          N--N
        testDirtyReadConcurrent(OPTIMISTIC, REPEATABLE_READ, 1, 1, 1, 3, 3, 3, 3, 3);
    }

    /** */
    @Test(expected = TransactionOptimisticException.class)
    public void testDirtyReadConcurrent3() {
        testDirtyReadConcurrent(OPTIMISTIC, SERIALIZABLE);
    }

    /** */
    @Test
    public void testDirtyReadConcurrent4() {
        testDirtyReadConcurrent(PESSIMISTIC, READ_COMMITTED, 1, 1, 3, 3, 3, 3, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadConcurrent5() {               //       N--N
        testDirtyReadConcurrent(PESSIMISTIC, REPEATABLE_READ, 1, 1, 1, 3, 3, 3, 3, 3);
    }

    /** */
    @Test
    public void testDirtyReadConcurrent6() {               //    N--N
        testDirtyReadConcurrent(PESSIMISTIC, SERIALIZABLE, 1, 1, 1, 3, 3, 3, 3, 3);
    }

    /** */
    private void testDirtyReadConcurrent(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        Object... expected) {
        BARRIERS = new CyclicBarrier[] {new CyclicBarrier(2), new CyclicBarrier(2)};

        //--- Initial state ----------------------------------------
        final Metadata oldHead = resource.head();
        writeKV("a", 1, firstCache, oldHead);
        writeKV("x", 1, firstCache, oldHead);

        IgniteCompute compute = resource.ignite().compute().withAsync();
        List<ComputeTaskFuture<Object>> futures = new ArrayList<>();

        compute.call(new IgniteCallable<Object[]>() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public Object[] call() throws Exception {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                Object[] result = new Object[8];
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[0] = a.get("x");
                    transaction.commit();
                }
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[1] = a.get("a");
                    await(0);
                    await(1);
                    result[2] = a.get("a");
                    result[3] = a.get("x");
                    transaction.commit();
                }
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[4] = a.get("a");
                    result[5] = a.get("x");

                    a.put("b", 3);
                    a.put("c", 3);
                    a.put("d", 3);
                    a.put("e", 3);
                    transaction.commit();
                }
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    result[6] = a.get("a");
                    result[7] = a.get("x");
                    transaction.commit();
                }
                return result;
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                await(0);
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("b", 2);
                    a.put("c", 2);
                    a.put("d", 2);
                    a.put("e", 2);
                    transaction.commit();
                }
                writeKV("a", 3, firstCache, oldHead);
                writeKV("x", 3, firstCache, oldHead);
                await(1);
            }
        });
        futures.add(compute.future());

        List<Object> result = new ArrayList<>();
        for (ComputeTaskFuture<Object> future : futures) {
            Object o = future.get();
            result.add(o);
        }

        Object[] call = (Object[])result.get(0);
        Assert.assertEquals(expected[0], call[0]); //x
        Assert.assertEquals(expected[1], call[1]); //a
        Assert.assertEquals(expected[2], call[2]); //a
        Assert.assertEquals(expected[3], call[3]); //x   //dirty read
        Assert.assertEquals(expected[4], call[4]); //a   //dirty read
        Assert.assertEquals(expected[5], call[5]); //x
        Assert.assertEquals(expected[6], call[6]); //a   //dirty read
        Assert.assertEquals(expected[7], call[7]); //x
    }
}

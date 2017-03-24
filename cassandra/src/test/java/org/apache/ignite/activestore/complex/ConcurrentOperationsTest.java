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
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.BaseCacheStoreTest;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests concurrent calls to store.
 *
 * Tests for reading from different snapshots with PESSIMISTIC transaction and isolation REPEATABLE_READ and
 * SERIALIZABLE were excluded as they will hang on active store (and this is expected).
 */
public class ConcurrentOperationsTest extends BaseCacheStoreTest {
    /** */
    private static CyclicBarrier[] BARRIERS;

    /** */
    private CyclicBarrier a;

    /** */
    private CyclicBarrier b;

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
    public void createBariers() {
        a = new CyclicBarrier(2);
        b = new CyclicBarrier(2);
    }

    /** */
    @Before
    public void createCaches() {
        createCache(firstCache, CacheAtomicityMode.TRANSACTIONAL, CacheMode.PARTITIONED, false, 0);
        createCache(secondCache, CacheAtomicityMode.TRANSACTIONAL, CacheMode.PARTITIONED, false, 0);
    }

    /** */
    @Test
    public void testWrite2Null() throws InterruptedException {
        testWrite(2, null, null, a, a, b, b, null);
    }

    /** */
    @Test
    public void testWrite1Null() throws InterruptedException {
        testWrite(1, null, a, b, null, a, b, null);
    }

    /** */
    @Test
    public void testWrite12() throws InterruptedException {
        testWrite(1, 2, null, a, b, null, a, b);
    }

    /** */
    @Test
    public void testWrite21() throws InterruptedException {
        testWrite(2, 1, b, null, null, a, a, b);
    }

    /** */
    @Test
    public void testWriteNull2() throws InterruptedException {
        testWrite(null, 2, a, b, b, null, null, a);
    }

    /** */
    @Test
    public void testWriteNull1() throws InterruptedException {
        testWrite(null, 1, b, null, a, b, null, a);
    }

    /** */
    private void testWrite(Integer one, Integer another, final CyclicBarrier... barriers) {
        Metadata oldHead = resource.head();
        BARRIERS = barriers;

        IgniteCompute compute = resource.ignite().compute().withAsync();
        List<ComputeTaskFuture<Object>> futures = new ArrayList<>();

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                IgniteCache<Object, Object> b = ignite.cache(secondCache);
                await(0);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("a", 1);
                    a.put("b", 1);
                    b.put("a", 1);
                    b.put("b", 1);
                    transaction.commit();
                }
                await(1);
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                IgniteCache<Object, Object> b = ignite.cache(secondCache);
                await(2);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("a", 2);
                    a.put("b", 2);
                    b.put("a", 2);
                    b.put("b", 2);
                    transaction.commit();
                }
                await(3);
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                await(4);
                resource.createSnapshot("snapshotFromClosure");
                await(5);
            }
        });
        futures.add(compute.future());

        for (ComputeTaskFuture<Object> future : futures) {
            future.get();
        }

        //------------------------------------------------------

        resource.ignite().compute().call(new IgniteCallable<Object[]>() {
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

        Metadata newHead = resource.head();

        Assert.assertEquals(one, readKV("a", firstCache, oldHead));
        Assert.assertEquals(one, readKV("b", firstCache, oldHead));
        Assert.assertEquals(one, readKV("a", secondCache, oldHead));
        Assert.assertEquals(one, readKV("b", secondCache, oldHead));
        Assert.assertEquals(another, readKV("a", firstCache, newHead));
        Assert.assertEquals(another, readKV("b", firstCache, newHead));
        Assert.assertEquals(another, readKV("a", secondCache, newHead));
        Assert.assertEquals(another, readKV("b", secondCache, newHead));
    }

    /** */
    @Test
    public void testWriteFromRead11() throws InterruptedException {
        testWriteFromRead(11, null, a, a, null);
    }

    /** */
    @Test
    public void testWriteFromRead12() throws InterruptedException {
        testWriteFromRead(12, a, null, null, a);
    }

    /** */
    private void testWriteFromRead(int expected, CyclicBarrier... barriers) {
        BARRIERS = barriers;

        //--- Initial state ----------------------------------------
        Metadata oldHead = resource.head();
        writeKV("a", 1, firstCache, oldHead);
        writeKV("b", 1, firstCache, oldHead);
        //----------------------------------------------------------

        IgniteCompute compute = resource.ignite().compute().withAsync();
        List<ComputeTaskFuture<Object>> futures = new ArrayList<>();

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                IgniteCache<Object, Object> b = ignite.cache(secondCache);
                await(0);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    b.put("a", ((Integer)a.get("a")) + 10);
                    b.put("b", ((Integer)a.get("a")) + 10);
                    transaction.commit();
                }
                await(1);
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                await(2);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("a", 2);
                    a.put("b", 2);
                    transaction.commit();
                }
                await(3);
            }
        });
        futures.add(compute.future());

        for (ComputeTaskFuture<Object> future : futures) {
            future.get();
        }

        //------------------------------------------------------

        Assert.assertEquals(2, readKV("a", firstCache, oldHead));
        Assert.assertEquals(2, readKV("b", firstCache, oldHead));
        Assert.assertEquals(expected, readKV("a", secondCache, oldHead));
        Assert.assertEquals(expected, readKV("b", secondCache, oldHead));
    }

    /** */
    @Test
    public void testConcurrentCreateSnapshotPresent() {
        testConcurrentCreateSnapshot(0, null, 1);
    }

    /** */
    @Test
    public void testConcurrentCreateSnapshotFuture() {
        testConcurrentCreateSnapshot(+60000, null, 1);
    }

    /** */
    @Test
    public void testConcurrentCreateSnapshotPast() {
        testConcurrentCreateSnapshot(-60000, null, 1);
    }

    /** */
    private void testConcurrentCreateSnapshot(final long timeShift, Integer one, Integer another) {
        BARRIERS = new CyclicBarrier[] {a, b, a, b};

        Metadata oldHead = resource.head();

        IgniteCompute compute = resource.ignite().compute().withAsync();
        List<ComputeTaskFuture<Object>> futures = new ArrayList<>();

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                IgniteCache<Object, Object> b = ignite.cache(secondCache);
                await(0);
                try (Transaction transaction = ignite.transactions().txStart()) {
                    a.put("a", 1);
                    a.put("b", 1);
                    b.put("a", 1);
                    b.put("b", 1);
                    await(1);
                    transaction.commit();
                }
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                await(2);
                resource.createSnapshot("snapshotFromClosure");
                await(3);
            }
        });
        futures.add(compute.future());

        for (ComputeTaskFuture<Object> future : futures) {
            future.get();
        }

        //------------------------------------------------------

        Metadata newHead = resource.head();

        Assert.assertEquals(one, readKV("a", firstCache, oldHead));
        Assert.assertEquals(one, readKV("b", firstCache, oldHead));
        Assert.assertEquals(one, readKV("a", secondCache, oldHead));
        Assert.assertEquals(one, readKV("b", secondCache, oldHead));
        Assert.assertEquals(another, readKV("a", firstCache, newHead));
        Assert.assertEquals(another, readKV("b", firstCache, newHead));
        Assert.assertEquals(another, readKV("a", secondCache, newHead));
        Assert.assertEquals(another, readKV("b", secondCache, newHead));
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsOptimisticReadCommitedNew() {
        testReadFromDifferentSnapshots(OPTIMISTIC, READ_COMMITTED, true);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsOptimisticRepeatableReadNew() {
        testReadFromDifferentSnapshots(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsOptimisticSerializableNew() {
        testReadFromDifferentSnapshots(OPTIMISTIC, SERIALIZABLE, true);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsPessimisticReadCommitedNew() {
        testReadFromDifferentSnapshots(PESSIMISTIC, READ_COMMITTED, true);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsPessimisticRepeatableReadNew() {
        testReadFromDifferentSnapshots(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsPessimisticSerializableNew() {
        testReadFromDifferentSnapshots(PESSIMISTIC, SERIALIZABLE, true);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsOptimisticReadCommited() {
        testReadFromDifferentSnapshots(OPTIMISTIC, READ_COMMITTED, false);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsOptimisticRepeatableRead() {
        testReadFromDifferentSnapshots(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /** */
    @Test(expected = TransactionOptimisticException.class)
    public void testReadFromDifferentSnapshotsOptimisticSerializable() {
        testReadFromDifferentSnapshots(OPTIMISTIC, SERIALIZABLE, false);
    }

    /** */
    @Test
    public void testReadFromDifferentSnapshotsPessimisticReadCommited() {
        testReadFromDifferentSnapshots(PESSIMISTIC, READ_COMMITTED, false);
    }

    /** */
    private void testReadFromDifferentSnapshots(
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        boolean newKey) {
        BARRIERS = new CyclicBarrier[] {new CyclicBarrier(2), new CyclicBarrier(2), new CyclicBarrier(2), new CyclicBarrier(2)};

        final String secondKey = newKey ? "b" : "a";

        //--- Initial state ----------------------------------------
        Metadata oldHead = resource.head();
        writeKV("a", 1, firstCache, oldHead);
        writeKV("b", 1, firstCache, oldHead);
        //----------------------------------------------------------

        IgniteCompute compute = resource.ignite().compute().withAsync();
        List<ComputeTaskFuture<Object>> futures = new ArrayList<>();

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                IgniteCache<Object, Object> b = ignite.cache(secondCache);
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    b.put("a", ((Integer)a.get("a")) + 10);
                    await(0);
                    await(2);
                    b.put("b", ((Integer)a.get(secondKey)) + 10);
                    transaction.commit();
                }
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                IgniteCache<Object, Object> a = ignite.cache(firstCache);
                await(1);
                try (Transaction transaction = ignite.transactions().txStart(concurrency, isolation)) {
                    a.put(secondKey, 2);
                    transaction.commit();
                }
                await(2);
            }
        });
        futures.add(compute.future());

        compute.run(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                await(0);
                resource.createSnapshot("snapshotFromClosure");
                await(1);
            }
        });
        futures.add(compute.future());

        for (ComputeTaskFuture<Object> future : futures) {
            future.get();
        }

        //------------------------------------------------------

        Metadata newHead = resource.head();
        Assert.assertEquals(11, readKV("a", secondCache, newHead));
        Assert.assertEquals(newKey || isolation == READ_COMMITTED ? 12 : 11, readKV("b", secondCache, newHead));
    }
}

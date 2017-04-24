/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lagerta.subscriber;

import com.epam.lagerta.BaseSingleJVMIntegrationTest;
import com.epam.lagerta.base.jdbc.common.Person;
import com.epam.lagerta.capturer.IdSequencer;
import com.epam.lagerta.mocks.ProxyReconciler;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.SpringResource;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class SubscriberIntegrationTest extends BaseSingleJVMIntegrationTest {
    private static final String CACHE_INFO_PROVIDER = "cacheInfoProvider";

    @DataProvider(name = CACHE_INFO_PROVIDER)
    public static Object[][] provideCacheInformation() {
        return DataProviderUtil.concat(provideCacheName(), new Object[][]{{false}, {true}});
    }

    @Test(dataProvider = CACHE_INFO_PROVIDER)
    public void sequentialTransactions(String cacheName, boolean asBinary) throws Exception {
        Person firstPerson = new Person(0, "firstName");
        Person secondPerson = new Person(1, "secondName");

        writePersonToCache(cacheName, 1, firstPerson);
        writePersonToCache(cacheName, 1, secondPerson);
        awaitTransactions();

        assertObjectsInDB(asBinary, entry(1, secondPerson));
    }

    @Test(dataProvider = CACHE_NAME_PROVIDER)
    public void testWriteThroughAndReadThrough(String cacheName) throws Exception {
        Person expected = new Person(312, "name");

        writePersonToCache(cacheName, 1, expected);
        awaitTransactions();

        IgniteCache<Object, Person> cache = ignite().cache(cacheName);
        cache.withSkipStore().clear(1);

        Person actual = cache.get(1);
        Assert.assertEquals(actual.getId(), expected.getId());
        Assert.assertEquals(actual.getName(), expected.getName());
    }

    @Test
    public void gapDetectionProcessFillsGaps() throws Exception {
        int firstPersonKey = 1;
        Person firstPerson = new Person(0, "some_name");
        writePersonToCache(CACHE_NAME, firstPersonKey, firstPerson);

        long missedTx = createGap();
        int lastPersonKey = 2;
        Person lastPerson = new Person(1, "abyrvalg");
        writePersonToCache(CACHE_NAME, lastPersonKey, lastPerson);

        awaitReconciliationOnTransaction(missedTx);
        awaitTransactions();
        assertObjectsInDB(false, entry(firstPersonKey, firstPerson), entry(lastPersonKey, lastPerson));
    }

    private void awaitReconciliationOnTransaction(long missedTx) {
        boolean wasReconciliationCalled;
        do {
            Collection<Boolean> gapDetectionResult = ignite().compute().broadcast(new ReconCheckerCallable(missedTx));
            wasReconciliationCalled = gapDetectionResult.stream().anyMatch(Boolean::booleanValue);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } while (!wasReconciliationCalled);
    }

    private long createGap() {
        return getBean(IdSequencer.class).getNextId();
    }

    private static class ReconCheckerCallable implements IgniteCallable<Boolean> {
        @SpringResource(resourceClass = ProxyReconciler.class)
        private transient ProxyReconciler reconciler;

        private final long missedTx;

        public ReconCheckerCallable(long missedTx) {
            this.missedTx = missedTx;
        }

        @Override
        public Boolean call() throws Exception {
            return reconciler.wasReconciliationCalled(missedTx);
        }
    }
}

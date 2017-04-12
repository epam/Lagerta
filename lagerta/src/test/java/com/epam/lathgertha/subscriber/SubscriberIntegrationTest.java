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

package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.BaseIntegrationTest;
import com.epam.lathgertha.base.jdbc.common.Person;
import org.apache.ignite.IgniteCache;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SubscriberIntegrationTest extends BaseIntegrationTest {
    private static final String CACHE_INFO_PROVIDER = "cacheInfoProvider";

    @DataProvider(name = CACHE_INFO_PROVIDER)
    public Object[][] provideCacheInformation() {
        return new Object[][] {
                {CACHE_NAME, false},
                {BINARY_KEEPING_CACHE_NAME, true}
        };
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

    @Test(dataProvider = CACHE_INFO_PROVIDER)
    public void testWriteThroughAndReadThrough(String cacheName, boolean asBinary) throws Exception {
        Person expected = new Person(312, "name");

        writePersonToCache(cacheName, 1, expected);
        awaitTransactions();

        IgniteCache<Object, Person> cache = ignite().cache(cacheName);
        cache.withSkipStore().clear(1);

        Person actual = cache.get(1);
        Assert.assertEquals(actual.getId(), expected.getId());
        Assert.assertEquals(actual.getName(), expected.getName());
    }
}

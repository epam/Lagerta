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

package org.apache.ignite.activestore.mocked.tests;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import com.google.inject.name.Named;
import javax.inject.Inject;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteCallable;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.mocked.mocks.InputProducer;
import org.apache.ignite.activestore.mocked.mocks.KafkaMockFactory;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author Aleksandr_Meterko
 * @since 12/26/2016
 */
public class DRClusterFunctionalTest extends BaseFunctionalTest {
    @Test
    public void oneTransactionCommitted() throws InterruptedException {
        int id = getNextTxId();
        Assert.assertNull(cache.get(id));
        InputProducer producer = kafkaMockFactory.inputProducer(KafkaMockFactory.REMOTE_ID, 0);
        int value = 0;
        producer.send(createMessage(id, singletonList(id), singletonList(value)));
        Thread.sleep(10_000);
        Assert.assertEquals(value, cache.get(id));
    }

    // tx1 - changes key  0
    // tx2 - changes keys 0 and 1 - comes first, so it is blocked
    @Test
    public void firstTransactionIsBlockedBySecond() throws InterruptedException {
        int key1 = getNextTxId(),
            key2 = getNextTxId();
        Assert.assertNull(cache.get(key1));
        Assert.assertNull(cache.get(key2));

        int expectedValue = 2;
        InputProducer firstProducer = kafkaMockFactory.inputProducer(KafkaMockFactory.REMOTE_ID, 0);
        firstProducer.send(createMessage(key2, asList(key1, key2), asList(expectedValue, expectedValue)));
        Thread.sleep(10_000);
        // nothing is applied
        Assert.assertNull(cache.get(key1));
        Assert.assertNull(cache.get(key2));

        InputProducer secondProducer = kafkaMockFactory.inputProducer(KafkaMockFactory.REMOTE_ID, 1);
        secondProducer.send(createMessage(key1, singletonList(key1), singletonList(0)));
        Thread.sleep(10_000);

        Assert.assertEquals(expectedValue, cache.get(key1));
        Assert.assertEquals(expectedValue, cache.get(key2));
    }

    @Test
    public void clusterIdIsTheSameOnAllNodes() {
        Collection<UUID> clusterIds = ignite.compute().broadcast(new ClusterIdRetriever());
        Assert.assertTrue(clusterIds.size() > 0);
        Iterator<UUID> iterator = clusterIds.iterator();
        UUID firstId = iterator.next();
        while (iterator.hasNext()) {
            Assert.assertEquals(firstId, iterator.next());
        }
    }

    private static class ClusterIdRetriever extends ActiveStoreIgniteCallable<UUID> {

        @Inject
        @Named(DataCapturerBusConfiguration.CLUSTER_ID)
        private UUID clusterId;

        @Override public UUID callInjected() throws Exception {
            return clusterId;
        }
    }

}

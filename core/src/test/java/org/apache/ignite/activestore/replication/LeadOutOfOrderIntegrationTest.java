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

package org.apache.ignite.activestore.replication;

import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerService;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerServiceImpl;
import org.apache.ignite.activestore.impl.subscriber.lead.*;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.cluster.ClusterGroup;
import org.eclipse.collections.api.list.primitive.LongList;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Andrei_Yakushin
 * @since 12/22/2016 4:28 PM
 */
public class LeadOutOfOrderIntegrationTest extends BasicSynchronousReplicationIntegrationTest {
    @AfterClass
    public static void tearDown() {
        restoreLead();
    }

    @Test
    public void failLeadOnNotifyTransactionsRead() throws InterruptedException {
        Thread.sleep(10_000);

        failOnNotifyTransactionsRead();

        write(true);
        Thread.sleep(1_000);
        Map<Integer, Integer> drValues = readValuesFromCache(drGrid());
        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertNull(drValues.get(i));
        }

        restoreLead();
        waitLead();

        Thread.sleep(5_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void failLeadOnNotifyTransactionCommitted() throws InterruptedException {
        Thread.sleep(10_000);

        failOnNotifyTransactionCommitted();
        write(true);

        Thread.sleep(2_000);
        assertCurrentStateOnServers();
        drGrid().services().cancel(LeadService.SERVICE_NAME);

        restoreLead();
        waitLead();

        Thread.sleep(5_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void fullFail() throws InterruptedException {
        Thread.sleep(10_000);

        drGrid().services().cancel(SubscriberConsumerService.SERVICE_NAME);
        drGrid().services().cancel(LeadService.SERVICE_NAME);
        write(true);

        Thread.sleep(2_000);
        Map<Integer, Integer> drValues = readValuesFromCache(drGrid());
        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertNull(drValues.get(i));
        }

        Thread.sleep(10_000);
        drGrid().services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());
        drGrid().services().deployNodeSingleton(SubscriberConsumerServiceImpl.SERVICE_NAME, new SubscriberConsumerServiceImpl());
        waitLead();

        Thread.sleep(5_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void partialFail() throws InterruptedException {
        Thread.sleep(10_000);

        ClusterGroup clusterGroup = drGrid().cluster().forNodes(Collections.singleton(drGrid().cluster().forServers().nodes().iterator().next()));

        drGrid().services(clusterGroup).cancel(SubscriberConsumerService.SERVICE_NAME);
        drGrid().services().cancel(LeadService.SERVICE_NAME);
        writeAndReplicate(0);

        Thread.sleep(2_000);
        drGrid().services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());
        drGrid().services(clusterGroup).deployNodeSingleton(SubscriberConsumerServiceImpl.SERVICE_NAME, new SubscriberConsumerServiceImpl());

        writeAndReplicate(1);

        waitLead();

        Thread.sleep(10_000);
        assertCurrentStateOnServers(1);
    }

    @Test
    public void failOnFinishingInitialLoading() throws InterruptedException {
        waitLead();

        drGrid().services().cancel(LeadService.SERVICE_NAME);
        Thread.sleep(2_000);
        drGrid().services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());

        Thread.sleep(500);
        failOnNotifyLocalLoaderStalled();
        Thread.sleep(500);
        drGrid().services().cancel(LeadService.SERVICE_NAME);

        Thread.sleep(2_000);
        restoreLead();

        waitLead();

        write(true);
        Thread.sleep(2_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void failDuringInitialLoading() throws InterruptedException {
        waitLead();

        drGrid().services().cancel(LeadService.SERVICE_NAME);
        Thread.sleep(2_000);
        drGrid().services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());

        Thread.sleep(500);
        failOnUpdateInitialContext();
        Thread.sleep(500);
        drGrid().services().cancel(LeadService.SERVICE_NAME);

        Thread.sleep(2_000);
        restoreLead();

        waitLead();

        write(true);
        Thread.sleep(2_000);
        assertCurrentStateOnServers();
    }

    //------------------------------------------------------------------------------------------------------------------

    private static void failOnNotifyTransactionsRead() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter() {
                    @Override
                    public LeadResponse notifyTransactionsRead(UUID consumerId, List<TransactionMetadata> metadatas) {
                        throw new IgniteException("planned fail");
                    }
                });
            }
        });
        drGrid().services().cancel(LeadService.SERVICE_NAME);
    }

    private static void failOnNotifyTransactionCommitted() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter() {
                    @Override
                    public void notifyTransactionsCommitted(UUID consumerId, LongList transactionsIds) {
                        throw new IgniteException("planned fail");
                    }
                });
            }
        });
    }

    private static void failOnUpdateInitialContext() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter() {
                    @Override
                    public void updateInitialContext(UUID localLoaderId, LongList txIds) {
                        throw new IgniteException("planned fail");
                    }
                });
            }
        });
    }

    private static void failOnNotifyLocalLoaderStalled() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter() {
                    @Override
                    public void notifyLocalLoaderStalled(UUID leadId, UUID localLoaderId) {
                        throw new IgniteException("planned fail");
                    }
                });
            }
        });
    }

    private static void restoreLead() {
        drGrid().services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter());
            }
        });
    }
}

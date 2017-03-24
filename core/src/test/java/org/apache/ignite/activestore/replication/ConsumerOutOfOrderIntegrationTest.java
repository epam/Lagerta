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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import kafka.common.KafkaException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.subscriber.KafkaFactoryForTests;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerAdapter;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerService;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerServiceImpl;
import org.apache.ignite.activestore.impl.subscriber.lead.DoubleLeadService;
import org.apache.ignite.activestore.impl.subscriber.lead.FastConsumerPingCheckStrategy;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadResponse;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadServiceAdapter;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Evgeniy_Ignatiev
 * @since 18:46 12/29/2016
 */
public class ConsumerOutOfOrderIntegrationTest extends BasicSynchronousReplicationIntegrationTest {
    private boolean connectivityFailing;
    private boolean kafkaConsumerFailing;
    private boolean consumersCrashed;

    @Before
    public void setUp() throws Exception {
        Thread.sleep(10_000);
        connectivityFailing = false;
        kafkaConsumerFailing = false;
        consumersCrashed = false;
    }

    @After
    public void cleanUp() {
        if (connectivityFailing) {
            restoreConnectivity();
        }
        if (kafkaConsumerFailing) {
            restoreKafkaConsumer();
        }
        if (consumersCrashed) {
            resurrectConsumers();
        }
    }

    @Test
    public void kafkaConsumerPollFailed() throws InterruptedException {
        waitLead();

        failOnKafkaPoll();
        Thread.sleep(1_000);

        writeAndReplicateSync();
        Thread.sleep(2_000);
        assertValuesNonExistenceOnServers();

        restoreKafkaConsumer();

        Thread.sleep(20_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void notifyTransactionsReadFailedConsumerReunitedAfter() throws InterruptedException {
        // Ensure there is only one consumer on cluster.
        ClusterGroup singleConsumerGroup = drGrid().cluster().forLocal();
        retainConsumersOnSubcluser(singleConsumerGroup);
        Thread.sleep(1_000); // Await cancel.

        failNotifyTransactionReadAfterRegisteringTxs();
        Thread.sleep(1_000);

        writeAndReplicateSync();
        Thread.sleep(FastConsumerPingCheckStrategy.FAST_CONSUMER_TIMEOUT + 5_000);
        assertValuesNonExistenceOnServers();

        restoreConnectivity();

        Thread.sleep(20_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void consumersCrashedAfterFirstNonEmptyPollAndResurrected() throws InterruptedException {
        failNotifyTransactionReadAfterRegisteringTxs();
        Thread.sleep(1_000);
        drGrid().services().cancel(SubscriberConsumerService.SERVICE_NAME);

        Thread.sleep(1_000);
        writeAndReplicateSync();
        Thread.sleep(FastConsumerPingCheckStrategy.FAST_CONSUMER_TIMEOUT + 5_000);
        assertValuesNonExistenceOnServers();

        restoreConnectivity();
        resurrectConsumers();

        Thread.sleep(20_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void oneOfConsumersCrashesAfterFirstNonEmptyPoll() throws InterruptedException {
        // Emulate single consumer crash.
        ClusterGroup restOfConsumers = drGrid().cluster().forRemotes();
        retainConsumersOnSubcluser(restOfConsumers);
        Thread.sleep(1_000);

        write(true);
        Thread.sleep(20_000);
        assertCurrentStateOnServers();
    }

    private void assertValuesNonExistenceOnServers() {
        Map<Integer, Integer> drValues = readValuesFromCache(drGrid());

        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertNull(drValues.get(i));
        }
    }

    private void failOnKafkaPoll() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient KafkaFactory kafkaFactory;

            @Override protected void runInjected() {
                ((KafkaFactoryForTests)kafkaFactory).substituteConsumers(
                    new ConsumerAdapter() {
                        @Override public ConsumerRecords poll(long timeout) {
                            throw new KafkaException("Poll failed");
                        }
                    });
            }
        });
        kafkaConsumerFailing = true;
    }

    private static void restoreKafkaConsumer() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient KafkaFactory kafkaFactory;

            @Override protected void runInjected() {
                ((KafkaFactoryForTests)kafkaFactory).substituteConsumers(null);
            }
        });
    }

    private void failNotifyTransactionReadAfterRegisteringTxs() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter() {
                    private boolean transactionsSent = false;

                    @Override public LeadResponse notifyTransactionsRead(UUID consumerId,
                        List<TransactionMetadata> metadatas) {
                        if (transactionsSent) {
                            throw new IgniteException();
                        }
                        if (!metadatas.isEmpty()) {
                            transactionsSent = true;
                        }
                        return null;
                    }
                });
            }
        });
        connectivityFailing = true;
    }

    private static void restoreConnectivity() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient DoubleLeadService lead;

            @Override protected void runInjected() {
                lead.setWorkInstead(new LeadServiceAdapter());
            }
        });
    }

    private static void resurrectConsumers() {
        drGrid().services().deployNodeSingleton(SubscriberConsumerService.SERVICE_NAME,
            new SubscriberConsumerServiceImpl());
    }

    private static void retainConsumersOnSubcluser(ClusterGroup subcluster) {
        drGrid().services().cancel(SubscriberConsumerService.SERVICE_NAME);
        drGrid().services(subcluster).deployNodeSingleton(SubscriberConsumerService.SERVICE_NAME,
            new SubscriberConsumerServiceImpl());
    }
}

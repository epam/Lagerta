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

import kafka.common.KafkaException;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.subscriber.KafkaFactoryForTests;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerAdapter;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * @author Evgeniy_Ignatiev
 * @since 16:47 01/13/2017
 */
public class LeadAndKafkaOutOfOrderIntegrationTest extends BasicSynchronousReplicationIntegrationTest {
    private boolean kafkaCrashed;
    private boolean leadCrashed;

    @Before
    public void setUp() throws Exception {
        Thread.sleep(10_000);
        leadCrashed = false;
        kafkaCrashed = false;
    }

    @After
    public void cleanUp() {
        if (kafkaCrashed) {
            resurrectKafka();
        }
        if (leadCrashed) {
            resurrectLead();
        }
    }

    @Test
    public void kafkaRessurectedBeforeLeadRestart() throws InterruptedException {
        failOnKafkaOperations();
        crashLead();

        write(true);
        Thread.sleep(1_000);
        assertValuesNonExistenceOnServers();

        resurrectKafka();
        resurrectLead();

        Thread.sleep(20_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void kafkaResurrectedAfterLeadReloadingStarted() throws InterruptedException {
        failOnKafkaOperations();
        crashLead();

        write(true);
        Thread.sleep(1_000);
        assertValuesNonExistenceOnServers();

        resurrectLead();
        Thread.sleep(5_000);
        assertValuesNonExistenceOnServers();

        resurrectKafka();
        Thread.sleep(20_000);
        assertCurrentStateOnServers();
    }

    private void assertValuesNonExistenceOnServers() {
        Map<Integer, Integer> drValues = readValuesFromCache(drGrid());

        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertNull(drValues.get(i));
        }
    }

    private void failOnKafkaOperations() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient KafkaFactory kafkaFactory;

            @Override protected void runInjected() {
                ((KafkaFactoryForTests)kafkaFactory).substituteConsumers(
                    new ConsumerAdapter() {
                        @Override public ConsumerRecords poll(long timeout) {
                            throw new KafkaException("Poll failed");
                        }

                        @Override public List<TopicPartition> partitionsFor(String topic) {
                            throw new KafkaException("Connection to server failed");
                        }
                    }
                );
            }
        });
        kafkaCrashed = true;
    }

    private void crashLead() throws InterruptedException {
        drGrid().services().cancel(LeadService.SERVICE_NAME);
        Thread.sleep(1_000);
    }


    private static void resurrectKafka() {
        drGrid().compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private transient KafkaFactory kafkaFactory;

            @Override protected void runInjected() {
                ((KafkaFactoryForTests)kafkaFactory).substituteConsumers(null);
            }
        });
    }

    private static void resurrectLead() {
        drGrid().services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());
    }
}

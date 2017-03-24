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

import java.util.List;
import javax.inject.Inject;
import kafka.common.KafkaException;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.commons.injection.InjectionForTests;
import org.apache.ignite.activestore.impl.subscriber.consumer.ConsumerAdapter;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadServiceImpl;
import org.apache.ignite.activestore.mocked.mocks.KafkaMockFactory;
import org.apache.ignite.activestore.mocked.mocks.MockRPCService;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;

/**
 * @author Aleksandr_Meterko
 * @since 1/17/2017
 */
public class CrashFunctionalTest extends BaseFunctionalTest {

    private boolean kafkaCrashed = false;
    private boolean leadCrashed = false;

    @After
    public void resurrectKafkaAndLead() {
        if (kafkaCrashed) {
            resurrectKafka();
        }
        if (leadCrashed) {
            resurrectLead();
        }
    }

    private MockRPCService rpcService(Ignite grid) {
        return InjectionForTests.get(MockRPCService.class, grid);
    }

    protected void failOnKafkaOperations() {
        KafkaMockFactory.substituteConsumers(
            new ConsumerAdapter() {
                @Override public ConsumerRecords poll(long timeout) {
                    throw new KafkaException("Poll failed");
                }

                @Override public List<TopicPartition> partitionsFor(String topic) {
                    throw new KafkaException("Connection to server failed");
                }
            }
        );
        kafkaCrashed = true;
    }

    protected void crashLead() throws InterruptedException {
        ignite.services().cancel(LeadService.SERVICE_NAME);
        Thread.sleep(1_000);
        leadCrashed = true;
    }

    protected static void resurrectKafka() {
        KafkaMockFactory.substituteConsumers(null);
    }

    protected void resurrectLead() {
        ignite.services().deployClusterSingleton(LeadService.SERVICE_NAME, new LeadServiceImpl());
    }

    protected <T> T serviceMock(Class<T> serviceClass) {
        return rpcService(ignite).get(serviceClass, null);
    }

    protected void crashMain() {
        ignite.compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private MockRPCService rpcService;

            @Override protected void runInjected() {
                rpcService.crashMain();
            }
        });
    }

    protected void resurrectMain() {
        ignite.compute().broadcast(new ActiveStoreIgniteRunnable() {
            @Inject
            private MockRPCService rpcService;

            @Override protected void runInjected() {
                rpcService.restoreMain();
            }
        });
    }

}

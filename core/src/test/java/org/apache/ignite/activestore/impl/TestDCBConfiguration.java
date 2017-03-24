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

package org.apache.ignite.activestore.impl;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.impl.config.RPCManager;
import org.apache.ignite.activestore.impl.config.RPCService;
import org.apache.ignite.activestore.impl.config.RPCUpdater;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.publisher.LastTransactionListener;
import org.apache.ignite.activestore.impl.subscriber.KafkaFactoryForTests;
import org.apache.ignite.activestore.impl.subscriber.ReceivedTransactionsListener;
import org.apache.ignite.activestore.impl.subscriber.ReceivedTransactionsListenerImpl;
import org.apache.ignite.activestore.impl.subscriber.lead.*;
import org.apache.ignite.activestore.mocked.mocks.MockRPCManager;
import org.apache.ignite.activestore.mocked.mocks.MockRPCService;

/**
 * @author Andrei_Yakushin
 * @since 12/22/2016 4:04 PM
 */
public class TestDCBConfiguration extends DataCapturerBusConfiguration {

    private static final long GAP_EXISTENCE_THRESHOLD = 200;
    private static final long MAIN_PING_CHECK_TIME = 200;
    private static final long CONSUMER_PING_CHECK_TIME = 1_000;

    private Class<KafkaFactory> kafkaFactory;

    public void setKafkaFactory(Class<KafkaFactory> kafkaFactory) {
        this.kafkaFactory = kafkaFactory;
    }

    public ReceivedTransactionsListener receivedTransactionsListener() {
        return new ReceivedTransactionsListenerImpl();
    }

    @Override public Module createModule() {
        setSuperClusterAddress("localhost:9999");
        setConsumerBufferLimit(3);

        listeners.add(LastTransactionListener.class);
        return Modules.override(super.createModule()).with(new AbstractModule() {
            @Override protected void configure() {
                bind(IdSequencer.class).to(InMemoryIdSequencer.class).in(Singleton.class);
                bind(LeadService.class).to(LeadServiceForTests.class).in(Singleton.class);
                bind(ConsumerPingCheckStrategy.class).to(FastConsumerPingCheckStrategy.class).in(Singleton.class);
                bind(DoubleLeadService.class).in(Singleton.class);
                bind(RPCService.class).to(MockRPCService.class);
                if (kafkaFactory != null) {
                    bind(KafkaFactory.class).to(kafkaFactory).in(Singleton.class);
                } else {
                    bind(KafkaFactory.class).to(KafkaFactoryForTests.class).in(Singleton.class);
                }
                bind(GapDetectionStrategy.class).toProvider(new GapDetectionProvider());
                bind(RPCManager.class).to(MockRPCManager.class);
                bind(RPCUpdater.class).to(MockRPCManager.class);
                bind(Long.class).annotatedWith(Names.named(Lead.MAIN_PING_CHECK_KEY)).toInstance(MAIN_PING_CHECK_TIME);
                bind(Long.class).annotatedWith(Names.named(Lead.CONSUMER_PING_CHECK_KEY))
                    .toInstance(CONSUMER_PING_CHECK_TIME);
                bind(ReconciliationState.class).to(TestReconciliationState.class);
            }
        });
    }

    private static class GapDetectionProvider implements Provider<GapDetectionStrategy> {
        @Override public GapDetectionStrategy get() {
            return new DefaultGapDetectionStrategy(GAP_EXISTENCE_THRESHOLD);
        }
    }

}

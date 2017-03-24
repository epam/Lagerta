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

import java.util.List;
import java.util.UUID;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.KeyValueListener;
import org.apache.ignite.activestore.SerializableProvider;
import org.apache.ignite.activestore.commons.ActiveStoreLifecycleService;
import org.apache.ignite.activestore.commons.BaseActiveStoreConfiguration;
import org.apache.ignite.activestore.commons.injection.ListOf;
import org.apache.ignite.activestore.commons.serializer.JavaSerializer;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.activestore.impl.config.RPCManager;
import org.apache.ignite.activestore.impl.config.RPCManagerImpl;
import org.apache.ignite.activestore.impl.config.RPCUpdater;
import org.apache.ignite.activestore.impl.config.ReplicaProducersManager;
import org.apache.ignite.activestore.impl.config.ReplicaProducersManagerImpl;
import org.apache.ignite.activestore.impl.config.ReplicaProducersUpdater;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.kafka.KafkaFactoryImpl;
import org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreService;
import org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreServiceImpl;
import org.apache.ignite.activestore.impl.publisher.CommandServiceImpl;
import org.apache.ignite.activestore.impl.publisher.LocalKafkaKVListener;
import org.apache.ignite.activestore.impl.subscriber.consumer.BufferOverflowCondition;
import org.apache.ignite.activestore.impl.subscriber.consumer.ParallelIgniteCommitter;
import org.apache.ignite.activestore.impl.subscriber.consumer.SimpleBufferOverflowCondition;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerService;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerServiceImpl;
import org.apache.ignite.activestore.impl.subscriber.lead.ConsumerPingCheckStrategy;
import org.apache.ignite.activestore.impl.subscriber.lead.DefaultConsumerPingCheckStrategy;
import org.apache.ignite.activestore.impl.subscriber.lead.DefaultGapDetectionStrategy;
import org.apache.ignite.activestore.impl.subscriber.lead.GapDetectionStrategy;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadServiceImpl;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadServiceProxyRetry;
import org.apache.ignite.activestore.impl.subscriber.lead.ReconciliationState;
import org.apache.ignite.activestore.impl.subscriber.lead.ReconciliationStateImpl;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;
import org.apache.ignite.activestore.publisher.CommandService;
import org.apache.ignite.activestore.subscriber.Committer;
import org.springframework.util.Assert;

/**
 * @author Andrei_Yakushin
 * @since 10/31/2016 12:43 PM
 */
public class DataCapturerBusConfiguration extends BaseActiveStoreConfiguration {
    public static final String CLUSTER_ID = "cluster.id";
    public static final String NODE_ID = "node.id";
    public static final String SUPER_CLUSTER_ADDRESS = "super.cluster.address";
    public static final String RPC_ADDRESS = "rpc.address";

    private static final long MAIN_PING_CHECK_TIME = 30_000;
    private static final long CONSUMER_PING_CHECK_TIME = 30_000;
    private static final int DEFAULT_CONSUMER_LIMIT = -1;

    protected List<Class<? extends KeyValueListener>> listeners;
    private DataRecoveryConfig dataRecoveryConfig;
    private SerializableProvider<IdSequencer> idSequencerFactory;
    private String address;
    private String superClusterAddress;
    private int consumerBufferLimit = DEFAULT_CONSUMER_LIMIT;

    /** */
    public void setListeners(List<Class<? extends KeyValueListener>> listeners) {
        this.listeners = listeners;
    }

    /**
     * Sets factory of custom implementation of {@link IdSequencer}
     *
     * @param idSequencerFactory factory that should be used to create id sequencer instance.
     */
    public void setIdSequencerFactory(SerializableProvider<IdSequencer> idSequencerFactory) {
        this.idSequencerFactory = idSequencerFactory;
    }

    /** */
    public void setDataRecoveryConfig(DataRecoveryConfig dataRecoveryConfig) {
        this.dataRecoveryConfig = dataRecoveryConfig;
    }

    /** */
    public void setAddress(String address) {
        this.address = address;
    }

    /** */
    public void setSuperClusterAddress(String superClusterAddress) {
        this.superClusterAddress = superClusterAddress;
    }

    /** */
    public void setConsumerBufferLimit(int consumerBufferLimit) {
        this.consumerBufferLimit = consumerBufferLimit;
    }

    /** */
    public CommandService commandService() {
        return new CommandServiceImpl();
    }

    /** */
    public SubscriberConsumerService subscriberConsumer() {
        return new SubscriberConsumerServiceImpl();
    }

    /** */
    public LeadService leadService() {
        return new LeadServiceImpl();
    }

    /** */
    public ActiveStoreLifecycleService lifecycleService() {
        return new ActiveStoreLifecycleService();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(dataRecoveryConfig);
        Assert.notEmpty(listeners);
    }

    @Override public Module createModule() {
        return new AbstractModule() {
            @Override protected void configure() {
                // singleton scope
                if (idSequencerFactory != null) {
                    bind(IdSequencer.class).toProvider(idSequencerFactory).in(Singleton.class);
                }
                bind(UUID.class).annotatedWith(Names.named(CLUSTER_ID)).toProvider(new ClusterIdProvider());
                bind(String.class).annotatedWith(Names.named(RPC_ADDRESS)).toInstance(address);
                bind(String.class).annotatedWith(Names.named(SUPER_CLUSTER_ADDRESS))
                    .toInstance(superClusterAddress);
                bind(DataRecoveryConfig.class).toInstance(dataRecoveryConfig);
                bind(Serializer.class).to(JavaSerializer.class).in(Singleton.class);
                bind(ReplicaProducersManager.class).to(ReplicaProducersManagerImpl.class);
                bind(ReplicaProducersUpdater.class).to(ReplicaProducersManagerImpl.class);
                bind(RPCManager.class).to(RPCManagerImpl.class);
                bind(RPCUpdater.class).to(RPCManagerImpl.class);
                bind(Committer.class).to(ParallelIgniteCommitter.class).in(Singleton.class);
                bind(KafkaFactory.class).to(KafkaFactoryImpl.class).in(Singleton.class);
                bind(LeadService.class).to(LeadServiceProxyRetry.class).in(Singleton.class);
                bind(ConsumerPingCheckStrategy.class).to(DefaultConsumerPingCheckStrategy.class).in(Singleton.class);
                bind(ActiveCacheStoreService.class).to(ActiveCacheStoreServiceImpl.class).in(Singleton.class);
                bind(BufferOverflowCondition.class).toInstance(new SimpleBufferOverflowCondition(consumerBufferLimit));
                bind(ReconciliationState.class).to(ReconciliationStateImpl.class);
                bind(Long.class).annotatedWith(Names.named(Lead.MAIN_PING_CHECK_KEY)).toInstance(MAIN_PING_CHECK_TIME);
                bind(Long.class).annotatedWith(Names.named(Lead.CONSUMER_PING_CHECK_KEY))
                    .toInstance(CONSUMER_PING_CHECK_TIME);

                listeners.add(LocalKafkaKVListener.class);
                bind(new TypeLiteral<List<KeyValueListener>>() {}).toProvider(new ListOf<>(listeners)).in(Singleton.class);
                bind(UUID.class).annotatedWith(Names.named(NODE_ID)).toProvider(new NodeIdFactory()).in(Singleton.class);
                // request scope
                bind(GapDetectionStrategy.class).to(DefaultGapDetectionStrategy.class);
            }
        };
    }

    @Singleton
    private static class ClusterIdProvider implements SerializableProvider<UUID> {
        /** Auto-injected ignite instance. */
        @Inject
        private transient Ignite ignite;

        @Override public UUID get() {
            UUID clusterId = UUID.randomUUID();
            return AtomicsHelper.<UUID>getReference(ignite, CLUSTER_ID, true).initIfAbsent(clusterId);
        }
    }

    private static class NodeIdFactory implements SerializableProvider<UUID> {
        /** Auto-injected ignite instance. */
        @Inject
        private transient Ignite ignite;

        @Override
        public UUID get() {
            return ignite.cluster().localNode().id();
        }
    }
}

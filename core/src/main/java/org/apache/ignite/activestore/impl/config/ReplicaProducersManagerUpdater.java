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

package org.apache.ignite.activestore.impl.config;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.publisher.RemoteKafkaProducer;
import org.apache.ignite.activestore.publisher.CommandService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.Closeable;
import java.util.*;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/7/2016 3:25 PM
 */
@Singleton
public class ReplicaProducersManagerUpdater implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaProducersManagerUpdater.class);

    private final Serializer serializer;
    private final KafkaFactory kafkaFactory;
    private final Provider<UUID> clusterId;
    private final Provider<ReplicaProducersUpdater> replicaProducersUpdaterProvider;
    private final Ignite ignite;

    private volatile Map<UUID, RemoteKafkaProducer> producers = Collections.emptyMap();

    @Inject
    public ReplicaProducersManagerUpdater(
        Serializer serializer,
        KafkaFactory kafkaFactory,
        @Named(DataCapturerBusConfiguration.CLUSTER_ID) Provider<UUID> clusterId,
        Provider<ReplicaProducersUpdater> replicaProducersUpdaterProvider,
        Ignite ignite
    ) {
        this.serializer = serializer;
        this.kafkaFactory = kafkaFactory;
        this.clusterId = clusterId;
        this.replicaProducersUpdaterProvider = replicaProducersUpdaterProvider;
        this.ignite = ignite;
    }

    @Override public void close() {
        closeProducers(producers.values());
        producers = Collections.emptyMap();
    }

    public Collection<RemoteKafkaProducer> getProducers() {
        return producers.values();
    }

    public void updateConfiguration(Map<UUID, ReplicaConfig> replicasConfigs) {
        Map<UUID, RemoteKafkaProducer> updatedProducers = new HashMap<>(replicasConfigs.size() - 1);
        List<RemoteKafkaProducer> outdatedProducers = new ArrayList<>(replicasConfigs.size() - 1);

        for (Map.Entry<UUID, RemoteKafkaProducer> entry : producers.entrySet()) {
            if (replicasConfigs.containsKey(entry.getKey())) {
                updatedProducers.put(entry.getKey(), entry.getValue());
            }
            else {
                outdatedProducers.add(entry.getValue());
            }
        }
        for (Map.Entry<UUID, ReplicaConfig> entry : replicasConfigs.entrySet()) {
            ReplicaConfig config = entry.getValue();
            UUID replicaId = entry.getKey();

            if (!replicaId.equals(clusterId.get()) && !updatedProducers.containsKey(replicaId)) {
                updatedProducers.put(replicaId, new RemoteKafkaProducer(
                    config,
                    serializer,
                    kafkaFactory,
                    new UnsubscriberOnFailWrapperImpl(replicaProducersUpdaterProvider.get(), replicaId)
                ));
            }
        }
        producers = updatedProducers;
        closeProducers(outdatedProducers);
    }

    public void unsubscribe(UUID clusterId, Exception e) {
        LOGGER.error("[M] Exception was thrown while sending to {}. It will be unsubscribed", clusterId, e);
        Map<UUID, RemoteKafkaProducer> updatedProducers = new HashMap<>(producers);
        RemoteKafkaProducer crashedProducer = updatedProducers.remove(clusterId);

        producers = updatedProducers;
        closeProducers(Collections.singletonList(crashedProducer));
        ignite.services()
            .serviceProxy(CommandService.SERVICE_NAME, CommandService.class, false)
            .notifyNodeUnsubscribedFromReplica(clusterId);
    }

    public void resubscribe(UUID clusterId, ReplicaConfig replicaConfig) {
        if (producers.containsKey(clusterId)) {
            return;
        }
        Map<UUID, RemoteKafkaProducer> updatedProducers = new HashMap<>(producers);

        updatedProducers.put(clusterId, new RemoteKafkaProducer(
            replicaConfig,
            serializer,
            kafkaFactory,
            new UnsubscriberOnFailWrapperImpl(replicaProducersUpdaterProvider.get(), clusterId)
        ));
        producers = updatedProducers;
    }

    private static void closeProducers(Collection<RemoteKafkaProducer> producers) {
        for (RemoteKafkaProducer producer : producers) {
            try {
                producer.close();
            }
            catch (Exception e) {
                LOGGER.error("[M] Exception while closing producer: ", e);
            }
        }
    }
}

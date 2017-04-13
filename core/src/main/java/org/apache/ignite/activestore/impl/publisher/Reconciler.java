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

package org.apache.ignite.activestore.impl.publisher;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.KeyValueListener;
import org.apache.ignite.activestore.commons.UUIDFormat;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.config.ReplicaMetadata;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadContextLoader;
import org.apache.ignite.activestore.impl.util.ClusterGroupService;
import org.apache.ignite.cluster.ClusterGroup;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/6/2016 2:16 PM
 */
@Singleton
public class Reconciler {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reconciler.class);

    private final Ignite ignite;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final KafkaFactory kafkaFactory;
    private final ClusterGroupService clusterGroupService;
    private final PublisherKafkaService kafkaService;
    private final PublisherReplicaService replicaService;
    private final List<KeyValueListener> allListeners;

    @Inject
    public Reconciler(
        Ignite ignite,
        DataRecoveryConfig dataRecoveryConfig,
        KafkaFactory kafkaFactory,
        ClusterGroupService clusterGroupService,
        PublisherKafkaService kafkaService,
        PublisherReplicaService replicaService,
        List<KeyValueListener> allListeners
    ) {
        this.ignite = ignite;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.kafkaFactory = kafkaFactory;
        this.clusterGroupService = clusterGroupService;
        this.kafkaService = kafkaService;
        this.replicaService = replicaService;
        this.allListeners = allListeners;
    }

    public void startReconciliation(UUID replicaId, long startTransactionId) {
        LOGGER.info("[M] Reconciliation started for replica {} and tx id {}", UUIDFormat.f(replicaId), startTransactionId);
        resubscribeReplicaIfIsOutOfOrder(replicaId);
        if (startTransactionId != LeadContextLoader.NOT_LOADED) {
            kafkaService.seekToTransaction(
                dataRecoveryConfig,
                startTransactionId,
                kafkaFactory,
                replicaService.toGroupId(replicaId));
        }
        ReplicaConfig config = replicaService.getReplicaConfig(replicaId);
        ClusterGroup clusterGroup = clusterGroupService.getReconciliationClusterGroup(ignite, kafkaFactory, config);
        ignite.compute(clusterGroup).broadcast(new StartReconciliationJob(replicaId));
    }

    private void resubscribeReplicaIfIsOutOfOrder(UUID replicaId) {
        Map<UUID, ReplicaMetadata> replicasMetadatas = replicaService.getReplicaMetadatas();
        ReplicaMetadata metadata = replicasMetadatas.get(replicaId);

        if (!metadata.isOutOfOrder()) {
            return;
        }
        ignite.compute().broadcast(new Resubscriber(replicaId));
        replicasMetadatas.put(replicaId, new ReplicaMetadata(metadata.config(), false));
        replicaService.getReplicaMetadatasRef(false).set(replicasMetadatas);
    }

    public void stopReconciliation(UUID replicaId) {
        ignite.compute().broadcast(new StopReconciliationJob(replicaId));
    }

    public void fixMissedIds(LongList missedIds) {
        for (LongIterator it = missedIds.longIterator(); it.hasNext();) {
            long id = it.next();
            for (KeyValueListener listener : allListeners) {
                listener.writeGapTransaction(id);
            }
        }
    }

    private static class StartReconciliationJob extends ActiveStoreIgniteRunnable {
        private final UUID replicaId;

        @Inject
        private transient LocalReconciliationWritersManager reconciliationWritersManager;

        StartReconciliationJob(UUID replicaId) {
            this.replicaId = replicaId;
        }

        @Override protected void runInjected() {
            reconciliationWritersManager.startWriter(replicaId);
        }
    }

    private static class StopReconciliationJob extends ActiveStoreIgniteRunnable {
        private final UUID replicaId;

        @Inject
        private transient LocalReconciliationWritersManager reconciliationWritersManager;

        StopReconciliationJob(UUID replicaId) {
            this.replicaId = replicaId;
        }

        @Override protected void runInjected() {
            reconciliationWritersManager.stopWriter(replicaId);
        }
    }
}

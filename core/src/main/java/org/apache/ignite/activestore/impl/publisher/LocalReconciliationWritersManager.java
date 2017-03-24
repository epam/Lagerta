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

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.util.PropertiesUtil;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Evgeniy_Ignatiev
 * @since 14:51 12/09/2016
 */
@Singleton
public class LocalReconciliationWritersManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalReconciliationWritersManager.class);
    private final ConcurrentMap<UUID, ReconciliationWriter> writers = new ConcurrentHashMap<>();

    @IgniteInstanceResource
    private Ignite ignite;

    @Inject
    private DataRecoveryConfig dataRecoveryConfig;

    @Inject
    private KafkaFactory kafkaFactory;

    @Inject
    private PublisherReplicaService replicaService;

    /**
     * Starts writer to reconciliation topic of replica kafka if it was not already
     * started on local node.
     *
     * @param replicaId Id of replica cluster
     */
    public void startWriter(UUID replicaId) {
        if (!writers.containsKey(replicaId)) {
            ReplicaConfig replicaConfig= replicaService.getReplicaConfig(replicaId);
            Properties mainConsumerProperties = PropertiesUtil.propertiesForGroup(
                dataRecoveryConfig.getConsumerConfig(), replicaService.toGroupId(replicaId));
            ReconciliationWriter writer = new ReconciliationWriter(kafkaFactory, dataRecoveryConfig.getLocalTopic(),
                mainConsumerProperties, replicaConfig.getReconciliationTopic(), replicaConfig.getProducerConfig());
            ReconciliationWriter oldValue = writers.putIfAbsent(replicaId, writer);

            if (oldValue == null) {
                LOGGER.info("[M] Registered new replica for reconciliation: {}", f(replicaId));
                ignite.scheduler().runLocal(new ReconciliationRunner(writer));
            }
        }
    }

    /**
     * Stops writer to reconciliation topic of replica kafka if it is running
     * started on local node.
     *
     * @param replicaId Id of replica cluster
     */
    public void stopWriter(UUID replicaId) {
        ReconciliationWriter writer = writers.remove(replicaId);

        if (writer != null) {
            LOGGER.debug("[M] Stopping reconciliation writer for replica {}", replicaId);
            writer.stop();
        }
    }

    private static class ReconciliationRunner implements IgniteRunnable {
        private final transient ReconciliationWriter writer;

        ReconciliationRunner(ReconciliationWriter writer) {
            this.writer = writer;
        }

        @Override public void run() {
            writer.start();
        }
    }
}

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

package org.apache.ignite.activestore.impl.subscriber.lead;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import gnu.trove.list.TLongList;
import gnu.trove.list.linked.TLongLinkedList;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteRunnable;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.publisher.PublisherKafkaService;
import org.apache.ignite.activestore.impl.util.ClusterGroupService;
import org.apache.ignite.cluster.ClusterGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/12/2016 6:27 PM
 */
public class LeadContextLoader {
    public static final long NOT_LOADED = -1;
    private static final Logger LOGGER = LoggerFactory.getLogger(LeadContextLoader.class);
    private static final String LEAD_LOADER_GROUP_ID_PREFIX = "leadContextLoadingGroup_";

    private final Ignite ignite;
    private final KafkaFactory kafkaFactory;
    private final ClusterGroupService clusterGroupService;
    private final PublisherKafkaService kafkaService;

    private final UUID leadId;

    private final DataRecoveryConfig dataRecoveryConfig;

    private final TLongList sparseCommitted = new TLongLinkedList();

    private final Reference<Long> storedLastDenseCommitted;

    private long lastDenseCommitted = NOT_LOADED;

    private Set<UUID> stalledLoadingJobs;

    private int numberOfLoadingJobs;

    public LeadContextLoader(Ignite ignite, KafkaFactory kafkaFactory, UUID leadId,
        DataRecoveryConfig dataRecoveryConfig,  Reference<Long> storedLastDenseCommitted,
        ClusterGroupService clusterGroupService, PublisherKafkaService kafkaService) {
        this.ignite = ignite;
        this.kafkaFactory = kafkaFactory;
        this.leadId = leadId;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.storedLastDenseCommitted = storedLastDenseCommitted;
        this.clusterGroupService = clusterGroupService;
        this.kafkaService = kafkaService;
    }

    public void start() {
        stopAll(); // Stop previous context loading process if running.
        String groupId = LEAD_LOADER_GROUP_ID_PREFIX + leadId;
        initState(groupId);
        ClusterGroup subcluster = clusterGroupService.getLeadContextLoadingClusterGroup(ignite, kafkaFactory,
            dataRecoveryConfig);

        numberOfLoadingJobs = subcluster.nodes().size();
        LOGGER.info("[L] Start lead load with {}", numberOfLoadingJobs);
        stalledLoadingJobs = new HashSet<>(numberOfLoadingJobs);
        ignite.compute(subcluster).broadcast(new StartContextLoadingJob(leadId, groupId));
    }

    private void initState(String groupId) {
        long storedCommittedValue = storedLastDenseCommitted.get();
        if (storedCommittedValue != NOT_LOADED) {
            kafkaService.seekToTransaction(dataRecoveryConfig, storedCommittedValue, kafkaFactory, groupId);
        }
    }

    public void stopAll() {
        LOGGER.info("[L] Stop all lead loaders");
        ignite.compute().broadcast(new StopContextLoadingJob());
    }

    public void processCommittedTxs(UUID localLoaderId, TLongList txIds) {
        lastDenseCommitted = MergeHelper.mergeWithDenseCompaction(txIds, sparseCommitted, lastDenseCommitted);
        stalledLoadingJobs.remove(localLoaderId);
    }

    public void markCurrentlyFinishedLoader(UUID localLoaderId) {
        stalledLoadingJobs.add(localLoaderId);
        LOGGER.info("[L] Stalled {} {}", f(localLoaderId) , (isContextLoaded() ? "done" : "continue"));
    }

    public long getLastDenseCommitted() {
        return lastDenseCommitted;
    }

    public TLongList getSparseCommitted() {
        return sparseCommitted;
    }

    public boolean isContextLoaded() {
        return stalledLoadingJobs.size() == numberOfLoadingJobs;
    }

    private static class StartContextLoadingJob extends ActiveStoreIgniteRunnable {
        @Inject
        private transient LocalLeadContextLoaderManager manager;

        private final UUID leadId;
        private final String groupId;

        StartContextLoadingJob(UUID leadId, String groupId) {
            this.leadId = leadId;
            this.groupId = groupId;
        }

        @Override protected void runInjected() {
            manager.startContextLoader(leadId, groupId);
        }
    }

    private static class StopContextLoadingJob extends ActiveStoreIgniteRunnable {
        @Inject
        private transient LocalLeadContextLoaderManager manager;

        @Override protected void runInjected() {
            manager.stopContextLoader();
        }
    }
}

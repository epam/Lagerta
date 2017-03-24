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

import com.google.inject.name.Named;
import gnu.trove.list.TLongList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.commons.tasks.ExtendedScheduler;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.config.RPCManager;
import org.apache.ignite.activestore.impl.config.RPCService;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreService;
import org.apache.ignite.activestore.impl.publisher.PublisherKafkaService;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;
import org.apache.ignite.activestore.impl.util.ClusterGroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Aleksandr_Meterko
 * @since 1/23/2017
 */
@SuppressWarnings("WeakerAccess")
public class Lead extends ExtendedScheduler {
    public static final String MAIN_PING_CHECK_KEY = "LeadBeanPingCheck";
    public static final String CONSUMER_PING_CHECK_KEY = "ConsumerPingCheck";

    private static final Logger LOGGER = LoggerFactory.getLogger(Lead.class);
    private static final String LAST_DENSE_COMMITTED_KEY = "LastDenseCommittedTxId";
    private static final long PING_CHECK_PERIOD = 60_000;
    private static final int LAST_DENSE_COMMITTED_SAVE_INTERVAL = 5 * 60_000;

    private final Ignite ignite;
    private final UUID clusterId;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final RPCManager manager;
    private final KafkaFactory kafkaFactory;
    private final LeadPlanningState planningState;

    private final UUID leadId;
    private final Map<UUID, LeadResponse> availableWorkBuffer;
    private final LeadPlanner planner;
    private final LeadContextLoader loader;
    private final ConsumerPingManager pingManager;

    private volatile boolean contextLoaded;
    private volatile boolean kafkaIsOutOfOrder;
    private volatile boolean mainIsOutOfOrder;

    @Inject
    public Lead(
        Ignite ignite,
        @Named(DataCapturerBusConfiguration.CLUSTER_ID) UUID clusterId,
        DataRecoveryConfig dataRecoveryConfig,
        RPCManager manager,
        KafkaFactory kafkaFactory,
        GapDetectionStrategy gapDetectionStrategy,
        PublisherKafkaService kafkaService,
        LeadPlanningState planningState,
        @Named(MAIN_PING_CHECK_KEY) Long mainPingCheckPeriod,
        @Named(CONSUMER_PING_CHECK_KEY) Long consumerPingCheckPeriod,
        ConsumerPingCheckStrategy strategy,
        ClusterGroupService clusterGroupService
    ) {
        this.ignite = ignite;
        this.clusterId = clusterId;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.manager = manager;
        this.kafkaFactory = kafkaFactory;
        this.planningState = planningState;

        leadId = UUID.randomUUID();
        availableWorkBuffer = new ConcurrentHashMap<>();
        planner = new LeadPlanner(planningState);
        pingManager = new ConsumerPingManager(strategy, availableWorkBuffer, planner);

        Reference<Long> storedCommitted = AtomicsHelper.getReference(ignite, LAST_DENSE_COMMITTED_KEY, false);
        storedCommitted.initIfAbsent(LeadContextLoader.NOT_LOADED);

        loader = new LeadContextLoader(ignite, kafkaFactory, leadId, dataRecoveryConfig, storedCommitted,
            clusterGroupService, kafkaService);

        if (mainPingCheckPeriod == null) {
            mainPingCheckPeriod = PING_CHECK_PERIOD;
        }
        if (consumerPingCheckPeriod == null) {
            consumerPingCheckPeriod = PING_CHECK_PERIOD;
        }
        schedulePeriodicTasks(storedCommitted, mainPingCheckPeriod, consumerPingCheckPeriod, gapDetectionStrategy);

        LOGGER.info("[L] New lead created with id {}{}", f(leadId), isReconciliationGoing() ? ", reconciliation is going" : "");
    }

    private void schedulePeriodicTasks(
        Reference<Long> storedCommitted,
        long mainPingCheckPeriod,
        long consumerPingCheckPeriod,
        final GapDetectionStrategy gapDetectionStrategy
    ) {
        schedulePeriodicTask(
            new DenseCommittedIdSavePeriodicTask(planningState, storedCommitted),
            LAST_DENSE_COMMITTED_SAVE_INTERVAL
        );
        schedulePeriodicTask(new MainHeartbeatPeriodicTask(this, manager), mainPingCheckPeriod);
        schedulePeriodicTask(new ConsumerPingCheckPeriodicTask(pingManager), consumerPingCheckPeriod);
        simpleTask(new Runnable() {
            @Override
            public void run() {
                RPCService main = manager.main();
                if (main != null && planningState.isReconciliationGoing() && planner.reconciliationFinishedConditionMet()) {
                    main.get(ActiveCacheStoreService.class, ActiveCacheStoreService.NAME).stopReconciliation(clusterId);
                    planningState.notifyReconcilliationStopped();
                    resumePausedConsumers();
                    LOGGER.info("[L] Reconciliation stopped");
                }
                if (!contextLoaded && loader.isContextLoaded()) {
                    planner.updateContext(loader.getLastDenseCommitted(), loader.getSparseCommitted());
                    loader.stopAll();
                    contextLoaded = true;
                    LOGGER.info("[L] Lead has been loaded");
                }
                if (!planningState.isReconciliationGoing() && gapDetectionStrategy.gapDetected(planningState)) {
                    LOGGER.info("[L] Lead detected gap, starting reconciliation");
                    callReconciliation(true);
                }
            }
        });
        simpleTask(new Runnable() {
            @Override
            public void run() {
                Map<UUID, LeadResponse> ready = planner.plan();
                for (Map.Entry<UUID, LeadResponse> entry : ready.entrySet()) {
                    LeadResponse availableTxs = availableWorkBuffer.remove(entry.getKey());
                    TLongList toCommitIds = entry.getValue().getToCommitIds();
                    if (toCommitIds != null) {
                        LOGGER.debug("[L] Plan you {} on {}", toCommitIds, f(entry.getKey()));
                        planner.markInProgressTransactions(toCommitIds);
                    }
                    availableTxs = availableTxs == null ? entry.getValue() : availableTxs.add(entry.getValue());
                    availableWorkBuffer.put(entry.getKey(), availableTxs);
                }
            }
        });
    }

    public void execute() throws IgniteException {
        super.execute();
    }

    @Override
    protected void onStart() {
        requestConsumersState();
        loader.start();
    }

    //------------------------------------------------------------------------------------------------------------------

    public boolean isInitialized() {
        return contextLoaded;
    }

    public boolean isReconciliationGoing() {
        return planningState.isReconciliationGoing();
    }

    public LeadResponse notifyTransactionsRead(UUID consumerId, List<TransactionMetadata> metadatas) {
        if (running) {
            if (metadatas.size() > 0) {
                LOGGER.debug("[L] Got {} from consumer {}", metadatas, f(consumerId));
            }
            enqueueTask(new NotifyTask(consumerId, pingManager, planner, metadatas));
            if (kafkaIsOutOfOrder && !metadatas.isEmpty()) {
                enqueueTask(new KafkaResurrectionCheckTask(this, dataRecoveryConfig, kafkaFactory));
            }
            LeadResponse result = availableWorkBuffer.remove(consumerId);
            if (result != null) {
                LOGGER.debug("[L] Return {} for {}", result, f(consumerId));
            }
            return result == null ? LeadResponse.EMPTY : result;
        } else {
            if (metadatas.size() > 0) {
                LOGGER.debug("[L] Got {} from consumer {} while dead", metadatas, f(consumerId));
            }
        }
        return LeadResponse.EMPTY;
    }

    public void updateInitialContext(UUID localLoaderId, TLongList txIds) {
        enqueueTask(new UpdateInitialContextTask(localLoaderId, txIds, loader));
    }

    public void notifyLocalLoaderStalled(UUID leadId, UUID localLoaderId) {
        if (this.leadId.equals(leadId)) {
            enqueueTask(new LoaderCurrentlyFinishedTask(localLoaderId, loader));
        }
    }

    public void notifyTransactionsCommitted(UUID consumerId, TLongList transactionsIds) {
        if (running) {
            LOGGER.debug("[L] Committed {} in {}", transactionsIds, f(consumerId));
            enqueueTask(new DoneTask(consumerId, pingManager, planner, transactionsIds));
        } else {
            LOGGER.debug("[L] Committed {} in {}, while dead", transactionsIds, f(consumerId));
        }
    }

    private void requestConsumersState() {
        ignite.compute().broadcast(new ConsumerMetadataRequest());
    }

    private void resumePausedConsumers() {
        ignite.compute().broadcast(new ConsumerResumeRequest());
    }

    public void notifyKafkaIsOutOfOrder(UUID consumerId) {
        kafkaIsOutOfOrder = true;
    }

    public long getLastDenseCommittedId() {
        return planningState.lastDenseCommitted();
    }

    public void notifyKafkaIsOkNow() {
        if (kafkaIsOutOfOrder) {
            kafkaIsOutOfOrder = false;
            callReconciliation(false);
        }
    }

    public void notifyMainIsOutOfOrder() {
        LOGGER.info("[L] Main is unreachable");
        mainIsOutOfOrder = true;
    }

    public void notifyMainIsOkNow() {
        if (mainIsOutOfOrder) {
            LOGGER.info("[L] Main is available");
            mainIsOutOfOrder = false;
            callReconciliation(false);
        }
    }

    private void callReconciliation(boolean gapFound) {
        boolean servicesOk = !kafkaIsOutOfOrder && !mainIsOutOfOrder;
        if (servicesOk && !planningState.isReconciliationGoing()) {
            RPCService main = manager.main();
            if (main != null) {
                LOGGER.info("[L] Reconciliation started");
                ActiveCacheStoreService service = main.get(ActiveCacheStoreService.class, ActiveCacheStoreService.NAME);
                long endTxId = gapFound ? planningState.getLastReadTxId() : service.lastProcessedTxId();
                service.startReconciliation(clusterId, planningState.lastDenseCommitted(), endTxId);
                planningState.notifyReconcilliationStarted(endTxId);
            }
        }
    }
}

/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.subscriber.lead;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.common.CallableKeyListTask;
import com.epam.lathgertha.common.CallableKeyTask;
import com.epam.lathgertha.common.PeriodicRule;
import com.epam.lathgertha.common.Scheduler;
import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.epam.lathgertha.subscriber.util.PlannerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class LeadImpl extends Scheduler implements Lead {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeadImpl.class);

    private static final long SAVE_STATE_PERIOD = 1000L;
    static final long DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD = 60_000;

    private final Set<Long> inProgress = new HashSet<>();
    private final Set<UUID> lostReaders = new HashSet<>();
    private final CallableKeyTask<List<Long>, UUID, List<Long>> toCommit = new CallableKeyListTask<>(this);

    private final CommittedTransactions committed;
    private final ReadTransactions readTransactions;
    private final Heartbeats heartbeats;

    LeadImpl(LeadStateAssistant stateAssistant, ReadTransactions readTransactions, CommittedTransactions committed, Heartbeats heartbeats) {
        this.readTransactions = readTransactions;
        this.committed = committed;
        this.heartbeats = heartbeats;
        pushTask(() -> stateAssistant.load(this));
        registerRule(this.committed::compress);
        registerRule(new PeriodicRule(this::markLostAndFound, DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD));
        registerRule(() -> this.readTransactions.pruneCommitted(this.committed, heartbeats, lostReaders, inProgress));
        registerRule(this::plan);
        registerRule(new PeriodicRule(() -> stateAssistant.saveState(this), SAVE_STATE_PERIOD));
    }

    public LeadImpl(LeadStateAssistant stateAssistant) {
        this(stateAssistant, new ReadTransactions(), CommittedTransactions.createNotReady(), new Heartbeats(DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> notifyRead(UUID readerId, List<TransactionScope> txScopes) {
        if (!txScopes.isEmpty()) {
            LOGGER.trace("[L] notify read from {} ->  {}", readerId, txScopes);
        }
        List<Long> result = !txScopes.isEmpty()
                ? toCommit.call(readerId, () -> readTransactions.addAllOnNode(readerId, txScopes))
                : toCommit.call(readerId);
        if (result != null) {
            LOGGER.trace("[L] ready to commit for {} ->  {}", readerId, result);
        }
        long beat = System.currentTimeMillis();

        pushTask(() -> heartbeats.update(readerId, beat));
        return result == null ? Collections.emptyList() : result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyCommitted(UUID readerId, List<Long> ids) {
        LOGGER.trace("[L] notify committed from {} -> {} ", readerId, ids);
        long beat = System.currentTimeMillis();

        pushTask(() -> {
            committed.addAll(ids);
            inProgress.removeAll(ids);
            heartbeats.update(readerId, beat);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyFailed(UUID readerId, Long id) {
        LOGGER.error("[L] notify failed transaction from {} ->  {}", readerId, id);
        //todo
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState(CommittedTransactions newCommitted) {
        LOGGER.info("[L] initialized {}", newCommitted);
        pushTask(() -> committed.addAll(newCommitted));
        pushTask(() -> readTransactions.pruneCommitted(committed, heartbeats, lostReaders, inProgress));
    }

    private void markLostAndFound() {
        for (UUID consumerId : heartbeats.knownReaders()) {
            boolean knownAsLost = lostReaders.contains(consumerId);

            if (heartbeats.isAvailable(consumerId)) {
                if (knownAsLost) {
                    lostReaders.remove(consumerId);
                    readTransactions.scheduleDuplicatesPruning();
                }
            } else if (!knownAsLost) {
                lostReaders.add(consumerId);
                readTransactions.scheduleDuplicatesPruning();
            }
        }
    }

    private void plan() {
        List<ConsumerTxScope> ready = PlannerUtil.plan(readTransactions, committed, inProgress, lostReaders);
        if (!ready.isEmpty()) {
            LOGGER.trace("[L] Planned {}", ready);
        }
        ready.stream()
                .peek(ConsumerTxScope::markInProgress)
                .peek(scope -> inProgress.add(scope.getTransactionId()))
                .collect(groupingBy(ConsumerTxScope::getConsumerId, toList()))
                .entrySet()
                .forEach(entry -> toCommit.append(entry.getKey(),
                        entry.getValue().stream().map(TransactionScope::getTransactionId).collect(toList())));
    }

    @Override
    public long getLastDenseCommitted() {
        return committed.getLastDenseCommit();
    }
}

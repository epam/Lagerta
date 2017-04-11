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
import com.epam.lathgertha.subscriber.util.PlannerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class LeadImpl extends Scheduler implements Lead {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeadImpl.class);

    private static final long SAVE_STATE_PERIOD = 1000L;

    private final Set<Long> inProgress = new HashSet<>();
    private final CallableKeyTask<List<Long>, UUID, List<Long>> toCommit = new CallableKeyListTask<>(this);

    private final CommittedTransactions committed;
    private final ReadTransactions readTransactions;

    LeadImpl(LeadStateAssistant stateAssistant, ReadTransactions readTransactions, CommittedTransactions committed) {
        this.readTransactions = readTransactions;
        this.committed = committed;
        pushTask(() -> stateAssistant.load(this));
        registerRule(this.committed::compress);
        registerRule(() -> this.readTransactions.pruneCommitted(this.committed));
        registerRule(this::plan);
        registerRule(new PeriodicRule(() -> stateAssistant.saveState(this), SAVE_STATE_PERIOD));
    }

    public LeadImpl(LeadStateAssistant stateAssistant) {
        this(stateAssistant, new ReadTransactions(), CommittedTransactions.createNotReady());
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
        return result == null ? Collections.emptyList() : result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyCommitted(List<Long> ids) {
        pushTask(() -> {
            committed.addAll(ids);
            inProgress.removeAll(ids);
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
        pushTask(() -> committed.addAll(newCommitted));
        pushTask(() -> readTransactions.setReadyAndPrune(committed));
    }

    private void plan() {
        Map<UUID, List<Long>> ready = PlannerUtil.plan(readTransactions, committed, inProgress);
        if (!ready.isEmpty()) {
            LOGGER.trace("[L] Planned {}", ready);
        }
        for (Map.Entry<UUID, List<Long>> entry : ready.entrySet()) {
            inProgress.addAll(entry.getValue());
            toCommit.append(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public long getLastDenseCommitted() {
        return committed.getLastDenseCommit();
    }
}

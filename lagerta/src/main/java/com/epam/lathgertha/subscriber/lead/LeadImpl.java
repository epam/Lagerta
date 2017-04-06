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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class LeadImpl extends Scheduler implements Lead {

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
        registerRule(new PeriodicRule(() -> stateAssistant.saveState(committed), SAVE_STATE_PERIOD));
    }

    public LeadImpl(LeadStateAssistant stateAssistant) {
        this(stateAssistant, new ReadTransactions(), new CommittedTransactions());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> notifyRead(UUID consumerId, List<TransactionScope> scopes) {
        List<Long> result = !scopes.isEmpty()
                ? toCommit.call(consumerId, () -> readTransactions.addAllOnNode(consumerId, scopes))
                : toCommit.call(consumerId);
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
    public void notifyFailed(Long id) {
        //todo
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState(CommittedTransactions newCommitted) {
        pushTask(() -> committed.updateLastDenseCommit(newCommitted));
        pushTask(readTransactions::updateLastDenseRead);
    }

    private void plan() {
        Map<UUID, List<Long>> ready = PlannerUtil.plan(readTransactions, committed, inProgress);
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

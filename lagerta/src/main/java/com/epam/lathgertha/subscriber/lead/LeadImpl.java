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
import com.epam.lathgertha.common.CallableKeyTask;
import com.epam.lathgertha.common.Scheduler;
import com.epam.lathgertha.subscriber.util.PlannerUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.epam.lathgertha.subscriber.lead.NotifyMessage.EMPTY_NOTIFY_MESSAGE;

public class LeadImpl extends Scheduler implements Lead {

    private final Set<Long> inProgress = new HashSet<>();
    private final CallableKeyTask<NotifyMessage, UUID, NotifyMessage> toCommit = new CallableKeyTask<>(this,
            (old, key, append) -> old.append(append));

    private final CommittedTransactions committed;
    private final ReadTransactions readTransactions;

    LeadImpl(ReadTransactions readTransactions, CommittedTransactions committed) {
        this.readTransactions = readTransactions;
        this.committed = committed;
        registerRule(this.committed::compress);
        registerRule(() -> this.readTransactions.pruneCommitted(this.committed));
        registerRule(this::plan);
    }

    public LeadImpl() {
        this(new ReadTransactions(), new CommittedTransactions());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NotifyMessage notifyRead(UUID consumerId, List<TransactionScope> txScopes) {
        NotifyMessage result = !txScopes.isEmpty()
                ? toCommit.call(consumerId, () -> readTransactions.addAllOnNode(consumerId, txScopes))
                : toCommit.call(consumerId);
        return result == null ? EMPTY_NOTIFY_MESSAGE : result;
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

    private void plan() {
        Map<UUID, NotifyMessage> ready = PlannerUtil.plan(readTransactions, committed, inProgress);
        for (Map.Entry<UUID, NotifyMessage> entry : ready.entrySet()) {
            inProgress.addAll(entry.getValue().getToCommit());
            toCommit.append(entry.getKey(), entry.getValue());
        }
    }
}

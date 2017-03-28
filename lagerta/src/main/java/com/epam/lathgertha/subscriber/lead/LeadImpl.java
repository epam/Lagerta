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
import com.epam.lathgertha.common.Scheduler;
import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.epam.lathgertha.subscriber.util.PlannerUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class LeadImpl extends Scheduler implements Lead {

    private final List<ConsumerTxScope> allTransactions = new ArrayList<>();
    private final Set<Long> inProgress = new HashSet<>();

    private final Map<UUID, List<Long>> toCommit = new ConcurrentHashMap<>();

    private final CommittedTransactions committed = new CommittedTransactions();

    public LeadImpl() {
        registerRule(committed::compress);
        registerRule(this::plan);
    }

    @Override
    public List<Long> notifyRead(UUID consumerId, List<TransactionScope> txScopes) {
        pushTask(() -> txScopes.forEach(tx -> addTransaction(consumerId, tx)));
        List<Long> result = toCommit.remove(consumerId);
        return result == null ? Collections.emptyList() : result;
    }

    @Override
    public void notifyCommitted(List<Long> ids) {
        pushTask(() -> {
            committed.addAll(ids);
            inProgress.removeAll(ids);
        });
    }

    private void addTransaction(UUID consumerId, TransactionScope txScopes) {
        allTransactions.add(new ConsumerTxScope(consumerId, txScopes.getTransactionId(), txScopes.getScope()));
    }

    private void plan() {
        allTransactions.sort(Comparator.comparingLong(ConsumerTxScope::getTransactionId));
        Map<UUID, List<Long>> ready = PlannerUtil.plan(allTransactions, committed, inProgress);
        for (Map.Entry<UUID, List<Long>> entry : ready.entrySet()) {
            inProgress.addAll(entry.getValue());
            List<Long> old = toCommit.remove(entry.getKey());
            if (old != null) {
                entry.getValue().addAll(old);
            }
            toCommit.put(entry.getKey(), entry.getValue());
        }
    }
}

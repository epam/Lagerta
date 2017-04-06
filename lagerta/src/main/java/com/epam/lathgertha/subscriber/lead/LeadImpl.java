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
    static final long DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD = 60_000;

    private final Set<Long> inProgress = new HashSet<>();
    private final Set<UUID> lostReaders = new HashSet<>();
    private final CallableKeyTask<List<Long>, UUID, List<Long>> toCommit = new CallableKeyListTask<>(this);

    private final CommittedTransactions committed;
    private final ReadTransactions readTransactions;
    private final Heartbeats heartbeats;

    LeadImpl(ReadTransactions readTransactions, CommittedTransactions committed, Heartbeats heartbeats) {
        this.readTransactions = readTransactions;
        this.committed = committed;
        this.heartbeats = heartbeats;
        registerRule(this.committed::compress);
        registerRule(new PeriodicRule(this::markLostAndFound, DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD));
        registerRule(() -> this.readTransactions.pruneCommitted(this.committed, lostReaders));
        registerRule(this::plan);

    }

    public LeadImpl() {
        this(
                new ReadTransactions(),
                new CommittedTransactions(),
                new Heartbeats(DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> notifyRead(UUID consumerId, List<TransactionScope> txScopes) {
        List<Long> result = !txScopes.isEmpty()
                ? toCommit.call(consumerId, () -> readTransactions.addAllOnNode(consumerId, txScopes))
                : toCommit.call(consumerId);
        long beat = System.currentTimeMillis();

        pushTask(() -> heartbeats.update(consumerId, beat));
        return result == null ? Collections.emptyList() : result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyCommitted(UUID consumerId, List<Long> ids) {
        long beat = System.currentTimeMillis();

        pushTask(() -> {
            committed.addAll(ids);
            inProgress.removeAll(ids);
            heartbeats.update(consumerId, beat);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyFailed(Long id) {
        //todo
    }

    private void markLostAndFound() {
        for (UUID consumerId : heartbeats.knownReaders()) {
            boolean knownAsLost = lostReaders.contains(consumerId);

            if (heartbeats.isAvailable(consumerId)) {
                if (knownAsLost) {
                    lostReaders.remove(consumerId);
                    readTransactions.scheduleDuplicatesPrunning();
                }
            } else if (!knownAsLost) {
                lostReaders.add(consumerId);
                readTransactions.scheduleDuplicatesPrunning();
            }
        }
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

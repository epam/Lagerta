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

import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.subscriber.util.MergeUtil;
import com.epam.lathgertha.util.Atomic;
import com.epam.lathgertha.util.AtomicsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.lang.IgniteAsyncSupported;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringResource;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class LeadStateAssistant {

    private static final String LEAD_STATE_CACHE = "leadStateCache";
    private static final String LOADER_GROUP_ID = "loaderGroupId";

    private final Ignite ignite;
    private final Atomic<Long> commitState;

    public LeadStateAssistant(Ignite ignite) {
        this.ignite = ignite;
        this.commitState = AtomicsHelper.getAtomic(ignite, LEAD_STATE_CACHE);
        commitState.initIfAbsent(CommittedTransactions.INITIAL_READY_COMMIT_ID);
    }

    public void saveState(CommittedTransactions state) {
        commitState.set(state.getLastDenseCommit());
    }

    public void load(Lead lead) {
        IgniteCompute asyncCompute = ignite
                .compute()
                .withAsync();
        asyncCompute
                .call(createLoadTask());
        asyncCompute
                .<CommittedTransactions>future()
                .listen(future -> lead.updateState(future.get()));
    }

    @IgniteAsyncSupported
    private IgniteCallable<CommittedTransactions> createLoadTask() {
        return new IgniteCallable<CommittedTransactions>() {
            @SpringResource(resourceClass = KafkaFactory.class)
            private transient KafkaFactory kafkaFactory;

            @SpringResource(resourceClass = SubscriberConfig.class)
            private transient SubscriberConfig config;

            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public CommittedTransactions call() throws Exception {
                LeadStateLoader loader = new LeadStateLoader(kafkaFactory, config, LOADER_GROUP_ID);
                Atomic<Long> atomic = AtomicsHelper.getAtomic(ignite, LEAD_STATE_CACHE);
                Long lastDense = atomic.get();
                return loader.loadCommitsAfter(lastDense);
            }
        };
    }
}

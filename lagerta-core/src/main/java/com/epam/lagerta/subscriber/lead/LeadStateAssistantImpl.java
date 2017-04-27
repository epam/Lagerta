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
package com.epam.lagerta.subscriber.lead;

import com.epam.lagerta.kafka.KafkaFactory;
import com.epam.lagerta.kafka.config.BasicTopicConfig;
import com.epam.lagerta.services.ReaderService;
import com.epam.lagerta.util.Atomic;
import com.epam.lagerta.util.AtomicsHelper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringResource;

public class LeadStateAssistantImpl implements LeadStateAssistant {

    private static final String LEAD_STATE_CACHE = "leadStateCache";
    private static final String LOADER_GROUP_ID = "loaderGroupId";

    private final Ignite ignite;
    private final Atomic<Long> commitState;

    public LeadStateAssistantImpl(Ignite ignite) {
        this.ignite = ignite;
        commitState = AtomicsHelper.getAtomic(ignite, LEAD_STATE_CACHE);
        commitState.initIfAbsent(CommittedTransactions.INITIAL_READY_COMMIT_ID);
    }

    public void saveState(Lead lead) {
        commitState.set(lead.getLastDenseCommitted());
    }

    public void load(Lead lead) {
        IgniteCompute asyncCompute = ignite
                .compute()
                .withAsync();
        asyncCompute
                .call(new LoadStateTask());
        asyncCompute
                .<CommittedTransactions>future()
                .listen(future -> lead.updateState(future.get()));
        ignite.compute().broadcast(new Resend());
    }

    private static class LoadStateTask implements IgniteCallable<CommittedTransactions> {
        @SpringResource(resourceClass = KafkaFactory.class)
        private transient KafkaFactory kafkaFactory;

        @SpringResource(resourceName = "local-index-config")
        private transient BasicTopicConfig config;

        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public CommittedTransactions call() throws Exception {
            LeadStateLoader loader = new LeadStateLoader(kafkaFactory, config, LOADER_GROUP_ID);
            Atomic<Long> atomic = AtomicsHelper.getAtomic(ignite, LEAD_STATE_CACHE);
            Long lastDense = atomic.get();
            return lastDense > CommittedTransactions.INITIAL_READY_COMMIT_ID
                    ? loader.loadCommitsAfter(lastDense)
                    : new CommittedTransactions();
        }
    }

    private static class Resend implements IgniteRunnable {
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public void run() {
            ReaderService service = ignite.services().service(ReaderService.NAME);
            if (service != null) {
                service.resendReadTransactions();
            }
        }
    }
}

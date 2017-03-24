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

import com.google.inject.Provider;
import gnu.trove.list.TLongList;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.ProxyService;
import org.apache.ignite.activestore.commons.retry.Retries;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.services.ServiceContext;

import javax.inject.Inject;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * @author Andrei_Yakushin
 * @since 12/16/2016 10:37 AM
 */
public class LeadServiceProxyRetry implements LeadService {
    private final Provider<LeadService> lead;

    @Inject
    public LeadServiceProxyRetry(Ignite ignite) {
        lead = new ProxyService<>(ignite, LeadService.class, LeadService.SERVICE_NAME);
    }

    public LeadServiceProxyRetry(Provider<LeadService> lead) {
        this.lead = lead;
    }

    @Override
    public boolean isInitialized() {
        return Retries.tryMe(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return lead.get().isInitialized();
            }
        });
    }

    @Override
    public boolean isReconciliationGoing() {
        return Retries.tryMe(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return lead.get().isReconciliationGoing();
            }
        });
    }

    @Override
    public LeadResponse notifyTransactionsRead(final UUID consumerId, final List<TransactionMetadata> metadatas) {
        return Retries.tryMe(new Callable<LeadResponse>() {
            @Override
            public LeadResponse call() throws Exception {
                return lead.get().notifyTransactionsRead(consumerId, metadatas);
            }
        });
    }

    @Override
    public void notifyTransactionsCommitted(final UUID consumerId, final TLongList transactionsIds) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                lead.get().notifyTransactionsCommitted(consumerId, transactionsIds);
            }
        });
    }

    @Override
    public void updateInitialContext(final UUID localLoaderId, final TLongList txIds) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                lead.get().updateInitialContext(localLoaderId, txIds);
            }
        });
    }

    @Override
    public void notifyLocalLoaderStalled(final UUID leadId, final UUID localLoaderId) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                lead.get().notifyLocalLoaderStalled(leadId, localLoaderId);
            }
        });
    }

    @Override
    public void notifyKafkaIsOutOfOrder(final UUID consumerId) {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                lead.get().notifyKafkaIsOutOfOrder(consumerId);
            }
        });
    }

    @Override public void notifyKafkaIsOkNow() {
        Retries.tryMe(new Runnable() {
            @Override
            public void run() {
                lead.get().notifyKafkaIsOkNow();
            }
        });
    }

    @Override
    public long getLastDenseCommittedId() {
        return Retries.tryMe(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return lead.get().getLastDenseCommittedId();
            }
        });
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ServiceContext ctx) {
        throw new UnsupportedOperationException();
    }
}

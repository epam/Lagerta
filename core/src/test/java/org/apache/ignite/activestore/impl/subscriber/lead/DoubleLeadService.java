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
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;

import javax.inject.Inject;
import java.util.List;
import java.util.UUID;

/**
 * @author Andrei_Yakushin
 * @since 12/27/2016 5:45 PM
 */
public class DoubleLeadService extends LeadServiceAdapter {
    private final Provider<LeadService> real;
    private volatile LeadService workInstead = new LeadServiceAdapter();

    @Inject
    public DoubleLeadService(Ignite ignite) {
        real = new ProxyService<>(ignite, LeadService.class, LeadService.SERVICE_NAME);
    }

    public void setWorkInstead(LeadService workInstead) {
        this.workInstead = workInstead;
    }

    @Override
    public boolean isInitialized() {
        workInstead.isInitialized();
        return real.get().isInitialized();
    }

    @Override
    public boolean isReconciliationGoing() {
        workInstead.isReconciliationGoing();
        return real.get().isReconciliationGoing();
    }

    @Override
    public LeadResponse notifyTransactionsRead(UUID consumerId, List<TransactionMetadata> metadatas) {
        workInstead.notifyTransactionsRead(consumerId, metadatas);
        return real.get().notifyTransactionsRead(consumerId, metadatas);
    }

    @Override
    public void notifyTransactionsCommitted(UUID consumerId, TLongList transactionsIds) {
        workInstead.notifyTransactionsCommitted(consumerId, transactionsIds);
        real.get().notifyTransactionsCommitted(consumerId, transactionsIds);
    }

    @Override
    public void updateInitialContext(UUID localLoaderId, TLongList txIds) {
        workInstead.updateInitialContext(localLoaderId, txIds);
        real.get().updateInitialContext(localLoaderId, txIds);
    }

    @Override
    public void notifyLocalLoaderStalled(UUID leadId, UUID localLoaderId) {
        workInstead.notifyLocalLoaderStalled(leadId, localLoaderId);
        real.get().notifyLocalLoaderStalled(leadId, localLoaderId);
    }

    @Override
    public void notifyKafkaIsOutOfOrder(UUID consumerId) {
        workInstead.notifyKafkaIsOutOfOrder(consumerId);
        real.get().notifyKafkaIsOutOfOrder(consumerId);
    }
}

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

import gnu.trove.list.TLongList;
import org.apache.ignite.activestore.commons.injection.ActiveStoreService;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.services.ServiceContext;

import javax.inject.Inject;
import java.util.List;
import java.util.UUID;

/**
 * @author Evgeniy_Ignatiev
 * @since 11/29/2016 12:27 PM
 */
public class LeadServiceImpl extends ActiveStoreService implements LeadService {
    @Inject
    private transient Lead lead;

    @Override public void cancel(ServiceContext context) {
        lead.cancel();
    }

    @Override public void execute(ServiceContext context) {
        lead.execute();
    }

    @Override public boolean isInitialized() {
        return lead.isInitialized();
    }

    @Override
    public boolean isReconciliationGoing() {
        return lead.isReconciliationGoing();
    }

    @Override public LeadResponse notifyTransactionsRead(UUID consumerId, List<TransactionMetadata> metadatas) {
        return lead.notifyTransactionsRead(consumerId, metadatas);
    }

    @Override public void updateInitialContext(UUID localLoaderId, TLongList txIds) {
        lead.updateInitialContext(localLoaderId, txIds);
    }

    @Override public void notifyLocalLoaderStalled(UUID leadId, UUID localLoaderId) {
        lead.notifyLocalLoaderStalled(leadId, localLoaderId);
    }

    @Override public void notifyTransactionsCommitted(UUID consumerId, TLongList transactionsIds) {
        lead.notifyTransactionsCommitted(consumerId, transactionsIds);
    }

    @Override public void notifyKafkaIsOutOfOrder(UUID consumerId) {
        lead.notifyKafkaIsOutOfOrder(consumerId);
    }

    @Override public long getLastDenseCommittedId() {
        return lead.getLastDenseCommittedId();
    }

    @Override public void notifyKafkaIsOkNow() {
        lead.notifyKafkaIsOkNow();
    }
}

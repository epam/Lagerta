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

import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.services.ServiceContext;
import org.eclipse.collections.api.list.primitive.LongList;

import java.util.List;
import java.util.UUID;

/**
 * @author Andrei_Yakushin
 * @since 12/22/2016 4:06 PM
 */
public class LeadServiceAdapter implements LeadService {
    @Override
    public boolean isInitialized() {
        return false;
    }

    @Override
    public boolean isReconciliationGoing() {
        return false;
    }

    @Override
    public LeadResponse notifyTransactionsRead(UUID consumerId, List<TransactionMetadata> metadatas) {
        return null;
    }

    @Override
    public void notifyTransactionsCommitted(UUID consumerId, LongList transactionsIds) {
    }

    @Override
    public void updateInitialContext(UUID localLoaderId, LongList txIds) {
    }

    @Override
    public void notifyLocalLoaderStalled(UUID leadId, UUID localLoaderId) {
    }

    @Override
    public void notifyKafkaIsOutOfOrder(UUID consumerId) {
    }

    @Override public void notifyKafkaIsOkNow() {
    }

    @Override
    public long getLastDenseCommittedId() {
        return LeadPlanningState.NOT_INITIALIZED;
    }

    @Override
    public void cancel(ServiceContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        throw new UnsupportedOperationException();
    }
}

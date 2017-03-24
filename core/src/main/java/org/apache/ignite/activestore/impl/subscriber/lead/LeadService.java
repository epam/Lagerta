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
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.services.Service;

import java.util.List;
import java.util.UUID;

/**
 * @author Aleksandr_Meterko
 * @since 12/13/2016
 */
public interface LeadService extends Service {

    String SERVICE_NAME = "subscriberLeadService";

    boolean isInitialized();

    boolean isReconciliationGoing();

    LeadResponse notifyTransactionsRead(UUID consumerId, List<TransactionMetadata> metadatas);

    void notifyTransactionsCommitted(UUID consumerId, TLongList transactionsIds);

    void updateInitialContext(UUID localLoaderId, TLongList txIds);

    void notifyLocalLoaderStalled(UUID leadId, UUID localLoaderId);

    void notifyKafkaIsOutOfOrder(UUID consumerId);

    void notifyKafkaIsOkNow();

    long getLastDenseCommittedId();
}

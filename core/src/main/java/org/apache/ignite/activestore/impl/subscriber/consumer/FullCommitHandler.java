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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import gnu.trove.list.TLongList;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;
import org.apache.ignite.lang.IgniteInClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.UUID;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Aleksandr_Meterko
 * @since 12/19/2016
 */
class FullCommitHandler extends TransactionsBufferHolder<FullCommitHandler> implements IgniteInClosure<TLongList> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FullCommitHandler.class);

    private final UUID consumerId;
    private final LeadService lead;

    @Inject
    public FullCommitHandler(@Named(DataCapturerBusConfiguration.NODE_ID) UUID consumerId, LeadService lead) {
        this.consumerId = consumerId;
        this.lead = lead;
    }

    @Override public void apply(TLongList txIds) {
        LOGGER.debug("[C] Committed {} in {}", txIds, f(consumerId));
        buffer.markCommitted(txIds);
        lead.notifyTransactionsCommitted(consumerId, txIds);
    }
}

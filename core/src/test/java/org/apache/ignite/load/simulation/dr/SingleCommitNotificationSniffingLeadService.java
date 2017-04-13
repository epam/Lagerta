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

package org.apache.ignite.load.simulation.dr;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadServiceProxyRetry;
import org.apache.ignite.load.statistics.Statistics;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.LongList;

import javax.inject.Inject;
import java.util.UUID;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/12/2017 2:04 PM
 */
public class SingleCommitNotificationSniffingLeadService extends LeadServiceProxyRetry {
    private final Statistics stats;

    @Inject
    public SingleCommitNotificationSniffingLeadService(Ignite ignite, Statistics stats) {
        super(ignite);
        this.stats = stats;
    }

    @Override
    public void notifyTransactionsCommitted(UUID consumerId, LongList transactionsIds) {
        super.notifyTransactionsCommitted(consumerId, transactionsIds);
        for (LongIterator it = transactionsIds.longIterator(); it.hasNext(); ) {
            stats.recordOperationEndTime(it.next(), System.currentTimeMillis());
        }
    }
}

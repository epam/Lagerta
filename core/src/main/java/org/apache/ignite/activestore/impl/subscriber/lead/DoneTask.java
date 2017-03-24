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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @author Andrei_Yakushin
 * @since 12/12/2016 12:18 PM
 */
class DoneTask extends ConsumerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DoneTask.class);
    private final LeadPlanner planner;
    private final TLongList txIds;

    public DoneTask(UUID consumerId, ConsumerPingManager pingManager, LeadPlanner planner, TLongList txIds) {
        super(consumerId, pingManager);
        this.planner = planner;
        this.txIds = txIds;
    }

    @Override
    public void run() {
        sendPing(false);
        planner.markCommitted(getConsumerId(), txIds);
        LOGGER.debug("[L] Transactions {} are marked as committed", txIds);
    }
}

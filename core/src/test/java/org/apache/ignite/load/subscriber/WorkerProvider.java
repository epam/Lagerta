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

package org.apache.ignite.load.subscriber;

import java.util.concurrent.atomic.AtomicLong;

import gnu.trove.list.array.TLongArrayList;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.load.statistics.Statistics;

/**
 * @author Evgeniy_Ignatiev
 * @since 12:25 01/25/2017
 */
class WorkerProvider implements Provider<LeadLoadWorker> {
    @Inject
    private Lead lead;

    @Inject
    private TxMetadataGenerator metadataGenerator;

    @Inject
    private LeadResponseProcessor responseProcessor;

    @Inject
    @Named(BaseLeadLoadTest.REQUESTS_PERIOD_BIND_NAME)
    private AtomicLong requestsPeriod;

    @Inject
    private Statistics stats;

    private final TxIdGenerator idGenerator;

    private final long[] pattern;

    public WorkerProvider(long[] pattern) {
        this.pattern = pattern;
        idGenerator = new TxIdGenerator(BaseLeadLoadTest.ID_PATTERN_PERIOD, new TLongArrayList(pattern));
    }

    @Override public LeadLoadWorker get() {
        return new LeadLoadWorker(
            lead,
            metadataGenerator,
            responseProcessor,
            requestsPeriod,
            idGenerator,
            stats
        );
    }

    long[] getPattern() {
        return pattern;
    }
}

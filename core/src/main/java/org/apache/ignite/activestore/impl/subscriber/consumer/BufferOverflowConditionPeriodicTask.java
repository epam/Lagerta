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

import org.apache.ignite.activestore.impl.subscriber.lead.LeadService;

/**
 * @author Andrei_Yakushin
 * @since 2/6/2017 4:25 PM
 */
public class BufferOverflowConditionPeriodicTask implements Runnable {
    private final SubscriberConsumer consumer;
    private final TransactionsBuffer buffer;
    private final BufferOverflowCondition bufferOverflowCondition;
    private final LeadService lead;

    public BufferOverflowConditionPeriodicTask(SubscriberConsumer consumer, TransactionsBuffer buffer, BufferOverflowCondition bufferOverflowCondition, LeadService lead) {
        this.consumer = consumer;
        this.buffer = buffer;
        this.bufferOverflowCondition = bufferOverflowCondition;
        this.lead = lead;
    }

    @Override
    public void run() {
        if (!consumer.isPaused() && bufferOverflowCondition.check(buffer) && lead.isReconciliationGoing()) {
            consumer.pause();
        }
    }
}

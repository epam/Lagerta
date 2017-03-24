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

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadResponse;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.load.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Evgeniy_Ignatiev
 * @since 0:13 01/20/2017
 */
public class LeadLoadWorker implements LifecycleAware, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeadLoadWorker.class);
    private static final AtomicLong WORKERS_COUNTER = new AtomicLong();

    private final Lead lead;
    private final TxMetadataGenerator metadataGenerator;
    private final LeadResponseProcessor responseProcessor;
    private final AtomicLong requestsPeriod;
    private final TxIdGenerator idGenerator;
    private final Statistics stats;
    private final UUID consumerId;

    private final List<TransactionMetadata> metadatas = new ArrayList<>(BaseLeadLoadTest.BATCH_SIZE);

    private long lastNotifyReadTime;

    private volatile boolean running = true;

    public LeadLoadWorker(
        Lead lead,
        TxMetadataGenerator metadataGenerator,
        LeadResponseProcessor responseProcessor,
        AtomicLong requestsPeriod,
        TxIdGenerator idGenerator,
        Statistics stats
    ) {
        this.lead = lead;
        this.metadataGenerator = metadataGenerator;
        this.responseProcessor = responseProcessor;
        this.requestsPeriod = requestsPeriod;
        this.idGenerator = idGenerator;
        this.stats = stats;

        long workerNumber = WORKERS_COUNTER.getAndIncrement();
        consumerId = new UUID(workerNumber, workerNumber);
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        running = false;
    }

    @Override public void run() {
        lastNotifyReadTime = System.currentTimeMillis();
        try {
            while (running) {
                fillBatch();
                long sleepTime = requestsPeriod.get() - timeSinceLastRequest();

                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
                long requestStartTime = System.currentTimeMillis();
                LeadResponse response = lead.notifyTransactionsRead(consumerId, new ArrayList<>(metadatas));

                lastNotifyReadTime = System.currentTimeMillis();
                recordTxProcessing(requestStartTime, metadatas, lastNotifyReadTime, response.getToCommitIds());
                responseProcessor.processResponse(consumerId, response);
                metadatas.clear();
            }
        }
        catch (InterruptedException e) {
            // Do nothing.
        }
        catch (Exception e) {
            LOGGER.error("[T] Error while applying load: ", e);
        }
    }

    private long timeSinceLastRequest() {
        return System.currentTimeMillis() - lastNotifyReadTime;
    }

    private void fillBatch() {
        for (int i = 0; i < BaseLeadLoadTest.BATCH_SIZE; i++) {
            metadatas.add(metadataGenerator.generateTx(idGenerator.nextId()));
        }
    }

    private void recordTxProcessing(
        long startTime,
        List<TransactionMetadata> metadatas,
        long endTime,
        TLongList toCommitIds
    ) {
        for (TransactionMetadata metadata : metadatas) {
           stats.recordOperationStartTime(metadata.getTransactionId(), startTime);
        }
        if (toCommitIds != null) {
            for (TLongIterator it = toCommitIds.iterator(); it.hasNext(); ) {
                stats.recordOperationEndTime(it.next(), endTime);
            }
        }
    }
}

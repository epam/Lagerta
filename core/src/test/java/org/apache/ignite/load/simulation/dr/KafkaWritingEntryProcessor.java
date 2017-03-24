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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import javax.cache.Cache;
import javax.inject.Inject;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.config.UnsubscriberOnFailWrapper;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.publisher.RemoteKafkaProducer;
import org.apache.ignite.activestore.commons.serializer.Serializer;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.load.LoadTestConfig;
import org.apache.ignite.load.WorkerEntryProcessor;
import org.apache.ignite.load.simulation.Account;
import org.apache.ignite.load.simulation.AccountKey;
import org.apache.ignite.load.simulation.AccountTransaction;
import org.apache.ignite.load.simulation.AccountTransactionKey;
import org.apache.ignite.load.simulation.TransactionData;
import org.apache.ignite.load.statistics.Statistics;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 7:50 PM
 */
public class KafkaWritingEntryProcessor implements WorkerEntryProcessor, LifecycleAware {
    private static final UnsubscriberOnFailWrapper NOPE_KILLER = new UnsubscriberOnFailWrapper() {
        @Override
        public Future<RecordMetadata> wrap(Future<RecordMetadata> future) {
            return future;
        }

        @Override
        public Future<RecordMetadata> empty(Exception e) {
            throw new RuntimeException(e);
        }
    };

    private final LoadTestConfig config;
    private final Statistics stats;
    private final IdSequencer idSequencer;
    private final RemoteKafkaProducer producer;

    private volatile boolean running = true;

    @Inject
    public KafkaWritingEntryProcessor(
        LoadTestConfig config,
        Statistics stats,
        IdSequencer idSequencer,
        DataRecoveryConfig dataRecoveryConfig,
        Serializer serializer,
        KafkaFactory kafkaFactory
    ) {
        this.config = config;
        this.stats = stats;
        this.idSequencer = idSequencer;
        producer = new RemoteKafkaProducer(
            dataRecoveryConfig.getReplicaConfig(null),
            serializer,
            kafkaFactory,
            NOPE_KILLER
        );
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        running = false;
        producer.close();
    }

    @Override public void process(Map<?, ?> entries) {
        if (!running) {
            return;
        }
        long txId = idSequencer.getNextId();
        Collection<Cache.Entry<?, ?>> updates = txDataToCacheUpdates(entries);

        producer.writeTransaction(txId, Collections.singletonMap(config.getCacheName(), updates));
        stats.recordOperationStartTime(txId, System.currentTimeMillis());
    }

    private static Collection<Cache.Entry<?, ?>> txDataToCacheUpdates(Map<?, ?> txDataEntries) {
        Collection<Cache.Entry<?, ?>> updates = new ArrayList<>();

        for (Object value : txDataEntries.values()) {
            TransactionData txData = (TransactionData)value;
            AccountTransactionKey txKey = new AccountTransactionKey(txData.getTransactionId(),
                txData.getPartition());
            AccountTransaction accountTx = new AccountTransaction(
                txKey,
                txData.getFromAccountId(),
                txData.getToAccountId(),
                txData.getMoneyAmount()
            );
            AccountKey toAccountKey = new AccountKey(txData.getToAccountId(), txData.getPartition());
            AccountKey fromAccountKey = new AccountKey(txData.getFromAccountId(), txData.getPartition());
            Account toAccount = new Account(toAccountKey);
            Account fromAccount = new Account(fromAccountKey);
            long timestamp = System.currentTimeMillis();

            fromAccount.addTransaction(timestamp, txKey);
            toAccount.addTransaction(timestamp, txKey);

            updates.add(new CacheEntryImpl<>(txKey, accountTx));
            updates.add(new CacheEntryImpl<>(toAccountKey, toAccount));
            updates.add(new CacheEntryImpl<>(fromAccountKey, fromAccount));
        }
        return updates;
    }
}

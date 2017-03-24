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

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.kv.MessageMetadata;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.activestore.subscriber.Committer;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author Aleksandr_Meterko
 * @since 11/29/2016
 */
public class SubscriberConsumer {

    private static final int POLL_TIMEOUT = 200;

    private final Ignite ignite;
    private final PollingRunnable pollingRunnable;

    public SubscriberConsumer(Serializer serializer, Properties properties, String topic, Ignite ignite) {
        this.ignite = ignite;
        this.pollingRunnable = new PollingRunnable(ignite, new KafkaConsumer<ByteBuffer, ByteBuffer>(properties),
            serializer, topic, new IgniteCommitter(ignite));
    }

    public void startPolling() {
        ignite.scheduler().runLocal(pollingRunnable);
    }

    public void stopPolling() {
        pollingRunnable.cancel();
    }

    private static class PollingRunnable implements Runnable {

        private final UUID consumerId;
        private final Lead leadProxy;
        private final Consumer<ByteBuffer, ByteBuffer> consumer;
        private final Map<Long, TransactionWrapper> buffer = new ConcurrentHashMap<>();
        private final Serializer serializer;
        private final String topic;
        private final Committer committer;
        private final DoneNotifier doneNotifier;
        private final DeserializerClosure deserializerClosure;

        private boolean running = true;

        PollingRunnable(Ignite ignite, Consumer<ByteBuffer, ByteBuffer> consumer, Serializer serializer,
            String topic, Committer committer) {
            this.consumer = consumer;
            this.serializer = serializer;
            this.topic = topic;
            this.committer = committer;
            this.consumerId = ignite.cluster().localNode().id();
            this.leadProxy = ignite.services().serviceProxy(Lead.SERVICE_NAME, Lead.class, false);
            this.doneNotifier = new DoneNotifier(leadProxy, buffer);
            this.deserializerClosure = new DeserializerClosure(buffer, serializer);
        }

        @Override public void run() {
            consumer.subscribe(Collections.singletonList(topic));
            try {
                while (running) {
                    ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);
                    List<MessageMetadata> metadatas = new ArrayList<>(records.count());

                    for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                        MessageMetadata metadata = (MessageMetadata)serializer.deserialize(record.key());
                        TransactionWrapper wrapper = new TransactionWrapper(metadata, record.value());

                        metadatas.add(metadata);
                        buffer.put(metadata.getTransactionId(), wrapper);
                    }
                    Collections.sort(metadatas, new MetadataComparator());
                    List<Long> transactionsToCommit = leadProxy.notifyTransactionsRead(consumerId, metadatas);

                    if (!transactionsToCommit.isEmpty()) {
                        committer.commit(transactionsToCommit, deserializerClosure, doneNotifier);
                    }
                    // TODO commit transaction only after applying in ignite
                    consumer.commitSync();
                }
            }
            finally {
                consumer.unsubscribe();
            }
        }

        void cancel() {
            running = false;
        }
    }

    private static class MetadataComparator implements Comparator<MessageMetadata> {
        @Override public int compare(MessageMetadata o1, MessageMetadata o2) {
            return Long.compare(o1.getTransactionId(), o2.getTransactionId());
        }
    }

    private static class DeserializerClosure implements IgniteClosure<Long, Map.Entry<List<IgniteBiTuple<String, ?>>, List<Object>>> {

        private final transient Map<Long, TransactionWrapper> buffer;
        private final Serializer serializer;

        DeserializerClosure(Map<Long, TransactionWrapper> buffer, Serializer serializer) {
            this.buffer = buffer;
            this.serializer = serializer;
        }

        @SuppressWarnings("unchecked")
        @Override public Map.Entry<List<IgniteBiTuple<String, ?>>, List<Object>> apply(Long txId) {
            TransactionWrapper currentTx = buffer.get(txId);
            List<IgniteBiTuple<String, ?>> keys = currentTx.metadata().getCompositeKeys();
            List<Object> values = (List<Object>)serializer.deserialize(currentTx.data());
            return new AbstractMap.SimpleImmutableEntry<>(keys, values);
        }
    }

    private static class DoneNotifier implements IgniteInClosure<Long> {

        private final Lead leadProxy;
        private final transient Map<Long, TransactionWrapper> buffer;

        DoneNotifier(Lead leadProxy, Map<Long, TransactionWrapper> buffer) {
            this.leadProxy = leadProxy;
            this.buffer = buffer;
        }

        @Override public void apply(Long txId) {
            // ToDo: batch committed ids.
            leadProxy.notifyTransactionCommitted(txId);
            buffer.remove(txId);
        }
    }
}

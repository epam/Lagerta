/*
 * Copyright 2017 EPAM Systems.
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

package com.epam.lagerta.subscriber.lead;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.kafka.DataRecoveryConfig;
import com.epam.lagerta.kafka.KafkaFactory;
import com.epam.lagerta.kafka.SubscriberConfig;
import com.epam.lagerta.util.Serializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.epam.lagerta.util.TransactionPartitionUtil.partition;
import static java.util.stream.Collectors.toCollection;

public class ReconcilerImpl implements Reconciler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReconcilerImpl.class);
    private static final int POLL_TIMEOUT = 200;

    private final String inputTopic;
    private final String reconciliationTopic;
    private final KafkaFactory kafkaFactory;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final Serializer serializer;
    private final ByteBuffer transactionValueTemplate;
    private volatile boolean reconciliationGoing;

    public ReconcilerImpl(KafkaFactory kafkaFactory, DataRecoveryConfig dataRecoveryConfig,
                          SubscriberConfig subscriberConfig, Serializer serializer) {
        this.kafkaFactory = kafkaFactory;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.serializer = serializer;
        inputTopic = subscriberConfig.getInputTopic();
        reconciliationTopic = dataRecoveryConfig.getReconciliationTopic();
        transactionValueTemplate = serializer.serialize(Collections.emptyList());
    }

    @Override
    public boolean isReconciliationGoing() {
        return reconciliationGoing;
    }

    @Override
    public void startReconciliation(List<Long> gaps) {
        reconciliationGoing = true;
        try (GapFixer gapFixer = new GapFixer()) {
            gapFixer.startReconciliation(gaps);
        } finally {
            reconciliationGoing = false;
        }
    }

    private class GapFixer implements AutoCloseable {
        private final Producer<ByteBuffer, ByteBuffer> producer;
        private final Consumer<ByteBuffer, ByteBuffer> consumer;

        public GapFixer() {
            producer = kafkaFactory.producer(dataRecoveryConfig.getProducerConfig());
            consumer = kafkaFactory.consumer(dataRecoveryConfig.getConsumerConfig());
        }

        public void startReconciliation(List<Long> gaps) {
            Map<Integer, TopicPartition> reconPartitions = producer.partitionsFor(inputTopic).stream()
                    .collect(Collectors.toMap(
                            PartitionInfo::partition,
                            info -> new TopicPartition(info.topic(), info.partition())
                    ));
            int partitions = reconPartitions.size();
            Map<TopicPartition, List<Long>> txByPartition = gaps.stream()
                    .collect(Collectors.groupingBy(
                            txId -> reconPartitions.get(partition(txId, partitions)),
                            toCollection(ArrayList::new))
                    );
            seekToMissingTransactions(txByPartition);
            txByPartition.forEach(this::resendMissingTransactions);
        }

        private void seekToMissingTransactions(Map<TopicPartition, List<Long>> txByPartition) {
            Map<TopicPartition, Long> timestamps = txByPartition.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> Collections.min(entry.getValue())
                    ));
            Map<TopicPartition, OffsetAndTimestamp> foundOffsets = consumer.offsetsForTimes(timestamps);
            Map<TopicPartition, OffsetAndMetadata> toCommit = foundOffsets.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> {
                                long offset = entry.getValue() != null? entry.getValue().offset() : 0;
                                return new OffsetAndMetadata(offset);
                            }
                    ));
            consumer.commitSync(toCommit);
        }

        // todo for now loading in one thread, but probably will need to do it in parallel
        private void resendMissingTransactions(TopicPartition topicPartition, List<Long> txIds) {
            consumer.assign(Collections.singleton(topicPartition));
            long lastOffset = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
            long currentOffset = 0;
            while (currentOffset < lastOffset - 1 && !txIds.isEmpty()) {
                for (ConsumerRecord<ByteBuffer, ByteBuffer> record : consumer.poll(POLL_TIMEOUT)) {
                    currentOffset = Math.max(currentOffset, record.offset());
                    processSingleRecord(txIds, record);
                }
            }
            txIds.forEach(this::sendEmptyTransaction);
        }

        private void processSingleRecord(List<Long> txIds, ConsumerRecord<ByteBuffer, ByteBuffer> record) {
            long txId = record.timestamp();
            boolean found = txIds.remove(txId);
            if (found) {
                ProducerRecord<ByteBuffer, ByteBuffer> producerRecord =
                        new ProducerRecord<>(reconciliationTopic, record.key(), record.value());
                producer.send(producerRecord);
            }
        }

        private void sendEmptyTransaction(Long txId) {
            TransactionScope scope = new TransactionScope(txId, Collections.emptyList());
            ByteBuffer key = serializer.serialize(scope);
            ProducerRecord<ByteBuffer, ByteBuffer> producerRecord =
                    new ProducerRecord<>(reconciliationTopic, key, transactionValueTemplate);
            LOGGER.warn("[L] Reconciler is sending empty transaction with id {}", txId);
            producer.send(producerRecord);
        }

        @Override
        public void close() {
            producer.close();
            consumer.close();
        }
    }
}

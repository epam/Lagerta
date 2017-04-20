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

import com.epam.lagerta.kafka.DataRecoveryConfig;
import com.epam.lagerta.kafka.KafkaFactory;
import com.epam.lagerta.kafka.SubscriberConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.epam.lagerta.util.TransactionPartitionUtil.partition;
import static java.util.stream.Collectors.toCollection;

public class ReconcilerImpl implements Reconciler {

    private static final int POLL_TIMEOUT = 200;

    private final String remoteTopic;
    private final String reconciliationTopic;
    private final Producer<ByteBuffer, ByteBuffer> producer;
    private final Consumer<ByteBuffer, ByteBuffer> consumer;
    private volatile boolean reconciliationGoing;

    public ReconcilerImpl(KafkaFactory kafkaFactory, DataRecoveryConfig dataRecoveryConfig,
                          SubscriberConfig subscriberConfig) {
        this.remoteTopic = subscriberConfig.getRemoteTopic();
        this.reconciliationTopic = dataRecoveryConfig.getReconciliationTopic();
        this.producer = kafkaFactory.producer(dataRecoveryConfig.getProducerConfig());
        this.consumer = kafkaFactory.consumer(dataRecoveryConfig.getConsumerConfig());
    }

    @Override
    public boolean isReconciliationGoing() {
        return reconciliationGoing;
    }

    @Override
    public void startReconciliation(List<Long> gaps) {
        reconciliationGoing = true;
        int reconPartitions = producer.partitionsFor(remoteTopic).size();
        Map<Integer, List<Long>> txByPartition = gaps.stream()
                .collect(Collectors.groupingBy(txId -> partition(txId, reconPartitions), toCollection(ArrayList::new)));
        seekToMissingTransactions(txByPartition);
        txByPartition.entrySet().forEach(entry -> resendMissingTransactions(entry.getKey(), entry.getValue()));
        reconciliationGoing = false;
    }

    private void seekToMissingTransactions(Map<Integer, List<Long>> txByPartition) {
        Map<TopicPartition, OffsetAndMetadata> toCommit = txByPartition.entrySet().stream()
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), Collections.min(entry.getValue())))
                .collect(Collectors.toMap(
                        entry -> new TopicPartition(remoteTopic, entry.getKey()),
                        entry -> new OffsetAndMetadata(entry.getValue())
                ));
        consumer.commitSync(toCommit);
    }

    // todo for now loading in one thread, but probably will need to do it in parallel
    private void resendMissingTransactions(int partition, List<Long> txIds) {
        TopicPartition topicPartition = new TopicPartition(remoteTopic, partition);
        consumer.assign(Collections.singleton(topicPartition));
        long lastOffset = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
        long currentOffset = 0;
        while (currentOffset < lastOffset - 1 || !txIds.isEmpty()) {
            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : consumer.poll(POLL_TIMEOUT)) {
                currentOffset = Math.max(currentOffset, record.offset());
                processSingleRecord(txIds, record);
            }
        }
        sendEmptyTransactions(txIds);
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

    // todo implement in later ticket along with its processing in Reader class
    private void sendEmptyTransactions(List<Long> txIds) {
    }

}

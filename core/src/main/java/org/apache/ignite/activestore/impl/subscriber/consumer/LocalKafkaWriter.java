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

import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.transactions.TransactionMessageUtil;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 9:09 AM
 */
class LocalKafkaWriter {
    private final String topic;
    private final Producer<ByteBuffer, ByteBuffer> producer;
    private final int partitions;

    @Inject
    public LocalKafkaWriter(KafkaFactory kafkaFactory, DataRecoveryConfig dataRecoveryConfig, OnKafkaStop onKafkaStop) {
        topic = dataRecoveryConfig.getLocalTopic();
        producer = kafkaFactory.producer(dataRecoveryConfig.getProducerConfig(), onKafkaStop);
        this.partitions = producer.partitionsFor(topic).size();
    }

    public void writeTransactions(TransactionWrapper txWrapper) {
        long transactionId = txWrapper.id();
        int partition = TransactionMessageUtil.partitionFor(txWrapper.deserializedMetadata(), partitions);

        producer.send(new ProducerRecord<>(topic, partition, transactionId, txWrapper.metadata(), txWrapper.data()),
                new LoggingErrorHandler());
    }

    public void close() {
        producer.close();
    }
}

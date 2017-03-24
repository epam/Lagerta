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

package org.apache.ignite.activestore.impl.publisher;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.transactions.TransactionMessageUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/8/2016 4:00 PM
 */
public class ReconciliationWriter {
    private static final long POLL_TIMEOUT = 200;

    private final KafkaFactory kafkaFactory;
    private final String localTopic;
    private final Properties localConsumerProperties;
    private final String reconciliationTopic;
    private final Properties replicaProducerProperties;

    private volatile boolean running = false;

    public ReconciliationWriter(KafkaFactory kafkaFactory, String localTopic, Properties mainConsumerProperties,
        String reconciliationTopic, Properties replicaProducerProperties) {
        this.kafkaFactory = kafkaFactory;
        this.localTopic = localTopic;
        this.localConsumerProperties = mainConsumerProperties;
        this.reconciliationTopic = reconciliationTopic;
        this.replicaProducerProperties = replicaProducerProperties;
    }

    public void start() {
        running = true;

        Runnable onStop = new Runnable() {
            @Override
            public void run() {
                //todo add logic
            }
        };

        try (Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(localConsumerProperties, onStop);
             Producer<ByteBuffer, ByteBuffer> producer = kafkaFactory.producer(replicaProducerProperties)) {
            int partitions = producer.partitionsFor(reconciliationTopic).size();

            consumer.subscribe(Collections.singletonList(localTopic));
            while (running) {
                ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                    long transactionId = record.timestamp();
                    int partition = TransactionMessageUtil.partitionFor(transactionId, partitions);

                    producer.send(new ProducerRecord<>(reconciliationTopic, partition, record.key(), record.value()));
                }
            }
        }
    }

    public void stop() {
        running = false;
    }
}

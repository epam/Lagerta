/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lagerta.kafka;

import com.epam.lagerta.kafka.config.BasicTopicConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class KafkaLogCommitterImpl implements KafkaLogCommitter {

    private final String logTopic;
    private final Producer producer;

    public KafkaLogCommitterImpl(KafkaFactory kafkaFactory, BasicTopicConfig localIndexConfig) {
        producer = kafkaFactory.producer(localIndexConfig.getKafkaConfig().getProducerConfig());
        logTopic = localIndexConfig.getTopic();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<RecordMetadata> commitTransaction(long transactionId) {
        int partition = 0;  //todo fix me in #208
        ProducerRecord record = new ProducerRecord(logTopic, partition, transactionId, null, null);
        return producer.send(record);
    }

    @Override
    public void close() {
        producer.close();
    }
}

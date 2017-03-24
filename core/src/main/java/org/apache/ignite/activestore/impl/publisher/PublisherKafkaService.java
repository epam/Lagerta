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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.inject.Singleton;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Evgeniy_Ignatiev
 * @since 12:11 12/09/2016
 */
@Singleton
public class PublisherKafkaService {

    public void seekToTransaction(DataRecoveryConfig config, long transactionId, KafkaFactory kafkaFactory,
        String groupId) {
        String topic = config.getLocalTopic();
        Properties consumerProperties = PropertiesUtil.propertiesForGroup(config.getConsumerConfig(), groupId);

        try (Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(consumerProperties)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            Map<TopicPartition, Long> seekMap = new HashMap<>(partitionInfos.size());

            for (PartitionInfo partitionInfo : partitionInfos) {
                seekMap.put(new TopicPartition(topic, partitionInfo.partition()), transactionId);
            }
            consumer.assign(seekMap.keySet());
            Map<TopicPartition, OffsetAndTimestamp> foundOffsets = consumer.offsetsForTimes(seekMap);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : foundOffsets.entrySet()) {
                if (entry.getValue() != null) {
                    offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
                }
            }
            consumer.commitSync(offsetsToCommit);
        }
    }
}

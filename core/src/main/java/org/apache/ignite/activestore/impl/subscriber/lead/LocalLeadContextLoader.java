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

package org.apache.ignite.activestore.impl.subscriber.lead;

import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/12/2016 4:30 PM
 */
class LocalLeadContextLoader implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalLeadContextLoader.class);

    private static final long POLL_TIMEOUT = 200;

    private final KafkaFactory kafkaFactory;
    private final UUID leadId;
    private final LeadService lead;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final UUID localLoaderId;
    private final String groupId;

    private volatile boolean running = true;

    // Stalled status may change if one loaders mid-work and kafka rebalances partitions to this one.
    private boolean stalled = false;

    public LocalLeadContextLoader(KafkaFactory kafkaFactory, UUID leadId, LeadService lead,
        DataRecoveryConfig dataRecoveryConfig, UUID localLoaderId, String groupId) {
        this.kafkaFactory = kafkaFactory;
        this.leadId = leadId;
        this.lead = lead;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.localLoaderId = localLoaderId;
        this.groupId = groupId;
    }

    @Override public void run() {
        LOGGER.info("[I] Start lead loading in {} / {}", f(leadId), f(localLoaderId));
        Runnable onStop = new Runnable() {
            @Override
            public void run() {
                lead.notifyKafkaIsOutOfOrder(localLoaderId);
            }
        };
        Properties localKafkaProperties = PropertiesUtil.propertiesForGroup(dataRecoveryConfig.getConsumerConfig(),
            groupId);

        try (Consumer<ByteBuffer, ByteBuffer> consumer = kafkaFactory.consumer(localKafkaProperties, onStop)) {
            String topic = dataRecoveryConfig.getLocalTopic();

            consumer.subscribe(Collections.singletonList(topic));
            while (running) {
                try {
                    pollCommunicateOnce(consumer);
                } catch (Exception e) {
                    LOGGER.error("[I]", e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("[I] Kafka throws exception", e);
        }
        LOGGER.info("[I] Finish lead loading in {} / {}", f(leadId), f(localLoaderId));
    }

    private void pollCommunicateOnce(Consumer<ByteBuffer, ByteBuffer> consumer) {
        ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);

        if (records.isEmpty()) {
            if (!stalled && checkStalled(consumer)) {
                LOGGER.info("[I] Loader stalled {} / {}", f(leadId), f(localLoaderId));
                stalled = true;
                lead.notifyLocalLoaderStalled(leadId, localLoaderId);
            }
            // ToDo: Consider sending empty messages for heartbeat sake.
            return;
        }
        if (stalled) {
            stalled = false;
        }
        MutableLongList committedIds = new LongArrayList(records.count());

        for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
            committedIds.add(record.timestamp());
        }
        committedIds.sortThis();
        lead.updateInitialContext(localLoaderId, committedIds);
        consumer.commitSync();
    }

    private static boolean checkStalled(Consumer<ByteBuffer, ByteBuffer> consumer) {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> lastOffsets = consumer.endOffsets(assignment);

        for (TopicPartition topicPartition : assignment) {
            if (consumer.position(topicPartition) != lastOffsets.get(topicPartition)) {
                return false;
            }
        }
        return true;
    }

    public void cancel() {
        running = false;
    }
}

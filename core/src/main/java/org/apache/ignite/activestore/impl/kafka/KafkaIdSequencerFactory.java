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

package org.apache.ignite.activestore.impl.kafka;

import javax.inject.Inject;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.SerializableProvider;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;

/**
 * @author Evgeniy_Ignatiev
 * @since 13:03 11/28/2016
 */
public class KafkaIdSequencerFactory implements SerializableProvider<IdSequencer> {
    private String topic;

    @Inject
    private transient KafkaFactory kafkaFactory;

    @Inject
    private transient DataRecoveryConfig dataRecoveryConfig;

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override public KafkaIdSequencer get() {
        return new KafkaIdSequencer(topic, kafkaFactory.producer(dataRecoveryConfig.getProducerConfig()),
            kafkaFactory.consumer(dataRecoveryConfig.getConsumerConfig()));
    }
}

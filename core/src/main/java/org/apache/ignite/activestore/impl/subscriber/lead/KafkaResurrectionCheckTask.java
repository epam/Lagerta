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
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Andrei_Yakushin
 * @since 12/16/2016 2:36 PM
 */
class KafkaResurrectionCheckTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResurrectionCheckTask.class);

    private final Lead lead;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final KafkaFactory kafkaFactory;

    public KafkaResurrectionCheckTask(
        Lead lead,
        DataRecoveryConfig dataRecoveryConfig,
        KafkaFactory kafkaFactory
    ) {
        this.lead = lead;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.kafkaFactory = kafkaFactory;
    }

    @Override
    public void run() {
        if (isKafkaOk()) {
            lead.notifyKafkaIsOkNow();
        }
    }

    private boolean isKafkaOk() {
        Properties config = dataRecoveryConfig.getConsumerConfig();

        try (Consumer<?, ?> consumer = kafkaFactory.consumer(config, new NoOp())) {
            // Simplistic check to ensure that broker is alive.
            return consumer.partitionsFor(dataRecoveryConfig.getRemoteTopic()) != null;
        }
        catch (Exception e) {
            LOGGER.warn("[L] Exception while instantiating consumer: ", e);
            return false;
        }
    }

    private static class NoOp implements Runnable {
        @Override public void run() {
            // Do nothing.
        }
    }
}

/*
 * Copyright 2017 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.lagerta.kafka.config;

import java.io.Serializable;
import java.util.Properties;

public class SubscriberConfig implements Serializable {
    private final String inputTopic;
    private final String reconciliationTopic;
    private final String gapTopic;
    private final Properties producerConfig;

    public SubscriberConfig(String inputTopic, String reconciliationTopic, String gapTopic, Properties producerConfig) {
        this.inputTopic = inputTopic;
        this.reconciliationTopic = reconciliationTopic;
        this.gapTopic = gapTopic;
        this.producerConfig = producerConfig;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getReconciliationTopic() {
        return reconciliationTopic;
    }

    public String getGapTopic() {
        return gapTopic;
    }

    public Properties getProducerConfig() {
        return producerConfig;
    }
}

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

public class ClusterConfig implements Serializable {
    private String inputTopic;
    private String reconciliationTopic;
    private String gapTopic;
    private KafkaConfig kafkaConfig;

    public String getInputTopic() {
        return inputTopic;
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    public String getReconciliationTopic() {
        return reconciliationTopic;
    }

    public void setReconciliationTopic(String reconciliationTopic) {
        this.reconciliationTopic = reconciliationTopic;
    }

    public String getGapTopic() {
        return gapTopic;
    }

    public void setGapTopic(String gapTopic) {
        this.gapTopic = gapTopic;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public SubscriberConfig build() {
        return new SubscriberConfig(inputTopic, reconciliationTopic, gapTopic, kafkaConfig.getProducerConfig());
    }
}

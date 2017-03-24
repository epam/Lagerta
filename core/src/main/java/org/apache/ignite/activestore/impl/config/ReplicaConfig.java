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

package org.apache.ignite.activestore.impl.config;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/28/2016 11:48 AM
 */
public class ReplicaConfig implements Serializable {
    private String address;
    private String remoteTopic;
    private String reconciliationTopic;
    private Properties consumerConfig;
    private Properties producerConfig;

    public ReplicaConfig() {
    }

    public ReplicaConfig(
        String address,
        String remoteTopic,
        String reconciliationTopic,
        Properties consumerConfig,
        Properties producerConfig
    ) {
        this.address = address;
        this.remoteTopic = remoteTopic;
        this.reconciliationTopic = reconciliationTopic;
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRemoteTopic() {
        return remoteTopic;
    }

    public void setRemoteTopic(String remoteTopic) {
        this.remoteTopic = remoteTopic;
    }

    public String getReconciliationTopic() {
        return reconciliationTopic;
    }

    public void setReconciliationTopic(String reconciliationTopic) {
        this.reconciliationTopic = reconciliationTopic;
    }

    public Properties getConsumerConfig() {
        return consumerConfig;
    }

    public void setConsumerConfig(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public Properties getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(Properties producerConfig) {
        this.producerConfig = producerConfig;
    }
}

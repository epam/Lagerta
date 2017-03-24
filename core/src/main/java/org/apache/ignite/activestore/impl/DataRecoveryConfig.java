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

package org.apache.ignite.activestore.impl;

import java.io.Serializable;
import java.util.Properties;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/26/2016 6:43 PM
 */
public class DataRecoveryConfig implements Serializable {
    private String remoteTopic;
    private String localTopic;
    private String reconciliationTopic;
    private Properties consumerConfig;
    private Properties producerConfig;

    public String getRemoteTopic() {
        return remoteTopic;
    }

    public void setRemoteTopic(String remoteTopic) {
        this.remoteTopic = remoteTopic;
    }

    public String getLocalTopic() {
        return localTopic;
    }

    public void setLocalTopic(String localTopic) {
        this.localTopic = localTopic;
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

    public ReplicaConfig getReplicaConfig(String address) {
        return new ReplicaConfig(address, remoteTopic, reconciliationTopic, consumerConfig, producerConfig);
    }
}

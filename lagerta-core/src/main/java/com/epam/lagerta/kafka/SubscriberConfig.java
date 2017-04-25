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

import java.io.Serializable;
import java.util.Properties;

@Deprecated
public class SubscriberConfig implements Serializable {
    private String subscriberId;
    private boolean suspendAllowed;
    private String inputTopic;
    private Properties consumerConfig;
    private Properties producerConfig;

    public String getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(String subscriberId) {
        this.subscriberId = subscriberId;
    }

    public boolean isSuspendAllowed() {
        return suspendAllowed;
    }

    public void setSuspendAllowed(boolean suspendAllowed) {
        this.suspendAllowed = suspendAllowed;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
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

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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.activestore.impl.util.PropertiesUtil;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * @author Aleksandr_Meterko
 * @since 11/29/2016
 */
public class ConsumerDeployService implements Service {

    @Inject
    @Qualifier(DRDataCapturerBusConfiguration.DATA_RECOVERY_CONFIG)
    private Properties dataRecoveryConfig;

    @Inject
    private transient Serializer serializer;

    @IgniteInstanceResource
    private transient Ignite ignite;

    // TODO serialization?
    private transient Collection<SubscriberConsumer> consumers = new ArrayList<>();

    @Override public void cancel(ServiceContext ctx) {
        for (SubscriberConsumer consumer: consumers) {
            consumer.stopPolling();
        }
    }

    @Override public void init(ServiceContext ctx) throws Exception {
        /* no-op */
    }

    @Override public void execute(ServiceContext ctx) throws Exception {
        if (ignite != null && Injection.isActive(ignite)) {
            Injection.inject(this, ignite);
            int numberOfConsumers = Integer.valueOf(dataRecoveryConfig.getProperty(
                DRDataCapturerBusConfiguration.NUMBER_OF_CONSUMERS));
            String topic = dataRecoveryConfig.getProperty(DataCapturerBusConfiguration.DATA_TOPIC);
            Properties kafkaProperties = PropertiesUtil.excludeProperties(dataRecoveryConfig,
                DRDataCapturerBusConfiguration.NUMBER_OF_CONSUMERS, DataCapturerBusConfiguration.DATA_TOPIC);
            // TODO in case of failover wouldn't this be called twice?
            for (int i = 0; i < numberOfConsumers; i++) {
                SubscriberConsumer consumer = new SubscriberConsumer(serializer, kafkaProperties, topic, ignite);
                consumer.startPolling();
                consumers.add(consumer);
            }
        }
    }
}

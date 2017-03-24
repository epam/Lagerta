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

import java.util.AbstractMap;
import java.util.Properties;

import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;

/**
 * @author Aleksandr_Meterko
 * @since 11/29/2016
 */
public class DRDataCapturerBusConfiguration extends DataCapturerBusConfiguration {
    public static final String NUMBER_OF_CONSUMERS = "data.capturer.dr.consumers";

    static final String DATA_RECOVERY_CONFIG = "subscriberConsumerProperties";

    private Properties dataRecoveryConfig;

    public void setDataRecoveryConfig(Properties dataRecoveryConfig) {
        this.dataRecoveryConfig = dataRecoveryConfig;
    }

    @Override public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        namedFactories.put(
            new AbstractMap.SimpleImmutableEntry<Class, String>(Properties.class, DATA_RECOVERY_CONFIG),
            FactoryBuilder.factoryOf(dataRecoveryConfig)
        );
    }

    public ConsumerDeployService replicationDeployService() {
        return new ConsumerDeployService();
    }
}

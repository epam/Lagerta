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

/**
 * @author Evgeniy_Ignatiev
 * @since 12/30/2016 12:20 PM
 */
public class DefaultConsumerPingCheckStrategy implements ConsumerPingCheckStrategy {
    private static final long DEFAULT_CONSUMER_TIMEOUT = 120000;

    private final long consumerTimeout;

    public DefaultConsumerPingCheckStrategy() {
        this(DEFAULT_CONSUMER_TIMEOUT);
    }

    protected DefaultConsumerPingCheckStrategy(long consumerTimeout) {
        this.consumerTimeout = consumerTimeout;
    }

    @Override public boolean isOutOfOrder(long checkTime, long lastPingTime) {
        return checkTime > lastPingTime + consumerTimeout;
    }
}

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

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.ignite.activestore.commons.UUIDFormat.f;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/14/2016 1:15 PM
 */
class ConsumerPingManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPingManager.class);

    private final Set<UUID> consumerOutOfOrder = new HashSet<>();
    private final Set<UUID> staleConsumers = new HashSet<>();
    private final MutableObjectLongMap<UUID> pingTimes = new ObjectLongHashMap<>();

    private final ConsumerPingCheckStrategy strategy;
    private final Map<UUID, LeadResponse> availableWorkBuffer;
    private final LeadPlanner planner;

    public ConsumerPingManager(ConsumerPingCheckStrategy strategy, Map<UUID, LeadResponse> availableWorkBuffer,
        LeadPlanner planner) {
        this.strategy = strategy;
        this.availableWorkBuffer = availableWorkBuffer;
        this.planner = planner;
    }

    public void checkPing() {
        long currentTime = System.currentTimeMillis();

        if (!consumerOutOfOrder.isEmpty()) {
            Set<UUID> cleaned = planner.getFinallyDeadConsumers(consumerOutOfOrder);
            LOGGER.info("[L] Consumers {} went to graveyard", cleaned);
            availableWorkBuffer.keySet().removeAll(cleaned);
            consumerOutOfOrder.removeAll(cleaned);
            for (UUID consumerId : cleaned) {
                pingTimes.remove(consumerId);
            }
            staleConsumers.removeAll(cleaned);
        }
        pingTimes.forEachKeyValue((consumerId, pingTime) -> {
            if (strategy.isOutOfOrder(currentTime, pingTime)) {
                if (consumerOutOfOrder.add(consumerId)) {
                    LOGGER.info("[L] Consumer {} is out of order", f(consumerId));
                    staleConsumers.add(consumerId);
                    planner.registerOutOfOrderConsumer(consumerId);
                }
            }
            else if (consumerOutOfOrder.remove(consumerId)) {
                planner.reuniteConsumer(consumerId);
            }
        });
    }

    public boolean areConsumersStalled() {
        return staleConsumers.size() == pingTimes.size();
    }

    public void receivePing(UUID consumerId, long pingTime, boolean pingOnly) {
        pingTimes.put(consumerId, pingTime);
        if (pingOnly) {
            staleConsumers.add(consumerId);
        } else {
            staleConsumers.remove(consumerId);
        }
    }
}

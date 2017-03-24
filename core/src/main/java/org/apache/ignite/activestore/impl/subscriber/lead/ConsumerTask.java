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

import java.util.UUID;

/**
 * @author Andrei_Yakushin
 * @since 12/12/2016 12:25 PM
 */
abstract class ConsumerTask implements Runnable {
    private final UUID consumerId;
    private final ConsumerPingManager pingManager;
    private final long time;

    public ConsumerTask(UUID consumerId, ConsumerPingManager pingManager) {
        this.consumerId = consumerId;
        this.pingManager = pingManager;
        time = System.currentTimeMillis();
    }

    public UUID getConsumerId() {
        return consumerId;
    }

    protected void sendPing(boolean fromEmptyTask) {
        pingManager.receivePing(consumerId, time, fromEmptyTask);
    }
}

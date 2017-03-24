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

import org.apache.ignite.activestore.impl.config.RPCManager;
import org.apache.ignite.activestore.impl.config.RPCService;
import org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/26/2017 6:50 PM
 */
class MainHeartbeatPeriodicTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainHeartbeatPeriodicTask.class);

    private final Lead lead;
    private final RPCManager rpcManager;

    private boolean mainIsOutOfOrder = false;

    public MainHeartbeatPeriodicTask(Lead lead, RPCManager rpcManager) {
        this.lead = lead;
        this.rpcManager = rpcManager;
    }

    @Override public void run() {
        boolean mainCurrentlyIsOutOfOrder;

        try {
            RPCService rpc = rpcManager.main();
            if (rpc != null) {
                int ping = rpc.get(ActiveCacheStoreService.class, ActiveCacheStoreService.NAME).ping();
                mainCurrentlyIsOutOfOrder = ping > 0;
            }
            else {
                mainCurrentlyIsOutOfOrder = true;
            }
        }
        catch (Exception e) {
            LOGGER.warn("[L] Ping the Main returned error: ", e);
            mainCurrentlyIsOutOfOrder = true;
        }
        if (mainIsOutOfOrder != mainCurrentlyIsOutOfOrder) {
            mainIsOutOfOrder = mainCurrentlyIsOutOfOrder;
            if (!mainCurrentlyIsOutOfOrder) {
                lead.notifyMainIsOkNow();
            }
            else {
                lead.notifyMainIsOutOfOrder();
            }
        }
    }
}

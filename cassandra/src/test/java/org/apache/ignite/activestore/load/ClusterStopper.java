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

package org.apache.ignite.activestore.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * @author Evgeniy_Ignatiev
 * @since 10/10/2016 6:34 PM
 */
public class ClusterStopper {
    private static final int GRACEFUL_CLIENTS_SHUTDOWN_TIMEOUT = 60000;

    public static void main(String[] args) {
        try (Ignite ignite = TestsHelper.getClusterClient()) {
            stopCluster(ignite);
        }
    }

    public static void stopCluster(Ignite ignite) {
        sendStopSignalToLoadGeneratingClients(ignite);
        ignite.cluster().stopNodes();
    }

    private static void sendStopSignalToLoadGeneratingClients(Ignite ignite) {
        ignite.compute(ignite.cluster().forClients()).broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                ignite.cluster().nodeLocalMap().put(LoadTestDriver.STOPPED, true);
            }
        });
        try {
            Thread.sleep(GRACEFUL_CLIENTS_SHUTDOWN_TIMEOUT);
        } catch (InterruptedException e) {
            // Do nothing.
        }
    }
}

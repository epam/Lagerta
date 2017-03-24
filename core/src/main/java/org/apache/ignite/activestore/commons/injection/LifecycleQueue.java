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

package org.apache.ignite.activestore.commons.injection;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * @author Aleksandr_Meterko
 * @since 1/12/2017
 */
class LifecycleQueue {

    private final Queue<LifecycleAware> startedBeans = new ConcurrentLinkedQueue<>();
    private final Queue<LifecycleAware> unprocessedBeans = new ConcurrentLinkedQueue<>();

    private volatile boolean active;

    public void start() {
        active = true;
        startBeans();
    }

    public void stop() {
        for (LifecycleAware lifecycleAware : startedBeans) {
            lifecycleAware.stop();
        }
        active = false;
    }

    public void add(LifecycleAware lifecycleAware) {
        unprocessedBeans.add(lifecycleAware);
        startBeans();
    }

    private void startBeans() {
        if (!active) {
            return;
        }
        while (!unprocessedBeans.isEmpty()) {
            LifecycleAware lifecycleAware = unprocessedBeans.remove();
            lifecycleAware.start();
            startedBeans.add(lifecycleAware);
        }
    }

}

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

package org.apache.ignite.load.statistics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/26/2017 3:20 PM
 */
public class ExecutorManager implements LifecycleAware {
    private static final int KEEP_ALIVE_THREADS = 2;
    private static final long SLEEP_TIME = 100;
    private static final long SHUTDOWN_TIMEOUT = 10_000;

    private final ScheduledExecutorService executor;

    public ExecutorManager() {
        executor = Executors.newScheduledThreadPool(KEEP_ALIVE_THREADS);
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        executor.shutdown();
        try {
            long shutdownStart = System.currentTimeMillis();

            while (System.currentTimeMillis() - shutdownStart < SHUTDOWN_TIMEOUT) {
                executor.awaitTermination(SLEEP_TIME, TimeUnit.MILLISECONDS);
            }
        }
        catch (InterruptedException e) {
            // Stop awaiting.
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }
}

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

package org.apache.ignite.load.subscriber;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * @author Evgeniy_Ignatiev
 * @since 20:51 01/19/2017
 */
class WorkersScheduler implements LifecycleAware {
    private final List<LeadLoadWorker> workers;
    private final ExecutorService executor;

    @Inject
    public WorkersScheduler(@Named(BaseLeadLoadTest.WORKERS_BIND_NAME) List<LeadLoadWorker> workers) {
        this.workers = workers;
        executor = Executors.newFixedThreadPool(workers.size());
    }

    @Override public void start() {
        // Do nothing.
    }

    @Override public void stop() {
        if (!executor.isTerminated()) {
            executor.shutdownNow();
        }
    }

    public void awaitShutdown(long timeout, TimeUnit timeUnit) throws InterruptedException {
        executor.awaitTermination(timeout, timeUnit);
    }

    public void scheduleWorkers() {
        for (LeadLoadWorker worker : workers) {
            executor.submit(worker);
        }
        executor.shutdown();
    }
}

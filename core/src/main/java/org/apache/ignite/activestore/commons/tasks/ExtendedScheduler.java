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

package org.apache.ignite.activestore.commons.tasks;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jsr166.LongAdder8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrei_Yakushin
 * @since 2/8/2017 10:47 AM
 */
public class ExtendedScheduler extends Scheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtendedScheduler.class);

    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    private final LongAdder8 queueSize = new LongAdder8();

    protected volatile boolean running = false;

    public void execute() {
        running = true;
        onStart();
        while (running) {
            try {
                runQueue();
                runTasks();
            }
            catch (Exception e) {
                LOGGER.error("[G] Exception on schedule", e);
            }
        }
    }

    public void cancel() {
        running = false;
    }

    protected void onStart() {

    }

    public void enqueueTask(Runnable task) {
        taskQueue.add(task);
        queueSize.increment();
    }

    private void runQueue() throws InterruptedException {
        if (!taskQueue.isEmpty()) {
            long size = queueSize.sum();
            for (long i = 0; i < size; i++) {
                Runnable task = taskQueue.poll();
                if (task == null) {
                    break;
                }
                task.run();
            }
            queueSize.add(-size);
        }
    }
}

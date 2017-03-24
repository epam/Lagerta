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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/26/2017 6:21 PM
 */
public class Scheduler {
    private final List<Task> tasks = new ArrayList<>();

    public void schedulePeriodicTask(Runnable task, long period) {
        tasks.add(new PeriodicTask(task, period));
    }

    public void simpleTask(Runnable runnable) {
        tasks.add(new SimpleTask(runnable));
    }

    public void cycleTask(Runnable runnable, int threshold) {
        tasks.add(new PeriodicCallTask(runnable, threshold));
    }

    public void runTasks() {
        long runTime = System.currentTimeMillis();

        for (Task task : tasks) {
            task.run(runTime);
        }
    }
}

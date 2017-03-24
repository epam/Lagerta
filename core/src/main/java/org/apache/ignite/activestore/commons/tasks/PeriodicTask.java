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

/**
 * @author Evgeniy_Ignatiev
 * @since 1/27/2017 12:40 PM
 */
class PeriodicTask implements Task {
    private final Runnable task;
    private final long period;

    private Long lastRunTime;

    public PeriodicTask(Runnable task, long period) {
        this.task = task;
        this.period = period;
    }

    @Override public void run(long runTime) {
        if (lastRunTime == null) {
            lastRunTime = runTime;
        }
        if (runTime - lastRunTime >= period) {
            task.run();
            lastRunTime = runTime;
        }
    }
}

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
 * @author Andrei_Yakushin
 * @since 2/8/2017 5:16 PM
 */
public class PeriodicCallTask implements Task {
    private final Runnable runnable;
    private final int threshold;

    private int cycle = 0;

    public PeriodicCallTask(Runnable runnable, int threshold) {
        this.runnable = runnable;
        this.threshold = threshold;
    }

    @Override
    public void run(long runTime) {
        ++cycle;
        if (cycle >= threshold) {
            cycle = 0;
            runnable.run();
        }
    }
}

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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/27/2017 2:44 PM
 */
@RunWith(MockitoJUnitRunner.class)
public class SchedulerUnitTest {
    private static final long PERIOD = 500;

    @Mock
    private Runnable task;

    private Scheduler scheduler;

    @Before
    public void setUp() {
        scheduler = new Scheduler();
    }

    @Test
    public void taskNotCalledAsPeriodIsNotPassedYet() throws InterruptedException {
        scheduler.schedulePeriodicTask(task, Long.MAX_VALUE);
        Thread.sleep(PERIOD);
        scheduler.runTasks();

        Mockito.verifyZeroInteractions(task);
    }

    @Test
    public void taskCalledAsPeriodIsPassed() throws InterruptedException {
        scheduler.schedulePeriodicTask(task, 0);
        Thread.sleep(PERIOD);
        scheduler.runTasks();

        Mockito.verify(task).run();
    }
}

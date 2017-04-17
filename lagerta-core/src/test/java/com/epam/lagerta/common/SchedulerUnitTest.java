/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lagerta.common;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerUnitTest {

    @Test
    public void allPushedTasksRan() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger i = new AtomicInteger(0);

        Scheduler scheduler = new Scheduler();

        scheduler.pushTask(() -> {
            i.incrementAndGet();
            latch.countDown();
        });

        new Thread(scheduler::execute).start();
        latch.await();
        scheduler.stop();
        assertEquals(1, i.get());
    }

    @Test
    public void rulesRunIndefinitely() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        Scheduler scheduler = new Scheduler();
        scheduler.registerRule(latch::countDown);
        new Thread(scheduler::execute).start();
        latch.await();
        scheduler.stop();
    }
}

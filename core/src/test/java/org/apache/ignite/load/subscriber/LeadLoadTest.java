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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Named;
import org.junit.Test;

/**
 * @author Evgeniy_Ignatiev
 * @since 14:05 01/19/2017
 */
public class LeadLoadTest extends BaseLeadLoadTest {
    @Inject
    private WorkersScheduler workersScheduler;

    @Inject
    @Named(REQUESTS_PERIOD_BIND_NAME)
    private AtomicLong requestsPeriod;

    @Test
    public void runProfile() throws Exception {
        Timer timer = new Timer();
        long startTime = System.currentTimeMillis();

        workersScheduler.scheduleWorkers();
        timer.schedule(
            new IncreaseRequestsRateTask(requestsPeriod),
            BaseLeadLoadTest.PERIOD_DECREASE_PERIOD,
            BaseLeadLoadTest.PERIOD_DECREASE_PERIOD
        );
        while (System.currentTimeMillis() - startTime < TEST_EXECUTION_TIME) {
            workersScheduler.awaitShutdown(SLEEP_TIME, TimeUnit.MILLISECONDS);
        }
        timer.cancel();
    }

    private static class IncreaseRequestsRateTask extends TimerTask  {
        private final AtomicLong requestsPeriod;

        IncreaseRequestsRateTask(AtomicLong requestsPeriod) {
            this.requestsPeriod = requestsPeriod;
        }

        @Override public void run() {
            long currentPeriod = requestsPeriod.get();

            if (currentPeriod <= BaseLeadLoadTest.MINIMUM_REQUESTS_PERIOD) {
                cancel();
                return;
            }
            long steppedPeriod = currentPeriod - BaseLeadLoadTest.PERIOD_DECREASE_STEP;

            if (steppedPeriod <= BaseLeadLoadTest.MINIMUM_REQUESTS_PERIOD) {
                requestsPeriod.set(BaseLeadLoadTest.MINIMUM_REQUESTS_PERIOD);
            }
            else {
                requestsPeriod.set(steppedPeriod);
            }
        }
    }
}

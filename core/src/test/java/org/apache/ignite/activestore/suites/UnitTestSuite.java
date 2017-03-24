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

package org.apache.ignite.activestore.suites;

import org.apache.ignite.activestore.commons.tasks.SchedulerUnitTest;
import org.apache.ignite.activestore.impl.subscriber.consumer.IgniteCommitterUnitTest;
import org.apache.ignite.activestore.impl.subscriber.consumer.OffsetCalculatorUnitTest;
import org.apache.ignite.activestore.impl.subscriber.consumer.ParallelIgniteCommitterUnitTest;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadContextLoaderUnitTest;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadPlannerUnitTest;
import org.apache.ignite.activestore.impl.subscriber.lead.MergeHelperUnitTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Andrei_Yakushin
 * @since 1/9/2017 2:21 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
    OffsetCalculatorUnitTest.class,
    MergeHelperUnitTest.class,
    IgniteCommitterUnitTest.class,
    ParallelIgniteCommitterUnitTest.class,
    LeadContextLoaderUnitTest.class,
    SchedulerUnitTest.class,

    //NO ACTUAL UNIT TESTS
    LeadPlannerUnitTest.class,
})
public class UnitTestSuite {
}

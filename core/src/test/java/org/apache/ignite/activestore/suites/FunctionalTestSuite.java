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

import org.apache.ignite.activestore.mocked.tests.BufferOverflowFunctionalTest;
import org.apache.ignite.activestore.mocked.tests.DRClusterFunctionalTest;
import org.apache.ignite.activestore.mocked.tests.LeadAndKafkaOutOfOrderFunctionalTest;
import org.apache.ignite.activestore.mocked.tests.LeadAndMainOutOfOrderFunctionalTest;
import org.apache.ignite.activestore.rules.SingleClusterResource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Aleksandr_Meterko
 * @since 1/17/2017
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
    DRClusterFunctionalTest.class,
    LeadAndKafkaOutOfOrderFunctionalTest.class,
    LeadAndMainOutOfOrderFunctionalTest.class,
    BufferOverflowFunctionalTest.class
})
public class FunctionalTestSuite {
    @ClassRule
    public static RuleChain clusterResource = SingleClusterResource.getAllResourcesRule(6 * 60_000);
}

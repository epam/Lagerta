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

import org.apache.ignite.activestore.replication.ConsumerOutOfOrderIntegrationTest;
import org.apache.ignite.activestore.replication.IdSequencerIntegrationTest;
import org.apache.ignite.activestore.replication.LeadAndKafkaOutOfOrderIntegrationTest;
import org.apache.ignite.activestore.replication.LeadOutOfOrderIntegrationTest;
import org.apache.ignite.activestore.replication.SynchronousReplicationIntegrationTest;
import org.apache.ignite.activestore.rules.FullClusterTestResourcesFactory;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        SynchronousReplicationIntegrationTest.class,
        LeadOutOfOrderIntegrationTest.class,
        ConsumerOutOfOrderIntegrationTest.class,
        LeadAndKafkaOutOfOrderIntegrationTest.class,
        // must be the last test as our engine does not support gaps in tx ids
        IdSequencerIntegrationTest.class
})
public class ActiveCacheStoreReplicationIntegrationTestSuite {
    @ClassRule
    public static RuleChain allResourcesRule = FullClusterTestResourcesFactory.getAllResourcesRule(15 * 60_000);
}

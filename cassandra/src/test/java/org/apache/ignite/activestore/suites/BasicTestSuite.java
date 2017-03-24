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

import org.apache.ignite.activestore.complex.ConcurrentOperationsTest;
import org.apache.ignite.activestore.complex.EvictionPolicyTest;
import org.apache.ignite.activestore.complex.ExpiryPolicyTest;
import org.apache.ignite.activestore.complex.TransactionIsolationTest;
import org.apache.ignite.activestore.simple.AtomicPartitionedACSTest;
import org.apache.ignite.activestore.simple.AtomicPartitionedWriteBehindACSTest;
import org.apache.ignite.activestore.simple.AtomicReplicatedACSTest;
import org.apache.ignite.activestore.simple.AtomicReplicatedWriteBehindACSTest;
import org.apache.ignite.activestore.simple.ExportTest;
import org.apache.ignite.activestore.simple.MergeTest;
import org.apache.ignite.activestore.simple.RollbackTest;
import org.apache.ignite.activestore.simple.TransactionalPartitionedACSTest;
import org.apache.ignite.activestore.simple.TransactionalPartitionedWriteBehindACSTest;
import org.apache.ignite.activestore.simple.TransactionalReplicatedACSTest;
import org.apache.ignite.activestore.simple.TransactionalReplicatedWriteBehindACSTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Specifies all tests to be run for a single suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
    TransactionalPartitionedACSTest.class,
    TransactionalReplicatedACSTest.class,
    TransactionalPartitionedWriteBehindACSTest.class,
    TransactionalReplicatedWriteBehindACSTest.class,
    AtomicPartitionedACSTest.class,
    AtomicReplicatedACSTest.class,
    AtomicPartitionedWriteBehindACSTest.class,
    AtomicReplicatedWriteBehindACSTest.class,

    ConcurrentOperationsTest.class,
    ExpiryPolicyTest.class,
    EvictionPolicyTest.class,
    TransactionIsolationTest.class,
    RollbackTest.class,
    MergeTest.class,
    ExportTest.class
})
public abstract class BasicTestSuite {
}

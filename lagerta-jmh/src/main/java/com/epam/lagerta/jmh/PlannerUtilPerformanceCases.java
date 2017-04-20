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

package com.epam.lagerta.jmh;

import com.epam.lagerta.subscriber.lead.Heartbeats;
import com.epam.lagerta.subscriber.lead.ReadTransactions;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.epam.lagerta.jmh.utils.DataUtil.getCommitRange;
import static com.epam.lagerta.jmh.utils.DataUtil.getDependentTransactions;
import static com.epam.lagerta.jmh.utils.DataUtil.getIndependentTransactions;
import static com.epam.lagerta.jmh.utils.DataUtil.getTotalDependentTransactions;
import static com.epam.lagerta.jmh.utils.DataUtil.list;

@State(Scope.Benchmark)
public class PlannerUtilPerformanceCases {

    private static final int COUNT_TX_PER_NODE = 10_000;

    private static final Heartbeats HEARTBEATS = new Heartbeats(0);
    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();

    public static class IndependentTransactionsPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            independentRead(read, 1000);
            List<Long> commits = getCommitRange(0L, 4000L, l -> l % 7 == 0)
                    .collect(Collectors.toList());
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L, 999L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }

    public static class DependentTransactionsPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            dependentRead(read, COUNT_TX_PER_NODE);
            List<Long> commits = getCommitRange(0L, 4000L, l -> l % 7 == 0)
                    .collect(Collectors.toList());
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L, 999L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }

    public static class TotalDependentTransactionsPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            totalDependentRead(read, COUNT_TX_PER_NODE);
            List<Long> commits = getCommitRange(0L, 4000L, l -> l % 7 == 0)
                    .collect(Collectors.toList());
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L, 999L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }

    private static void dependentRead(ReadTransactions read, int idCountByNode) {
        read.addAllOnNode(A, getDependentTransactions(idCountByNode, list(0L, 1L, 5L, 6L, 15L, 18L)));
        read.addAllOnNode(B, getDependentTransactions(idCountByNode, list(2L, 4L, 7L, 13L, 17L)));
        read.addAllOnNode(A, getDependentTransactions(idCountByNode, list(3L, 8L, 9L, 10L, 16L)));
        read.addAllOnNode(B, getDependentTransactions(idCountByNode, list(11L, 12L, 14L, 19L)));
    }

    private static void independentRead(ReadTransactions read, int idCountByNode) {
        read.addAllOnNode(A, getIndependentTransactions(idCountByNode, list(0L, 1L, 5L, 6L, 15L, 18L)));
        read.addAllOnNode(B, getIndependentTransactions(idCountByNode, list(2L, 4L, 7L, 13L, 17L)));
        read.addAllOnNode(A, getIndependentTransactions(idCountByNode, list(3L, 8L, 9L, 10L, 16L)));
        read.addAllOnNode(B, getIndependentTransactions(idCountByNode, list(11L, 12L, 14L, 19L)));
    }

    private static void totalDependentRead(ReadTransactions read, int idCountByNode) {
        read.addAllOnNode(A, getTotalDependentTransactions(idCountByNode, list(0L, 1L, 5L, 6L, 15L, 18L)));
        read.addAllOnNode(B, getTotalDependentTransactions(idCountByNode, list(2L, 4L, 7L, 13L, 17L)));
        read.addAllOnNode(A, getTotalDependentTransactions(idCountByNode, list(3L, 8L, 9L, 10L, 16L)));
        read.addAllOnNode(B, getTotalDependentTransactions(idCountByNode, list(11L, 12L, 14L, 19L)));
    }
}

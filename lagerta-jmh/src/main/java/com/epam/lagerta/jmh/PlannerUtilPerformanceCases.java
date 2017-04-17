package com.epam.lagerta.jmh;

import com.epam.lagerta.subscriber.lead.Heartbeats;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.UUID;

import static com.epam.lagerta.jmh.DataUtil.get1000Transactions;
import static com.epam.lagerta.jmh.DataUtil.get10Transactions;
import static com.epam.lagerta.jmh.DataUtil.getCommitRange;
import static com.epam.lagerta.jmh.DataUtil.list;

@State(Scope.Benchmark)
public class PlannerUtilPerformanceCases {

    private static final Heartbeats HEARTBEATS = new Heartbeats(0);
    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();

    private static final String CACHE1 = "cache1";
    private static final String CACHE2 = "cache2";

    public static class SimpleNonIntersectedPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            read.addAllOnNode(A, get10Transactions(CACHE1, 0L, 1L));
            read.addAllOnNode(B, get10Transactions(CACHE1, 10L, 2L));
            read.addAllOnNode(A, get10Transactions(CACHE2, 20L, 3L));
            read.addAllOnNode(B, get10Transactions(CACHE2, 30L, 4L));
            List<Long> commits = getCommitRange(0L, 40L);
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(22L, 33L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }

    public static class SimpleIntersectedPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            read.addAllOnNode(A, get10Transactions(CACHE1, 0L, 1L));
            read.addAllOnNode(B, get10Transactions(CACHE1, 10L, 1L, 2L));
            read.addAllOnNode(B, get10Transactions(CACHE2, 20L, 2L));
            read.addAllOnNode(A, get10Transactions(CACHE2, 30L, 2L, 3L));
            List<Long> commits = getCommitRange(0L, 40L);
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(22L, 33L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }

    public static class ManyTransactionsNonIntersectedPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            read.addAllOnNode(A, get1000Transactions(CACHE1, 0L, 1L));
            read.addAllOnNode(B, get1000Transactions(CACHE1, 1000L, 2L));
            read.addAllOnNode(A, get1000Transactions(CACHE2, 1000L, 3L));
            read.addAllOnNode(B, get1000Transactions(CACHE2, 1000L, 4L));
            List<Long> commits = getCommitRange(0L, 4000L);
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L, 999L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }

    public static class ManyTransactionsIntersectedPlan extends AbstractPlannerUtilPerformance {
        @Override
        public void setup() {
            read.addAllOnNode(A, get1000Transactions(CACHE1, 0L, 1L));
            read.addAllOnNode(B, get1000Transactions(CACHE1, 1000L, 1L, 2L));
            read.addAllOnNode(B, get1000Transactions(CACHE2, 2000L, 2L));
            read.addAllOnNode(A, get1000Transactions(CACHE2, 3000L, 2L, 3L));
            List<Long> commits = getCommitRange(0L, 4000L);
            committed.addAll(commits);
            committed.compress();
            commits.addAll(list(111L, 222L, 333L, 444L, 555L, 666L, 777L, 888L, 999L));
            inProgress.addAll(commits);
            read.makeReady();
            read.pruneCommitted(committed, HEARTBEATS, lostReaders, inProgress);
        }
    }
}

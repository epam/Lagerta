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

import com.epam.lagerta.subscriber.ConsumerTxScope;
import com.epam.lagerta.subscriber.lead.CommittedTransactions;
import com.epam.lagerta.subscriber.lead.ReadTransactions;
import com.epam.lagerta.subscriber.util.PlannerUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xms256M", "-Xmx256M"})
@Threads(1)
public abstract class AbstractPlannerUtilPerformance {

    protected ReadTransactions read = new ReadTransactions();
    protected CommittedTransactions committed = new CommittedTransactions();
    protected HashSet<Long> inProgress = new HashSet<>();
    protected HashSet<UUID> lostReaders = new HashSet<>();

    @Setup
    public abstract void setup();

    @Benchmark
    public List<ConsumerTxScope> runSample() {
        return PlannerUtil.plan(read, committed, inProgress, lostReaders);
    }
}

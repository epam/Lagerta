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

package org.apache.ignite.activestore.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.BaseReplicationIntegrationTest;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.commons.injection.InjectionForTests;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Aleksandr_Meterko
 * @since 12/14/2016
 */
public class IdSequencerIntegrationTest extends BaseReplicationIntegrationTest {

    private static long startId;

    @Before
    public void initStartId() {
        startId = sequencer().getNextId() + 1;
    }

    private IdSequencer sequencer() {
        return InjectionForTests.get(IdSequencer.class, mainGrid());
    }

    @Test
    public void getIdsInOneThread() {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ids.add(sequencer().getNextId());
        }
        for (int i = 0; i < ids.size(); i++) {
            Assert.assertEquals(startId + i, (long) ids.get(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getIdsFromDifferentLocalIgniteThreads() {
        int numberOfIgniteThreads = 20;
        final int numberOfIdsPerThread = 50;
        List<IgniteFuture<List<Long>>> futures = new ArrayList<>();
        for (int i = 0; i < numberOfIgniteThreads; i++) {
            IgniteFuture future = mainGrid().scheduler().callLocal(new ComputeJobAdapter() {
                @Override public List<Long> execute() throws IgniteException {
                    List<Long> localIds = new ArrayList<>();
                    IdSequencer sequencer = sequencer();
                    for (int j = 0; j < numberOfIdsPerThread; j++) {
                        localIds.add(sequencer.getNextId());
                    }
                    return localIds;
                }
            });
            futures.add(future);
        }
        List<Long> ids = new ArrayList<>();
        for (IgniteFuture<List<Long>> future: futures) {
            ids.addAll(future.get());
        }

        Assert.assertEquals(numberOfIgniteThreads * numberOfIdsPerThread, ids.size());
        Collections.sort(ids);
        for (int i = 0; i < ids.size(); i++) {
            Assert.assertEquals(startId + i, (long) ids.get(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getIdsFromDifferentComputeTasks() {
        int numberOfIgniteThreads = 20;
        final int numberOfIdsPerThread = 50;
        Collection callables = new ArrayList<>();
        for (int i = 0; i < numberOfIgniteThreads; i++) {
            callables.add(new IgniteCallable() {
                @Override public List<Long> call() throws Exception {
                    List<Long> localIds = new ArrayList<>();
                    IdSequencer sequencer = sequencer();
                    for (int j = 0; j < numberOfIdsPerThread; j++) {
                        localIds.add(sequencer.getNextId());
                    }
                    return localIds;
                }
            });
        }
        Collection<List<Long>> result = mainGrid().compute().call(callables);
        List<Long> ids = new ArrayList<>();
        for (List<Long> localResult: result) {
            ids.addAll(localResult);
        }

        Assert.assertEquals(numberOfIgniteThreads * numberOfIdsPerThread, ids.size());
        Collections.sort(ids);
        for (int i = 0; i < ids.size(); i++) {
            Assert.assertEquals(startId + i, (long) ids.get(i));
        }
    }

}

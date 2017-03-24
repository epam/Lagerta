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
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Aleksandr_Meterko
 * @since 12/12/2016
 */
public class SynchronousReplicationIntegrationTest extends BasicSynchronousReplicationIntegrationTest {
    @Test
    public void clustersDoNotShareData() {
        write(false);
        Map<Integer, Integer> mainValues = readValuesFromCache(mainGrid());
        Map<Integer, Integer> drValues = readValuesFromCache(drGrid());
        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertEquals(i, (int)mainValues.get(i));
            Assert.assertNull(drValues.get(i));
        }
    }

    @Test
    public void baseReplicationCase() throws InterruptedException {
        Map<Integer, Integer> drValues = readValuesFromCache(drGrid());
        for (int i = startIndex; i < endIndex; i++) {
            Assert.assertNull(drValues.get(i));
        }

        write(true);
        Thread.sleep(10_000);
        assertCurrentStateOnServers();
    }

    @Test
    public void concurrentWrites() throws InterruptedException {
        int numberOfThreads = 20;
        final int writesPerThread = INCREMENT_STEP / numberOfThreads;
        Collection<IgniteRunnable> runnables = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            final int finalIter = i;
            runnables.add(new IgniteRunnable() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public void run() {
                    int startIdx = startIndex + writesPerThread * finalIter;
                    int endIdx = startIdx + writesPerThread;
                    writeValuesToCache(ignite, true, startIdx, endIdx, 0);
                }
            });
        }
        mainGrid().compute().run(runnables);
        Thread.sleep(10_000);
        assertCurrentStateOnServers();
    }
}

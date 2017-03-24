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

package org.apache.ignite.activestore.mocked.tests;

import org.apache.ignite.activestore.mocked.mocks.KafkaMockFactory;
import org.junit.Assert;
import org.junit.Test;

import static java.lang.Thread.sleep;

/**
 * @author Andrei_Yakushin
 * @since 2/7/2017 9:59 AM
 */
public class BufferOverflowFunctionalTest extends BaseFunctionalTest {
    @Test
    public void testSimpleScenario() throws InterruptedException {
        writeValueToRemoteKafka(1, 1);
        int gapId = getNextTxId();
        writeValueToRemoteKafka(2, 3);
        writeValueToRemoteKafka(2, 4);
        writeValueToRemoteKafka(2, 5);
        writeValueToRemoteKafka(2, 6);
        writeValueToRemoteKafka(2, 7);
        writeValueToRemoteKafka(2, 8);
        writeValueToRemoteKafka(2, 9);
        writeValueToRemoteKafka(2, 10);

        sleep(5_000);

        writeValueToKafka(KafkaMockFactory.RECONCILIATION_ID, gapId, 1, 2);

        sleep(2_000);

        Assert.assertEquals(2, cache.get(1));
        Assert.assertEquals(10, cache.get(2));
    }
}

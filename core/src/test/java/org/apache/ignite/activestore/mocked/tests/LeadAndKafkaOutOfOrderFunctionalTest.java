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

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/16/2017 6:19 PM
 */
public class LeadAndKafkaOutOfOrderFunctionalTest extends CrashFunctionalTest {
    private static final int KEY = 1;
    private static final int VALUE = 2;

    @Test
    public void kafkaResurrectedBeforeLeadRestart() throws InterruptedException {
        failOnKafkaOperations();
        crashLead();

        writeValueToRemoteKafka(KEY, VALUE);
        Thread.sleep(1_000);
        assertCacheEmpty();

        resurrectKafka();
        resurrectLead();

        Thread.sleep(10_000);
        assertUpdatesInCache();
    }

    @Test
    public void kafkaResurrectedAfterLeadReloadingStarted() throws InterruptedException {
        failOnKafkaOperations();
        crashLead();

        writeValueToRemoteKafka(KEY, VALUE);
        Thread.sleep(1_000);
        assertCacheEmpty();

        resurrectLead();
        Thread.sleep(5_000);
        assertCacheEmpty();

        resurrectKafka();
        Thread.sleep(10_000);
        assertUpdatesInCache();
    }

    private void assertCacheEmpty() {
        Assert.assertEquals(0, cache.size());
    }

    private void assertUpdatesInCache() {
        Assert.assertEquals(1, cache.size());
        Assert.assertEquals(VALUE, cache.get(KEY));
    }

}

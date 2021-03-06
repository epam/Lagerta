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
package com.epam.lagerta.common;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class PeriodicRuleUnitTest {

    @Test
    public void ruleRunsAtDesignedPeriodOnce() throws Exception {
        AtomicInteger i = new AtomicInteger(0);
        PeriodicRule periodicRule = new PeriodicRule(i::incrementAndGet, 1000);
        periodicRule.run();
        periodicRule.run();
        periodicRule.run();
        periodicRule.run();
        assertEquals(1, i.get());
    }
}

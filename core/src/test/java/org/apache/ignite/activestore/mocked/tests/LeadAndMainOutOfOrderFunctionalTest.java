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

import org.apache.ignite.activestore.commons.injection.ActiveStoreIgniteCallable;
import org.apache.ignite.activestore.mocked.mocks.MockRPCService;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * @author Aleksandr_Meterko
 * @since 1/17/2017
 */
public class LeadAndMainOutOfOrderFunctionalTest extends CrashFunctionalTest {

    private static final int START_KEY = 0;

    // main fails - lead restarts - main starts
    @Test
    public void stateConsistentWhenLeadRestartsInTheMiddleOfMainFail() throws InterruptedException {
        int key = writeAndRestartLeadInMiddleOfMainFail();
        assertStateOnServer(key);
    }

    // main fails - lead restarts - main starts
    @Test
    public void reconcilliationCalledWhenLeadRestartsInTheMiddleOfMainFail() throws InterruptedException {
        writeAndRestartLeadInMiddleOfMainFail();
        assertTrue(wasReconcilliationCalled());
    }

    // main fails - lead fails - main starts - lead starts
    @Test
    public void mainAndLeadFailsAndRestoresConsequently() throws InterruptedException {
        int key = crashMainThenLead(START_KEY);
        resurrectMainThenLead();
        assertStateOnServer(key);
    }

    // main fails - lead fails - tx gap appears - main starts - lead starts
    @Test
    public void mainAndLeadFailsAndRestoresConsequentlyButGapDetected() throws InterruptedException {
        int key = crashMainThenLead(START_KEY);
        createTxGap(key);
        resurrectMainThenLead();
        assertTrue(wasReconcilliationCalled());
    }

    // lead fails - main fails - main starts - lead starts
    @Test
    public void stateConsistentWhenMainRestartsInTheMiddleOfLeadFail() throws InterruptedException {
        int key = crashLeadThenMain(START_KEY);
        resurrectMainThenLead();
        assertStateOnServer(key);
    }

    // lead fails - main fails - lead starts - main starts
    @Test
    public void leadAndMainFailsAndRestoresConsequently() throws InterruptedException {
        int key = crashLeadThenMain(START_KEY);
        resurrectLeadThenMain();
        assertStateOnServer(key);
    }

    // main fails - lead restarts - main starts
    private int writeAndRestartLeadInMiddleOfMainFail() throws InterruptedException {
        int key = START_KEY;
        key = crashMainThenLead(key);
        resurrectLeadThenMain();
        return key;
    }

    private void resurrectLeadThenMain() throws InterruptedException {
        resurrectLead();
        Thread.sleep(1_000);
        resurrectMain();
        Thread.sleep(10_000);
    }

    private void resurrectMainThenLead() throws InterruptedException {
        resurrectMain();
        Thread.sleep(1_000);
        resurrectLead();
        Thread.sleep(10_000);
    }

    private int crashMainThenLead(int key) throws InterruptedException {
        writeValueToRemoteKafka(key, key);
        Thread.sleep(1_000);
        crashMain();
        Thread.sleep(1_000);
        int key1 = ++key;
        writeValueToRemoteKafka(key1, key1); // main is not responding but kafka still writes value
        Thread.sleep(1_000);
        crashLead();
        Thread.sleep(1_000);
        int key2 = ++key;
        writeValueToRemoteKafka(key2, key2);
        Thread.sleep(1_000);
        return key;
    }

    private int crashLeadThenMain(int key) throws InterruptedException {
        writeValueToRemoteKafka(key, key);
        Thread.sleep(1_000);
        crashLead();
        Thread.sleep(1_000);
        int key1 = ++key;
        writeValueToRemoteKafka(key1, key1);
        Thread.sleep(1_000);
        crashMain();
        Thread.sleep(1_000);
        int key2 = ++key;
        writeValueToRemoteKafka(key2, key2);
        Thread.sleep(1_000);
        return key;
    }

    private int createTxGap(int key) {
        getNextTxId();
        int key1 = ++key;
        writeValueToRemoteKafka(key1, key1);
        return key;
    }

    private boolean wasReconcilliationCalled() {
        Collection<Boolean> clusterResult = ignite.compute().broadcast(new ActiveStoreIgniteCallable<Boolean>() {
            @Inject
            private MockRPCService rpcService;

            @Override protected Boolean callInjected() throws Exception {
                return rpcService.reconcilliationWasCalled();
            }
        });
        boolean calledReconcilliation = false;
        for (Boolean result: clusterResult) {
            calledReconcilliation = calledReconcilliation || result;
        }
        return calledReconcilliation;
    }

    private void assertStateOnServer(int lastKey) {
        for (int i = START_KEY; i < lastKey; i++) {
            Assert.assertEquals(i, cache.get(i));
        }
    }

}

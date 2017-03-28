/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.BaseIntegrationTest;
import com.epam.lathgertha.base.jdbc.common.Person;
import org.testng.annotations.Test;

public class SubscriberIntegrationTest extends BaseIntegrationTest {
    @Test
    public void sequentialTransactions() throws Exception {
        Person firstPerson = new Person(0, "firstName");
        Person secondPerson = new Person(1, "secondName");

        writePersonToCache(1, firstPerson);
        writePersonToCache(1, secondPerson);
        awaitTransactions();

        assertObjectsInDB(entry(1, secondPerson));
    }

    @Test
    public void parallelTransactions() {
        // ToDo
    }
}

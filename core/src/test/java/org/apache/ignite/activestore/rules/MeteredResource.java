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

package org.apache.ignite.activestore.rules;

import java.util.concurrent.ConcurrentHashMap;
import org.junit.rules.ExternalResource;

/**
 * Resource which calls sets up or tears down only one time for a suite.
 */
public abstract class MeteredResource extends ExternalResource {
    /**
     * Number of performed setups per resource name.
     */
    private static final ConcurrentHashMap<String, Integer> setups = new ConcurrentHashMap<>();

    private final String resourceName;

    public MeteredResource(String resourceName) {
        this.resourceName = resourceName;
        setups.putIfAbsent(resourceName, 0);
    }

    /**
     * Set up environment for a suite.
     */
    protected abstract void setUp();

    /**
     * Tear down environment for a suite.
     */
    protected abstract void tearDown();

    /** {@inheritDoc} */
    @Override protected void before() throws Throwable {
        int setup = setups.get(resourceName);
        setups.put(resourceName, setup + 1);
        if (setup == 0) {
            setUp();
        }
    }

    /** {@inheritDoc} */
    @Override protected void after() {
        int newValue = setups.get(resourceName) - 1;
        setups.put(resourceName, newValue);
        if (newValue == 0)
            tearDown();
    }
}

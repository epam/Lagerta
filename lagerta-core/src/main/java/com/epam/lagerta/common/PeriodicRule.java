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

import java.util.function.BooleanSupplier;

public class PeriodicRule extends PredicateRule {

    private static final long INIT_TIME = 0;

    public PeriodicRule(Runnable rule, long periodInMillis) {
        super(rule, getPeriodicPredicate(periodInMillis));
    }

    private static BooleanSupplier getPeriodicPredicate(long periodInMillis) {
        return new BooleanSupplier() {

            private long lastRun = INIT_TIME;

            @Override
            public boolean getAsBoolean() {
                long now = System.currentTimeMillis();
                boolean result = now - lastRun >= periodInMillis;
                if (result) {
                    lastRun = now;
                }
                return result;
            }
        };
    }
}

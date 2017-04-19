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

public class PredicateRule implements Runnable {

    private final Runnable innerRule;
    private final BooleanSupplier predicate;

    public PredicateRule(Runnable rule, BooleanSupplier predicate) {
        this.innerRule = rule;
        this.predicate = predicate;
    }

    @Override
    public void run() {
        if (predicate.getAsBoolean()) {
            innerRule.run();
        }
    }

    public static class Builder {
        private final Scheduler scheduler;
        private final BooleanSupplier predicate;

        public Builder(Scheduler scheduler, BooleanSupplier predicate) {
            this.scheduler = scheduler;
            this.predicate = predicate;
        }

        public void execute(Runnable runnable) {
            scheduler.registerRule(new PredicateRule(runnable, predicate));
        }
    }
}

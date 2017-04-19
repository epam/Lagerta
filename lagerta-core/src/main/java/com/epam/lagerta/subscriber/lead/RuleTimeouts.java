/*
 * Copyright 2017 EPAM Systems.
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

package com.epam.lagerta.subscriber.lead;

public class RuleTimeouts {

    private static final long DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD = 60_000;
    private static final long SAVE_STATE_PERIOD = 1_000;
    private static final long DEFAULT_GAP_CHECK = 120_000;

    private final long hearbeatExpirationThreshold;
    private final long saveStatePeriod;
    private final long gapCheckPeriod;

    public RuleTimeouts() {
        this(DEFAULT_HEARTBEAT_EXPIRATION_THRESHOLD, SAVE_STATE_PERIOD, DEFAULT_GAP_CHECK);
    }

    public RuleTimeouts(long hearbeatExpirationThreshold, long saveStatePeriod, long gapCheckPeriod) {
        this.hearbeatExpirationThreshold = hearbeatExpirationThreshold;
        this.saveStatePeriod = saveStatePeriod;
        this.gapCheckPeriod = gapCheckPeriod;
    }

    public long getHearbeatExpirationThreshold() {
        return hearbeatExpirationThreshold;
    }

    public long getSaveStatePeriod() {
        return saveStatePeriod;
    }

    public long getGapCheckPeriod() {
        return gapCheckPeriod;
    }

}

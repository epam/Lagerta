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

package org.apache.ignite.activestore.impl.subscriber.lead;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/19/2016 3:42 PM
 */
public class DefaultGapDetectionStrategy implements GapDetectionStrategy {
    private static final long DEFAULT_GAP_EXISTENCE_TIME_THRESHOLD = 120000;

    private long checkTime;
    private long gapExistenceTime;
    private long lastCheckedDenseCommitted;
    private final long gapExistenceTimeThreshold;

    public DefaultGapDetectionStrategy() {
        this(DEFAULT_GAP_EXISTENCE_TIME_THRESHOLD);
    }

    public DefaultGapDetectionStrategy(long gapExistenceTimeThreshold) {
        this.gapExistenceTimeThreshold = gapExistenceTimeThreshold;
    }

    @Override public boolean gapDetected(LeadPlanningState planningState) {
        long previousCheckTime = checkTime;

        checkTime = System.currentTimeMillis();
        if (previousCheckTime == 0) {
            lastCheckedDenseCommitted = planningState.lastDenseCommitted();
            return false;
        }
        boolean gapMayExist = lastCheckedDenseCommitted >= -1 && !planningState.txInfos().isEmpty()
            && planningState.lastDenseRead() == lastCheckedDenseCommitted;

        if (lastCheckedDenseCommitted == planningState.lastDenseCommitted()) {
            if (gapMayExist) {
                gapExistenceTime += checkTime - previousCheckTime;
            }
            else {
                gapExistenceTime = 0;
            }
        }
        else {
            gapExistenceTime = 0;
            lastCheckedDenseCommitted = planningState.lastDenseCommitted();
        }
        return gapExistenceTime > gapExistenceTimeThreshold;
    }
}

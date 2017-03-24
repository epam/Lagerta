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

import org.apache.ignite.activestore.commons.Reference;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/26/2017 6:33 PM
 */
class DenseCommittedIdSavePeriodicTask implements Runnable {
    private final LeadPlanningState planningState;
    private final Reference<Long> storedLastDenseCommitted;

    public DenseCommittedIdSavePeriodicTask(LeadPlanningState planningState, Reference<Long> storedLastDenseCommitted) {
        this.planningState = planningState;
        this.storedLastDenseCommitted = storedLastDenseCommitted;
    }

    @Override public void run() {
        storedLastDenseCommitted.set(planningState.lastDenseCommitted());
    }
}

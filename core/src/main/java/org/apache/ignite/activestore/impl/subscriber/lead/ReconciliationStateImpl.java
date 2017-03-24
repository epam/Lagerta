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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.util.AtomicsHelper;

/**
 * @author Andrei_Yakushin
 * @since 2/9/2017 2:11 PM
 */
@Singleton
public class ReconciliationStateImpl implements ReconciliationState {
    private static final String GAP_UPPER_BOUND = "GAP_UPPER_BOUND";
    private static final String RECONCILIATION_IS_GOING = "RECONCILIATION_IS_GOING";

    private final Reference<Long> gapUpperBound;
    private final Reference<Boolean> reconciliationIsGoing;

    @Inject
    public ReconciliationStateImpl(Ignite ignite) {
        gapUpperBound = AtomicsHelper.getReference(ignite, GAP_UPPER_BOUND, false);
        reconciliationIsGoing = AtomicsHelper.getReference(ignite, RECONCILIATION_IS_GOING, false);
    }

    @Override
    public Reference<Long> gapUpperBound() {
        return gapUpperBound;
    }

    @Override
    public Reference<Boolean> reconciliationIsGoing() {
        return reconciliationIsGoing;
    }
}

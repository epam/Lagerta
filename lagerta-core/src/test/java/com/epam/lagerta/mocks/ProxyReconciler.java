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

package com.epam.lagerta.mocks;

import com.epam.lagerta.subscriber.lead.Reconciler;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ProxyReconciler implements Reconciler {

    private final Reconciler delegate;
    private final Collection<Long> registeredGaps = new ConcurrentLinkedQueue<>();

    public ProxyReconciler() {
        this(null);
    }

    public ProxyReconciler(Reconciler delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isReconciliationGoing() {
        return delegate != null && delegate.isReconciliationGoing();
    }

    @Override
    public void startReconciliation(List<Long> gaps) {
        registeredGaps.addAll(gaps);
        if (delegate != null) {
            delegate.startReconciliation(gaps);
        }
    }

    public boolean wasReconciliationCalled(long txId) {
        return registeredGaps.contains(txId);
    }

}

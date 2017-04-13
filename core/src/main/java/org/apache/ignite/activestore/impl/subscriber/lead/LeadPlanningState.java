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
import org.apache.ignite.activestore.commons.Reference;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/19/2016 2:49 PM
 */
public class LeadPlanningState {
    static final long NOT_INITIALIZED = -2;

    private final LinkedList<TxInfo> txInfos = new LinkedList<>();
    private final MutableLongSet inProgressTransactions = new LongHashSet();
    private final MutableLongList sparseCommitted = new LongArrayList();
    private final MutableLongSet orphanTransactions = new LongHashSet();
    private final List<TxInfo> toRemove = new ArrayList<>();

    private long lastDenseRead = NOT_INITIALIZED;
    private long lastDenseCommitted = NOT_INITIALIZED;

    private final Reference<Long> gapUpperBound;
    private final Reference<Boolean> reconciliationIsGoing;

    @Inject
    public LeadPlanningState(ReconciliationState reconciliationState) {
        this(reconciliationState.gapUpperBound(), reconciliationState.reconciliationIsGoing());
    }

    public LeadPlanningState(Reference<Long> gapUpperBound, Reference<Boolean> reconciliationIsGoing) {
        this.gapUpperBound = gapUpperBound;
        this.reconciliationIsGoing = reconciliationIsGoing;
        gapUpperBound.initIfAbsent(NOT_INITIALIZED);
        reconciliationIsGoing.initIfAbsent(false);
    }

    LinkedList<TxInfo> txInfos() {
        return txInfos;
    }

    ListIterator<TxInfo> txInfoListIterator() {
        return txInfos.listIterator();
    }

    MutableLongSet inProgressTransactions() {
        return inProgressTransactions;
    }

    MutableLongList sparseCommitted() {
        return sparseCommitted;
    }

    MutableLongSet orphanTransactions() {
        return orphanTransactions;
    }

    long lastDenseRead() {
        return lastDenseRead;
    }

    void setLastDenseRead(long lastDenseRead) {
        this.lastDenseRead = lastDenseRead;
    }

    long lastDenseCommitted() {
        return lastDenseCommitted;
    }

    void setLastDenseCommitted(long lastDenseCommitted) {
        this.lastDenseCommitted = lastDenseCommitted;
    }

    boolean isReconciliationGoing() {
        return reconciliationIsGoing.get();
    }

    void notifyReconcilliationStarted(long gapUpperBound) {
        reconciliationIsGoing.set(true);
        this.gapUpperBound.set(gapUpperBound);
    }

    void notifyReconcilliationStopped() {
        reconciliationIsGoing.set(false);
        gapUpperBound.set(NOT_INITIALIZED);
    }

    public long getLastReadTxId() {
        return txInfos.getLast().id();
    }

    long gapUpperBound() {
        return gapUpperBound.get();
    }

    List<TxInfo> toRemove() {
        return toRemove;
    }
}

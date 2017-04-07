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
package com.epam.lathgertha.subscriber.lead;

import com.epam.lathgertha.subscriber.util.MergeUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CommittedTransactions implements Serializable {

    static final long INITIAL_READY_COMMIT_ID = -1L;
    private static final long INITIAL_COMMIT_ID = -2L;
    private static final int INITIAL_CAPACITY = 100;

    private final List<Long> sparseCommitted = new LinkedList<>();
    private volatile long lastDenseCommit;

    private transient List<List<Long>> toMerge = new ArrayList<>(INITIAL_CAPACITY);

    public CommittedTransactions() {
        this.lastDenseCommit = INITIAL_COMMIT_ID;
    }

    public boolean addAll(List<Long> sortedTransactions) {
        return toMerge.add(sortedTransactions);
    }

    public boolean contains(long l) {
        return l <= lastDenseCommit || sparseCommitted.contains(l);
    }

    public long getLastDenseCommit() {
        return lastDenseCommit;
    }

    public void compress() {
        mergeCollections();
        Iterator<Long> iterator = sparseCommitted.iterator();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            if (lastDenseCommit + 1 == next) {
                iterator.remove();
                lastDenseCommit = next;
            } else {
                break;
            }
        }
    }

    public void updateCommitted(CommittedTransactions newCommitted) {
        addAll(newCommitted.sparseCommitted);
        lastDenseCommit = newCommitted.lastDenseCommit;
        compress();
    }

    public void setReady() {
        if (lastDenseCommit == INITIAL_COMMIT_ID) {
            lastDenseCommit = INITIAL_READY_COMMIT_ID;
        }
    }

    private void mergeCollections() {
        MergeUtil.mergeCollections(sparseCommitted, toMerge, Long::compare);
        toMerge = new ArrayList<>(INITIAL_CAPACITY);
    }
}

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

package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.subscriber.util.MergeUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CommittedOffset {

    static final long INITIAL_COMMIT_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;

    private final List<Long> read = new LinkedList<>();
    private final List<Long> committed = new LinkedList<>();
    private long lastDenseCommit = INITIAL_COMMIT_ID;
    private List<Long> toMerge = new ArrayList<>(INITIAL_CAPACITY);

    public void notifyRead(Long offset) {
        read.add(offset);
    }

    public void notifyCommit(Long offset) {
        toMerge.add(offset);
    }

    public long getLastDenseCommit() {
        return lastDenseCommit;
    }

    public void compress() {
        mergeCollections();
        Iterator<Long> iteratorRead = read.iterator();
        Iterator<Long> iteratorCommitted = committed.iterator();
        while (iteratorRead.hasNext() && iteratorCommitted.hasNext()) {
            Long nextRead = iteratorRead.next();
            Long nextCommitted = iteratorCommitted.next();
            if (nextRead < nextCommitted) {
                break;
            } else {
                iteratorRead.remove();
                iteratorCommitted.remove();
                lastDenseCommit = nextRead;
            }
        }
    }

    private void mergeCollections() {
        if (toMerge.isEmpty()) {
            return;
        }
        Collections.sort(toMerge);
        MergeUtil.merge(committed, toMerge, Long::compareTo);
        toMerge = new ArrayList<>(INITIAL_CAPACITY);
    }
}

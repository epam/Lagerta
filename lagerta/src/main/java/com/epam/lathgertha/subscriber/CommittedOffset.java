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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;


public class CommittedOffset {

    private static final long INITIAL_COMMIT_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;

    private List<Long> read = new LinkedList<>();

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
        Iterator<Long> iterator = read.iterator();
        Iterator<Long> iteratorCommitted = committed.iterator();
        while (iterator.hasNext() && iteratorCommitted.hasNext()) {
            Long next = iterator.next();
            Long nextCommitted = iteratorCommitted.next();
            if (next < nextCommitted) {
                break;
            } else {
                iterator.remove();
                iteratorCommitted.remove();
                lastDenseCommit = next;
            }
        }
    }

    private void mergeCollections() {
        if (toMerge.isEmpty()) {
            return;
        }
        Collections.sort(toMerge);
        merge(committed, toMerge);
        toMerge = new ArrayList<>(INITIAL_CAPACITY);
    }

    /**
     * merge two collections into first one
     */
    static void merge(List<Long> first, List<Long> second) {
        ListIterator<Long> firstIter = first.listIterator();
        ListIterator<Long> secondIter = second.listIterator();

        Long a = getNext(firstIter);
        Long b = getNext(secondIter);

        while (a != null && b != null) {
            if (a.compareTo(b) > 0) {
                firstIter.previous();
                firstIter.add(b);
                firstIter.next();
                b = getNext(secondIter);
            } else {
                a = getNext(firstIter);
            }
        }

        while (b != null) {
            firstIter.add(b);
            b = getNext(secondIter);
        }
    }

    private static Long getNext(ListIterator<Long> iter) {
        return iter.hasNext() ? iter.next() : null;
    }

}

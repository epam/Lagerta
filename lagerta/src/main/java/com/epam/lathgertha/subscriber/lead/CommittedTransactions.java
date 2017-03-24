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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class CommittedTransactions {

    private static final long INITIAL_COMMIT_ID = -1L;
    private static final int INITIAL_CAPACITY = 100;
    private static final double LOG2 = Math.log(2);

    private final List<Long> committed = new LinkedList<>();
    private long lastDenseCommit = INITIAL_COMMIT_ID;
    private List<List<Long>> toMerge = new ArrayList<>(INITIAL_CAPACITY);

    private final ForkJoinPool pool = new ForkJoinPool();

    public boolean addAll(List<Long> sortedTransactions) {
        return toMerge.add(sortedTransactions);
    }

    public boolean contains(long l) {
        return l <= lastDenseCommit || committed.contains(l);
    }

    public long getLastDenseCommit() {
        return lastDenseCommit;
    }

    public void compress() {
        mergeCollections();
        Iterator<Long> iterator = committed.iterator();
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

    private void mergeCollections() {
        if (toMerge.isEmpty()) {
            return;
        }
        int size = toMerge.size();
        double deep = Math.ceil((Math.log(size) / LOG2));
        int prevStep = 1;
        for (int i = 0; i < deep; i++) {
            int step = prevStep * 2;
            List<ForkJoinTask> tasks = new ArrayList<>();
            for (int j = 0; j < size - 1; j += step) {
                final int current = j;
                final int next = j + prevStep;
                Runnable task = () -> merge(toMerge.get(current), toMerge.get(next));
                tasks.add(pool.submit(task));
            }
            tasks.forEach(ForkJoinTask::join);
            prevStep = step;
        }
        merge(committed, toMerge.get(0));
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
            if (a > b) {
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

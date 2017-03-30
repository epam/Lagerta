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

package com.epam.lathgertha.subscriber.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public final class MergeUtil {

    private static final double LOG2 = Math.log(2);
    private static final ForkJoinPool pool = ForkJoinPool.commonPool();

    /**
     * squashes and merges buffer into container
     *
     * @param container  result list
     * @param buffer     items to merge
     * @param comparator
     */
    public static <T> void mergeCollections(
            List<T> container,
            List<List<T>> buffer,
            Comparator<T> comparator) {
        if (buffer.isEmpty()) {
            return;
        }
        int size = buffer.size();
        double deep = Math.ceil((Math.log(size) / LOG2));
        int prevStep = 1;
        for (int i = 0; i < deep; i++) {
            int step = prevStep * 2;
            List<ForkJoinTask> tasks = new ArrayList<>();
            for (int j = 0; j < size - 1; j += step) {
                final int current = j;
                final int next = j + prevStep;
                Runnable task = () -> merge(buffer.get(current), buffer.get(next), comparator);
                tasks.add(pool.submit(task));
            }
            tasks.forEach(ForkJoinTask::join);
            prevStep = step;
        }
        merge(container, buffer.get(0), comparator);
    }

    /**
     * merges two collections
     *
     * @param first      result list
     * @param second     merges with first list
     * @param comparator
     */
    public static <T> void merge(List<T> first, List<T> second, Comparator<T> comparator) {
        ListIterator<T> firstIter = first.listIterator();
        ListIterator<T> secondIter = second.listIterator();

        T a = getNext(firstIter);
        T b = getNext(secondIter);

        while (a != null && b != null) {
            if (comparator.compare(a, b) > 0) {
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

    private static <T> T getNext(ListIterator<T> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}

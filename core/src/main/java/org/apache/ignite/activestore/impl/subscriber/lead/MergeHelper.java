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

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;

/**
 * @author Andrei_Yakushin
 * @since 12/9/2016 12:01 PM
 */
public class MergeHelper {
    private static final Comparator<TxInfo> COMPARATOR = new Comparator<TxInfo>() {
        @Override
        public int compare(TxInfo lhs, TxInfo rhs) {
            return Long.compare(lhs.id(), rhs.id());
        }
    };

    public static void mergeTo(List<TxInfo> from, LinkedList<TxInfo> to) {
        if (from.isEmpty()) {
            return;
        }
        Iterator<TxInfo> fromIterator = from.iterator();
        ListIterator<TxInfo> toIterator = to.listIterator();
        TxInfo currentFromElement = null;
        if (toIterator.hasNext()) {
            currentFromElement = fromIterator.next();
            while (currentFromElement != null && toIterator.hasNext()) {
                TxInfo existing = toIterator.next();
                int compare = COMPARATOR.compare(existing, currentFromElement);
                if (compare == 0) {
                    toIterator.remove();
                    toIterator.add(currentFromElement);
                    currentFromElement = fromIterator.hasNext() ? fromIterator.next() : null;
                } else if (compare > 0) {
                    toIterator.previous();
                    toIterator.add(currentFromElement);
                    currentFromElement = fromIterator.hasNext() ? fromIterator.next() : null;
                }
            }
        }
        if (currentFromElement != null) {
            to.add(currentFromElement);
        }
        while (fromIterator.hasNext()) {
            to.add(fromIterator.next());
        }
    }

    public static long mergeWithDenseCompaction(LongList from, MutableLongList to, long dense) {
        int i = 0;
        int j = 0;
        long nextDense = dense + 1;

        while (j < from.size() && from.get(j) < nextDense) {
            j++;
        }
        while (i < to.size() && j < from.size()) {
            long a = to.get(i);
            long b = from.get(j);

            if (a == b) {
                j++;
            } else if (a > b) {
                j++;
                if (b == nextDense) {
                    nextDense++;
                }
                else {
                    to.addAtIndex(i, b);
                    i++;
                }
            } else {
                if (a == nextDense) {
                    nextDense++;
                    to.removeAtIndex(i);
                } else {
                    i++;
                }
            }
        }
        while (i < to.size()) {
            if (to.get(i) == nextDense) {
                nextDense++;
                to.removeAtIndex(i);
            } else {
                break;
            }
        }
        while (j < from.size()) {
            long b = from.get(j);
            if (b == nextDense) {
                nextDense++;
            } else {
                to.add(b);
            }
            j++;
        }
        return nextDense - 1;
    }
}

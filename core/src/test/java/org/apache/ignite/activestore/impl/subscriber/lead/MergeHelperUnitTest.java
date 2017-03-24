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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import gnu.trove.list.array.TLongArrayList;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.ignite.activestore.impl.transactions.TransactionMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Andrei_Yakushin
 * @since 12/9/2016 12:02 PM
 */
@RunWith(JUnitParamsRunner.class)
public class MergeHelperUnitTest {
    @Test
    public void testMergeTo() {
        LinkedList<TxInfo> to = new LinkedList<>(Arrays.asList(
                info(1, true),
                info(2, false),
                info(5, false),
                info(15, true),
                info(19, false)
        ));
        List<TxInfo> from = new ArrayList<>(Arrays.asList(
                info(2, true),
                info(4, false),
                info(6, true),
                info(15, false)
        ));

        MergeHelper.mergeTo(from, to);
        Assert.assertEquals(7, to.size());
        Assert.assertEquals(1, to.get(0).id());
        Assert.assertEquals(2, to.get(1).id());
        Assert.assertEquals(4, to.get(2).id());
        Assert.assertEquals(5, to.get(3).id());
        Assert.assertEquals(6, to.get(4).id());
        Assert.assertEquals(15, to.get(5).id());
        Assert.assertEquals(19, to.get(6).id());
    }

    @Test
    public void testMergeToAddTail() {
        LinkedList<TxInfo> to = new LinkedList<>(Arrays.asList(
                info(1, false),
                info(2, false)
        ));
        List<TxInfo> from = new ArrayList<>(Arrays.asList(
                info(4, false),
                info(6, false),
                info(15, false)
        ));

        MergeHelper.mergeTo(from, to);
        Assert.assertEquals(5, to.size());
        Assert.assertEquals(1, to.get(0).id());
        Assert.assertEquals(2, to.get(1).id());
        Assert.assertEquals(4, to.get(2).id());
        Assert.assertEquals(6, to.get(3).id());
        Assert.assertEquals(15, to.get(4).id());
    }

    @Test
    public void testMergeToNothingToAdd() {
        LinkedList<TxInfo> to = new LinkedList<>(Arrays.asList(
                info(1, false),
                info(2, false)
        ));
        List<TxInfo> from = Collections.emptyList();

        MergeHelper.mergeTo(from, to);
        Assert.assertEquals(2, to.size());
        Assert.assertEquals(1, to.get(0).id());
        Assert.assertEquals(2, to.get(1).id());
    }

    @Test
    @Parameters(source = MergeToLongProvider.class)
    public void testMergeToLong(long[] to, long[] from, long dense, long[] expectedResult, long expectedDense) {
        TLongArrayList actualResult = new TLongArrayList(to);
        long result = MergeHelper.mergeWithDenseCompaction(new TLongArrayList(from), actualResult, dense);
        Assert.assertEquals(expectedDense, result);
        Assert.assertEquals(new TLongArrayList(expectedResult), actualResult);
    }

    @SuppressWarnings("unused")
    public static class MergeToLongProvider {
        public static Object[] provideForMergeWithoutDuplicates() {
            return new Object[]{
                new Object[]{new long[] {2, 3, 4}, new long[] {}, 0, new long[] {2, 3, 4}, 0},
                new Object[]{new long[] {1, 2, 4}, new long[] {}, 0, new long[] {4}, 2},
                new Object[]{new long[] {2, 3, 4}, new long[] {5}, 0, new long[] {2, 3, 4, 5}, 0},
                new Object[]{new long[] {2, 3, 5}, new long[] {4}, 0, new long[] {2, 3, 4, 5}, 0},
                new Object[]{new long[] {1, 4}, new long[] {2}, 0, new long[] {4}, 2},
                new Object[]{new long[] {2, 4}, new long[] {1}, 0, new long[] {4}, 2},
                new Object[]{new long[] {2, 3, 4}, new long[] {1}, 0, new long[] {}, 4}
            };
        }

        public static Object[] provideForMergeWithDuplicates() {
            return new Object[]{
                new Object[]{new long[] {2, 3, 4}, new long[] {1, 4, 5, 7}, 0, new long[] {7}, 5},
                new Object[]{new long[] {2, 4, 5}, new long[] {1, 4, 5, 7}, 0, new long[] {4, 5, 7}, 2},
                new Object[]{new long[] {6, 8}, new long[] {4, 5}, 4, new long[] {8}, 6}
            };
        }
    }

    private TxInfo info(int id, boolean reconciliation) {
        return new TxInfo(null, new TransactionMetadata(id, null));
    }
}

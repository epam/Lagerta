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

package com.epam.lagerta.subscriber;

import com.epam.lagerta.capturer.TransactionScope;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ParallelCommitStrategyUnitTest {
    private static final String TEST = "test";

    private static final String CACHE_NAME = "cache";

    private static final List<String> A = singletonList("A");
    private static final List<String> B = singletonList("B");
    private static final List<String> C = singletonList("C");
    private static final List<String> A_B = asList("A", "B");
    private static final List<String> B_C = asList("B", "C");
    private static final List<String> A_C = asList("A", "C");
    private static final List<String> B_D = asList("B", "D");

    @DataProvider(name = TEST)
    public Object[][] dataProviderForCommit() {
        return new Object[][] {
                //normal cases
                {asList(A, A), -1, asList(0, 1), singletonList(pair(0, 1))},
                {asList(A, B, A), -1, asList(0, 1, 2), singletonList(pair(0, 2))},
                {asList(A, B, A, B), -1, asList(0, 1, 2, 3), asList(pair(0, 2), pair(1, 3))},
                {asList(A_B, A_B, A_B), -1, asList(0, 1, 2), asList(pair(0, 1), pair(1, 2))},
                {asList(A_B, A_C, B_D), -1, asList(0, 1, 2), asList(pair(0, 1), pair(0, 2))},
                {asList(A_B, A, B, A_B), -1, asList(0, 1, 2, 3), asList(pair(0, 1), pair(0, 2), pair(1, 3), pair(2, 3))},
                {asList(A_B, B_C, A, C, A_B, B_C), -1, asList(0, 1, 2, 3, 4, 5), asList(pair(0, 1), pair(0, 2), pair(1, 3), pair(1, 4), pair(2, 4), pair(3, 5), pair(4, 5))},

                //dead transactions
                {asList(A_B, B_C, A, C, A_B, B_C), 0, emptyList(), emptyList()},
                {asList(A_B, B_C, A, C, A_B, B_C), 1, asList(0, 2), singletonList(pair(0, 2))},
                {asList(A_B, B_C, A, C, A_B, B_C), 2, asList(0, 1, 3), asList(pair(0, 1), pair(1, 3))},
                {asList(A_B, B_C, A, C, A_B, B_C), 3, asList(0, 1, 2, 4), asList(pair(0, 1), pair(0, 2), pair(2, 4))},
                {asList(A_B, B_C, A, C, A_B, B_C), 4, asList(0, 1, 2, 3), asList(pair(0, 1), pair(0, 2), pair(1, 3))},
                {asList(A_B, B_C, A, C, A_B, B_C), 5, asList(0, 1, 2, 3, 4), asList(pair(0, 1), pair(0, 2), pair(1, 3), pair(1, 4), pair(2, 4))},
        };
    }

    @Test(dataProvider = TEST)
    public void testCommit(
            List<List<Object>> changes,
            long deadTransaction,
            List<Integer> expectedCommitted,
            List<Map.Entry<Integer, Integer>> expectedBefore
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(0);

        List<Long> txIdsToCommit = LongStream.range(0, changes.size()).boxed().collect(toList());
        Map<Long, TransactionData> input = new HashMap<>(changes.size());
        for (int i = 0; i < changes.size(); i++) {

            List<Map.Entry<String, List>> list = singletonList(pair(CACHE_NAME, changes.get(i)));
            TransactionData data = new TransactionData(new TransactionScope((long) i, list), buffer, new TopicPartition("topic", 0), 0L);
            input.put((long) i, data);
        }

        CommitServitor servitor = mock(CommitServitor.class);
        List<Long> actual = Collections.synchronizedList(new ArrayList<>());
        doAnswer(mock -> !mock.getArguments()[0].equals(deadTransaction) && actual.add((Long) mock.getArguments()[0]))
                .when(servitor)
                .commit(anyLong(), anyMap());

        ParallelCommitStrategy strategy = new ParallelCommitStrategy(servitor, "localGridName");
        List<Long> actualCommitted = strategy.commit(txIdsToCommit, input);

        Assert.assertEquals(actualCommitted, expectedCommitted.stream().map(Integer::longValue).collect(toList()));

        checkOrder(expectedBefore, actual);
    }

    private void checkOrder(List<Map.Entry<Integer, Integer>> expectedBefore, List<Long> actual) {
        for (Map.Entry<Integer, Integer> entry : expectedBefore) {
            Iterator<Long> iterator = actual.iterator();
            while (iterator.hasNext() && entry.getKey().longValue() != iterator.next());
            boolean ok = false;
            while (iterator.hasNext()) {
                if (entry.getValue().longValue() == iterator.next()) {
                    ok = true;
                }
            }
            Assert.assertTrue(ok, entry.getKey() + " is not before " + entry.getValue());
        }
    }

    private static <K, V> Map.Entry<K, V> pair(K k, V v) {
        return new AbstractMap.SimpleImmutableEntry<K, V>(k, v);
    }
}

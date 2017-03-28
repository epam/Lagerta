package com.epam.lathgertha.subscriber;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.fail;

import com.epam.lathgertha.capturer.TransactionScope;
import org.jetbrains.annotations.NotNull;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * @author Andrei_Yakushin
 * @since 3/27/2017 3:26 PM
 */
public class ParallelCommitStrategyUnitTest {
    private static final String CACHE_NAME = "cache";

    private static final List<String> A = singletonList("A");
    private static final List<String> B = singletonList("B");
    private static final List<String> C = singletonList("C");
    private static final List<String> A_B = asList("A", "B");
    private static final List<String> B_C = asList("B", "C");
    private static final List<String> A_C = asList("A", "C");
    private static final List<String> B_D = asList("B", "D");

    @DataProvider(name = "test")
    public Object[][] dataProviderForCommit() {
        return new Object[][] {
                new Object[] {asList(A, A), singletonList(pair(0, 1))},
                new Object[] {asList(A, B, A), singletonList(pair(0, 2))},
                new Object[] {asList(A, B, A, B), asList(pair(0, 2), pair(1, 3))},
                new Object[] {asList(A_B, A_B, A_B), asList(pair(0, 1), pair(1, 2))},
                new Object[] {asList(A_B, A_C, B_D), asList(pair(0, 1), pair(0, 2))},
                new Object[] {asList(A_B, A, B, A_B), asList(pair(0, 1), pair(0, 2), pair(1, 3), pair(2, 3))},
                new Object[] {asList(A_B, B_C, A, C, A_B, B_C), asList(pair(0, 1), pair(0, 2), pair(1, 3), pair(1, 4), pair(2, 4), pair(3, 5), pair(4, 5))},
        };
    }

    @Test(dataProvider = "test")
    public void testCommit(List<List<Object>> changes, List<Map.Entry<Integer, Integer>> expectedBefore) {
        ByteBuffer buffer = ByteBuffer.allocate(0);

        List<Long> txIdsToCommit = LongStream.range(0, changes.size()).boxed().collect(Collectors.toList());
        Map<Long, Map.Entry<TransactionScope, ByteBuffer>> input = new HashMap<>(changes.size());
        for (int i = 0; i < changes.size(); i++) {
            List<Map.Entry<String, List>> list = singletonList(pair(CACHE_NAME, changes.get(i)));
            input.put((long) i, pair(new TransactionScope((long) i, list), buffer));
        }

        CommitServitor servitor = mock(CommitServitor.class);
        List<Long> actual = Collections.synchronizedList(new ArrayList<>());
        doAnswer(mock -> actual.add((Long) mock.getArguments()[0]))
                .when(servitor)
                .commit(anyLong(), anyMap());

        ParallelCommitStrategy strategy = new ParallelCommitStrategy(servitor);
        strategy.commit(txIdsToCommit, input);

        while (actual.size() < changes.size()) {
            try {
                sleep(100);
            } catch (InterruptedException e) {
                fail();
            }
        }

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

    @NotNull
    private static <K, V> Map.Entry<K, V> pair(K k, V v) {
        return new AbstractMap.SimpleImmutableEntry<K, V>(k, v);
    }
}

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

package org.apache.ignite.activestore.impl.subscriber.consumer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.impl.transactions.LinearTransactionDataIterator;
import org.apache.ignite.activestore.impl.transactions.LinearTransactionScopeIterator;
import org.apache.ignite.activestore.subscriber.Committer;
import org.apache.ignite.activestore.subscriber.TransactionSupplier;
import org.apache.ignite.activestore.transactions.TransactionDataIterator;
import org.apache.ignite.activestore.transactions.TransactionScopeIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.transactions.Transaction;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Andrei_Yakushin
 * @since 1/9/2017 12:04 PM
 */
@SuppressWarnings("unchecked")
@RunWith(JUnitParamsRunner.class)
public abstract class AbstractIgniteCommitterUnitTest {
    private static final String CACHE_NAME = "cache";

    @Test
    @Parameters(source = TestDataProvider.class)
    @SuppressWarnings("unchecked")
    public void test(
            Map<Long, Map.Entry<List<IgniteBiTuple<String, List>>, List<List>>> input,
            Map<String, Integer> expected
    ) {
        Map<String, Integer> actual = new HashMap<>();
        Ignite ignite = getIgnite(actual);

        final Thread main = Thread.currentThread();
        Committer committer = create(ignite);
        committer.commitAsync(ids(input), supplier(input), mock(IgniteInClosure.class), new IgniteInClosure<LongList>() {
            @Override
            public void apply(LongList tLongList) {
                main.interrupt();
            }
        });
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
        Assert.assertEquals(expected, actual);
        if (committer instanceof LifecycleAware) {
            ((LifecycleAware) committer).stop();
        }
    }

    private Ignite getIgnite(Map<String, Integer> actual) {
        IgniteCache cache = mock(IgniteCache.class);
        doAnswer(new OnPutAnswer(actual)).when(cache).put(any(String.class), any(Integer.class));
        doAnswer(new OnRemoveAnswer(actual)).when(cache).remove(any(String.class));

        IgniteTransactions transactions = mock(IgniteTransactions.class);
        doReturn(mock(Transaction.class)).when(transactions).txStart();

        Ignite ignite = mock(Ignite.class);
        doReturn(cache).when(ignite).cache(CACHE_NAME);
        doReturn(transactions).when(ignite).transactions();
        return ignite;
    }

    protected abstract Committer create(Ignite ignite);

    public static class TestDataProvider {
        @SuppressWarnings("unchecked")
        public static Object[] provideForTest() {
            return new Object[] {
                    new Object[] {pack(tx(pair("A", 1)), tx(pair("A", 2))), toMap(pair("A", 2))},
                    new Object[] {pack(tx(pair("A", 1)), tx(pair("B", 1))), toMap(pair("A", 1), pair("B", 1))},
                    new Object[] {pack(tx(pair("A", 1)), tx(pair("B", 1)), tx(pair("B", ActiveCacheStore.TOMBSTONE))), toMap(pair("A", 1))},
                    new Object[] {pack(tx(pair("A", 1)), tx(pair("B", 1)), tx(pair("A", 2))), toMap(pair("A", 2), pair("B", 1))},
                    new Object[] {pack(tx(pair("A", 1)), tx(pair("B", 1)), tx(pair("A", 2), pair("B", 2))), toMap(pair("A", 2), pair("B", 2))},
                    new Object[] {pack(tx(pair("A", 1), pair("B", 1)), tx(pair("A", 2)), tx(pair("B", 2))), toMap(pair("A", 2), pair("B", 2))},
                    new Object[] {pack(tx(pair("A", 1), pair("B", 1)), tx(pair("A", 2)), tx(pair("B", 2)), tx(pair("A", 3), pair("B", 3))), toMap(pair("A", 3), pair("B", 3))},
                    new Object[] {pack(tx(pair("A", 1), pair("B", 1)), tx(pair("B", 2), pair("C", 2)), tx(pair("A", 2)), tx(pair("C", 3)), tx(pair("A", 3), pair("B", 3)), tx(pair("B", 4), pair("C", 4))), toMap(pair("A", 3), pair("B", 4), pair("C", 4))},
            };
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    private static Map.Entry<String, Object> pair(String key, Object value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    private static Map.Entry<List<IgniteBiTuple<String, List<Object>>>, List<List<Object>>> tx(
        Map.Entry<String, Object>... pairs
    ) {
        List<Object> keys = new ArrayList<>(pairs.length);
        List<Object> values = new ArrayList<>(pairs.length);

        for (Map.Entry<String, ?> pair : pairs) {
            keys.add(pair.getKey());
            values.add(pair.getValue());
        }
        IgniteBiTuple<String, List<Object>> cacheToKeys = new IgniteBiTuple<>(CACHE_NAME, keys);

        return new AbstractMap.SimpleImmutableEntry<>(Collections.singletonList(cacheToKeys),
            Collections.singletonList(values));
    }

    private static Map<String, Object> toMap(Map.Entry<String, Object>... pairs) {
        Map<String, Object> result = new HashMap<>(pairs.length);
        for (Map.Entry<String, Object> pair : pairs) {
            result.put(pair.getKey(), pair.getValue());
        }
        return result;
    }

    private static Map<Long, Map.Entry<List<IgniteBiTuple<String, List<Object>>>, List<List<Object>>>> pack(
        Map.Entry<List<IgniteBiTuple<String, List<Object>>>, List<List<Object>>>... txs
    ) {
        Map<Long, Map.Entry<List<IgniteBiTuple<String, List<Object>>>, List<List<Object>>>> result = new LinkedHashMap<>(txs.length);
        long id = 0;
        for (Map.Entry<List<IgniteBiTuple<String, List<Object>>>, List<List<Object>>> tx : txs) {
            result.put(id++, tx);
        }
        return result;
    }

    private static LongList ids(Map<Long, Map.Entry<List<IgniteBiTuple<String, List>>, List<List>>> map) {
        MutableLongList result = new LongArrayList(map.size());
        for (Long id : map.keySet()) {
            result.add(id);
        }
        return result;
    }

    private static TransactionSupplier supplier(
        final Map<Long, Map.Entry<List<IgniteBiTuple<String, List>>, List<List>>> result
    ) {
        return new TransactionSupplier() {
            @Override public TransactionScopeIterator scopeIterator(long id) {
                return new LinearTransactionScopeIterator(result.get(id).getKey());
            }

            @Override public TransactionDataIterator dataIterator(long id) {
                return new LinearTransactionDataIterator(scopeIterator(id), result.get(id).getValue());
            }
        };
    }

    private static class OnPutAnswer implements Answer {
        private final Map<String, Integer> actual;

        public OnPutAnswer(Map<String, Integer> actual) {
            this.actual = actual;
        }

        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            Object[] arguments = invocationOnMock.getArguments();
            actual.put((String) arguments[0], (Integer) arguments[1]);
            return null;
        }
    }

    private static class OnRemoveAnswer implements Answer {
        private final Map<String, Integer> actual;

        public OnRemoveAnswer(Map<String, Integer> actual) {
            this.actual = actual;
        }

        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            actual.remove(invocationOnMock.getArguments()[0]);
            return null;
        }
    }
}

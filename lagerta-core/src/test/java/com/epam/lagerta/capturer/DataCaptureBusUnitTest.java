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
package com.epam.lagerta.capturer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataCaptureBusUnitTest {

    public static final String CACHE_NAME = "cacheName";

    @Test
    public void testWriteWithoutTransaction() {
        CacheStoreSession session = mock(CacheStoreSession.class);
        doReturn(null).when(session).transaction();

        IdSequencer idSequencer = mock(IdSequencer.class);
        doReturn(1L).when(idSequencer).getNextId();

        ModificationListener listener = mock(ModificationListener.class);
        Map<Long, Map<String, Collection<Cache.Entry<?, ?>>>> calls = new HashMap<>();
        doAnswer(answer -> {
            calls.put(answer.<Long>getArgument(0), answer.getArgument(1));
            return null;
        }).when(listener).handle(anyLong(), anyMap());

        DataCapturerBus<Integer, Integer> cacheStore = create(session, CACHE_NAME, Collections.singletonList(listener), idSequencer);
        cacheStore.write(new CacheEntryImpl<>(1, 1));

        verify(idSequencer, times(1)).getNextId();

        assertEquals(1, calls.size());
        assertTrue(calls.containsKey(1L));
        Map<String, Collection<Cache.Entry<?, ?>>> map = calls.get(1L);
        assertNotNull(map);
        assertEquals(1, map.size());
        assertTrue(map.containsKey(CACHE_NAME));
        Collection<Cache.Entry<?, ?>> entries = map.get(CACHE_NAME);
        assertEquals(1, entries.size());
        Cache.Entry<?, ?> entry = entries.iterator().next();
        assertNotNull(entry);
        assertEquals(1, entry.getKey());
        assertEquals(1, entry.getValue());
    }

    private DataCapturerBus<Integer, Integer> create(
            CacheStoreSession session,
            String cacheName,
            List<ModificationListener> allListeners,
            IdSequencer sequencer
    ) {
        DataCapturerBus<Integer, Integer> result = new DataCapturerBus<>();
        result.session = session;
        result.cacheName = cacheName;
        result.allListeners = allListeners;
        result.sequencer = sequencer;
        return result;
    }
}

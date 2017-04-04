/*
 * Copyright (c) 2017. EPAM Systems.
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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import java.util.Iterator;
import java.util.List;

public class InCacheCommitter implements Committer {

    public static final String TX_COMMIT_CACHE_NAME = "txCommitCache";

    private IgniteCache<Object, Object> cache;


    private InCacheCommitter(Ignite ignite) {
        cache = ignite.cache(TX_COMMIT_CACHE_NAME);
    }

    @Override
    public void commit(Iterator<String> names, Iterator<List> keys, Iterator<List<?>> values) {
        while (names.hasNext()) {
            names.next();
            List<?> keysList = keys.next();
            List<?> valuesList = values.next();
            for (int j = 0; j < keysList.size(); j++) {
                cache.put(keysList.get(j), valuesList.get(j));
            }
        }
    }
}

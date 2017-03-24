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
package com.epam.lathgertha.capturer;

import javax.cache.Cache;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultKeyTransformer implements KeyTransformer {
    @Override
    public TransactionScope apply(Long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates) {
        List<Map.Entry<String, List>> scope = new ArrayList<>(updates.size());

        for (Map.Entry<String, Collection<Cache.Entry<?, ?>>> cacheUpdate : updates.entrySet()) {
            List keys = cacheUpdate
                .getValue()
                .stream()
                .map(Cache.Entry::getKey)
                .collect(Collectors.toList());
            scope.add(new AbstractMap.SimpleImmutableEntry<String, List>(cacheUpdate.getKey(), keys));
        }
        return new TransactionScope(transactionId, scope);
    }
}

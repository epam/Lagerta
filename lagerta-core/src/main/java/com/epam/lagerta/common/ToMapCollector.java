/*
 * Copyright 2017 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.lagerta.common;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.function.Function.identity;

public class ToMapCollector<T, K, U> implements Collector<T, Map<K, U>, Map<K, U>> {
    private static final Set<Collector.Characteristics> CH_ID
            = Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));

    private final BiConsumer<Map<K, U>, T> accumulator;

    public static <T, K, U> Collector<T, ?, Map<K, U>> toMap(Function<? super T, ? extends K> keyMapper,
                                                             Function<? super T, ? extends U> valueMapper) {
        BiConsumer<Map<K, U>, T> accumulator = (map, element) ->
                map.put(keyMapper.apply(element), valueMapper.apply(element));
        return new ToMapCollector<>(accumulator);
    }

    private ToMapCollector(BiConsumer<Map<K, U>, T> accumulator) {
        this.accumulator = accumulator;
    }

    @Override
    public BiConsumer<Map<K, U>, T> accumulator() {
        return accumulator;
    }

    @Override
    public Supplier<Map<K, U>> supplier() {
        return HashMap::new;
    }

    @Override
    public BinaryOperator<Map<K, U>> combiner() {
        return (a, b) -> {
            a.putAll(b);
            return a;
        };
    }

    @Override
    public Function<Map<K, U>, Map<K, U>> finisher() {
        return identity();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return CH_ID;
    }
}

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

package com.epam.lagerta.jmh.d;

import java.util.Iterator;
import java.util.List;

public class IdGenerator {
    private static final int ID_PATTERN_PERIOD = 20;
    private final List<Long> pattern;

    private long shift;
    private Iterator<Long> iterator;

    public IdGenerator(List<Long> pattern) {
        this.pattern = pattern;
        iterator = pattern.iterator();
    }

    public long nextId() {
        if (!iterator.hasNext()) {
            iterator = pattern.iterator();
            shift += ID_PATTERN_PERIOD;
        }
        return iterator.next() + shift;
    }
}

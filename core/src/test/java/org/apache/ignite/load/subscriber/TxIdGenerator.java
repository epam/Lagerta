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

package org.apache.ignite.load.subscriber;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.LongList;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/24/2017 7:16 PM
 */
public class TxIdGenerator {
    private final int period;
    private final LongList pattern;

    private long shift;
    private LongIterator it;

    public TxIdGenerator(int period, LongList pattern) {
        this.period = period;
        this.pattern = pattern;
        it = pattern.longIterator();
    }

    public long nextId() {
        if (!it.hasNext()) {
            it = pattern.longIterator();
            shift += period;
        }
        return it.next() + shift;
    }
}

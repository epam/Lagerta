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

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

/**
 * @author Aleksandr_Meterko
 * @since 12/19/2016
 */
class OffsetHolder {

    private long lastDenseOffset = 0;
    private final MutableLongList sparseCommittedOffsets = new LongArrayList();

    public long getLastDenseOffset() {
        return lastDenseOffset;
    }

    public void setLastDenseOffset(long lastDenseOffset) {
        this.lastDenseOffset = lastDenseOffset;
    }

    public MutableLongList getSparseCommittedOffsets() {
        return sparseCommittedOffsets;
    }
}

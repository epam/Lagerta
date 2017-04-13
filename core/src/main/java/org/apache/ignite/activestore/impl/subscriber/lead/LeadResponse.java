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

package org.apache.ignite.activestore.impl.subscriber.lead;

import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * @author Andrei_Yakushin
 * @since 12/8/2016 11:15 AM
 */
public class LeadResponse implements Serializable {
    public static final LeadResponse EMPTY = new LeadResponse(null, null);

    @Nullable
    private final LongList toCommitIds;

    @Nullable
    private final LongList alreadyProcessedIds;

    public LeadResponse(@Nullable LongList toCommitIds, @Nullable LongList alreadyProcessedIds) {
        this.toCommitIds = toCommitIds;
        this.alreadyProcessedIds = alreadyProcessedIds;
    }

    @Nullable
    public LongList getToCommitIds() {
        return toCommitIds;
    }

    @Nullable
    public LongList getAlreadyProcessedIds() {
        return alreadyProcessedIds;
    }

    public LeadResponse add(LeadResponse other) {
        LongList toCommitIds = join(this.toCommitIds, other.toCommitIds);
        LongList alreadyProcessedIds = join(this.alreadyProcessedIds, other.alreadyProcessedIds);
        return toCommitIds == this.toCommitIds && alreadyProcessedIds == this.alreadyProcessedIds
                ? this
                : new LeadResponse(toCommitIds, alreadyProcessedIds);
    }

    private static LongList join(LongList a, LongList b) {
        if (b == null) {
            return a;
        }
        if (a == null) {
            return LongArrayList.newList(b);
        }
        MutableLongList result = LongArrayList.newList(a);
        result.addAll(b);
        return result;
    }

    @Override
    public String toString() {
        return "Response{" +
                "+" + toCommitIds +
                ", -" + alreadyProcessedIds +
                '}';
    }
}

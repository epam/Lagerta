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

import org.apache.ignite.activestore.commons.Lazy;
import org.apache.ignite.activestore.impl.subscriber.lead.MergeHelper;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Aleksandr_Meterko
 * @since 12/16/2016
 */
class OffsetCalculator {

    private static final long INITIAL_SYNC_POINT = 0;

    private final Lazy<TopicPartition, OffsetHolder> offsets = new Lazy<>(new C1<TopicPartition, OffsetHolder>() {
        @Override public OffsetHolder apply(TopicPartition partition) {
            return new OffsetHolder();
        }
    });

    public Map<TopicPartition, OffsetAndMetadata> calculateChangedOffsets(List<List<TransactionWrapper>> txToCommit) {
        if (txToCommit.isEmpty()) {
            return Collections.emptyMap();
        }
        Lazy<TopicPartition, MutableLongList> offsetsFromTransactions = calculateOffsetsFromTransactions(txToCommit);
        Collection<TopicPartition> allTopics = new HashSet<>(offsets.keySet());
        allTopics.addAll(offsetsFromTransactions.keySet());
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        for (TopicPartition topic : allTopics) {
            OffsetHolder offsetHolder = offsets.get(topic);
            long currentOffset = offsetHolder.getLastDenseOffset();
            long updatedOffset = MergeHelper.mergeWithDenseCompaction(offsetsFromTransactions.get(topic),
                offsetHolder.getSparseCommittedOffsets(), currentOffset);
            if (updatedOffset != INITIAL_SYNC_POINT && updatedOffset != currentOffset) {
                offsetHolder.setLastDenseOffset(updatedOffset);
                result.put(topic, new OffsetAndMetadata(updatedOffset));
            }
        }
        return result;
    }

    private Lazy<TopicPartition, MutableLongList> calculateOffsetsFromTransactions(
        List<List<TransactionWrapper>> txToCommit) {
        Lazy<TopicPartition, MutableLongList> topicToSparseOffsets = new Lazy<>(new IgniteClosure<TopicPartition, MutableLongList>() {
            @Override public MutableLongList apply(TopicPartition s) {
                return new LongArrayList();
            }
        });

        for (List<TransactionWrapper> batch : txToCommit) {
            for (TransactionWrapper wrapper : batch) {
                MutableLongList sparseOffsets = topicToSparseOffsets.get(wrapper.topicPartition());
                sparseOffsets.add(wrapper.offset());
            }
        }
        return topicToSparseOffsets;
    }

}

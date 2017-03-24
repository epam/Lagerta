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

package org.apache.ignite.activestore.impl.transactions;

import com.google.common.primitives.Longs;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/8/2016 3:13 PM
 */
public final class TransactionMessageUtil {
    private TransactionMessageUtil() {
    }

    public static int partitionFor(TransactionMetadata metadata, int partitions) {
        return partitionFor(metadata.getTransactionId(), partitions);
    }

    public static int partitionFor(long transactionId, int partitions) {
        return Longs.hashCode(transactionId) % partitions;
    }
}

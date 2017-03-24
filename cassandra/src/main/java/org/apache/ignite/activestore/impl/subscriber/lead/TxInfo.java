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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.activestore.impl.kv.MessageMetadata;

/**
 * @author Evgeniy_Ignatiev
 * @since 11:28 11/30/2016
 */
public class TxInfo {
    private final UUID consumerId;
    private final MessageMetadata txMetadata;

    public TxInfo(UUID consumerId, MessageMetadata txMetadata) {
        this.consumerId = consumerId;
        this.txMetadata = txMetadata;
    }

    public long id() {
        return txMetadata.getTransactionId();
    }

    public Collection<?> scope() {
        return txMetadata.getCompositeKeys();
    }

    public UUID consumerId() {
        return consumerId;
    }
}


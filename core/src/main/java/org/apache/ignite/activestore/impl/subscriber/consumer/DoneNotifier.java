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

import org.apache.ignite.lang.IgniteInClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 9:09 AM
 */
class DoneNotifier extends TransactionsBufferHolder<DoneNotifier> implements IgniteInClosure<Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DoneNotifier.class);
    private final LocalKafkaWriter localKafkaWriter;

    @Inject
    public DoneNotifier(LocalKafkaWriter localKafkaWriter) {
        this.localKafkaWriter = localKafkaWriter;
    }

    @Override public void apply(Long txId) {
        LOGGER.debug("[C] Transaction {} was committed", txId);
        localKafkaWriter.writeTransactions(buffer.getInProgressTx(txId));
    }

    public void close() {
        localKafkaWriter.close();
    }
}

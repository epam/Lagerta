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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 9:08 AM
 */
class LoggingErrorHandler implements Callback {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingErrorHandler.class);

    @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOGGER.error("[C] Exception while writing record " + metadata, exception);
        }
    }
}

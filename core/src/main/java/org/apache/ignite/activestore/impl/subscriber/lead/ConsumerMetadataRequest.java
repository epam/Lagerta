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

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.subscriber.consumer.SubscriberConsumerService;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * @author Andrei_Yakushin
 * @since 12/19/2016 9:50 AM
 */
class ConsumerMetadataRequest implements IgniteRunnable {
    @IgniteInstanceResource
    private transient Ignite ignite;

    @Override
    public void run() {
        SubscriberConsumerService service = ignite.services().service(SubscriberConsumerService.SERVICE_NAME);

        if (service != null) {
            service.resendTransactionsMetadata();
        }
    }
}

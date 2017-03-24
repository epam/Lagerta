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

import org.apache.ignite.activestore.commons.injection.ActiveStoreService;
import org.apache.ignite.services.ServiceContext;

import javax.inject.Inject;

/**
 * @author Aleksandr_Meterko
 * @since 11/29/2016
 */
public class SubscriberConsumerServiceImpl extends ActiveStoreService implements SubscriberConsumerService {
    @Inject
    private transient SubscriberConsumer subscriberConsumer;

    @Override public void cancel(ServiceContext ctx) {
        subscriberConsumer.cancel();
    }

    @Override public void execute(ServiceContext ctx) throws Exception {
        subscriberConsumer.execute();
    }

    @Override public void resendTransactionsMetadata() {
        subscriberConsumer.resendTransactionsMetadata();
    }

    @Override public void resume() {
        subscriberConsumer.resume();
    }
}

/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.services;

import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.subscriber.CommitStrategy;
import com.epam.lathgertha.subscriber.Reader;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.services.ServiceContext;

public class ReaderServiceImpl implements ReaderService {

    @IgniteInstanceResource
    private transient Ignite ignite;

    @SpringResource(resourceClass = KafkaFactory.class)
    private transient KafkaFactory kafkaFactory;

    @SpringResource(resourceClass = SubscriberConfig.class)
    private transient SubscriberConfig config;

    @SpringResource(resourceClass = Serializer.class)
    private transient Serializer serializer;

    @SpringResource(resourceClass = CommitStrategy.class)
    private transient CommitStrategy commitStrategy;

    private transient Reader reader;

    @Override
    public void cancel(ServiceContext ctx) {
        reader.stop();
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        reader = new Reader(ignite, kafkaFactory, config, serializer, commitStrategy);
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        reader.execute();
    }
}
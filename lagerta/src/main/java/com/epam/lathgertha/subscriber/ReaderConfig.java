/*
 * Copyright 2017 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.kafka.KafkaFactory;
import com.epam.lathgertha.kafka.KafkaLogCommitter;
import com.epam.lathgertha.kafka.SubscriberConfig;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class ReaderConfig {
    public static AnnotationConfigApplicationContext create(ApplicationContext parent) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.setParent(parent);
        context.register(ReaderConfig.class);
        context.refresh();
        return context;
    }

    @Bean(name = "readerId")
    public UUID readerId() {
        return UUID.randomUUID();
    }

    @Bean
    public Reader reader(@Qualifier("ignite-bean") Ignite ignite, KafkaFactory kafkaFactory, SubscriberConfig config,
                         Serializer serializer, CommitStrategy commitStrategy, @Qualifier("readerId") UUID readerId) {
        return new Reader(ignite, kafkaFactory, config, serializer, commitStrategy, readerId);
    }

    @Bean
    public CommitStrategy commitStrategy(CommitServitor commitServitor) {
        return new SequentialCommitStrategy(commitServitor);
    }

    @Bean
    public CommitServitor commitServitor(Serializer serializer, Committer committer,
                                         KafkaLogCommitter kafkaLogCommitter, @Qualifier("readerId") UUID readerId,
                                         @Qualifier("ignite-bean") Ignite ignite) {
        return new CommitServitor(serializer, committer, kafkaLogCommitter, readerId, ignite);
    }
}

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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import com.google.inject.name.Named;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/12/2016 4:46 PM
 */
@Singleton
public class LocalLeadContextLoaderManager {
    private final Ignite ignite;
    private final DataRecoveryConfig dataRecoveryConfig;
    private final UUID nodeId;

    private final LeadService lead;
    private final KafkaFactory kafkaFactory;
    private final AtomicReference<LocalLeadContextLoader> localLoaderRef = new AtomicReference<>();

    @Inject
    public LocalLeadContextLoaderManager(
            Ignite ignite,
            LeadService lead,
            KafkaFactory kafkaFactory,
            DataRecoveryConfig dataRecoveryConfig,
            @Named(DataCapturerBusConfiguration.NODE_ID) UUID nodeId
    ) {
        this.ignite = ignite;
        this.lead = lead;
        this.dataRecoveryConfig = dataRecoveryConfig;
        this.nodeId = nodeId;
        this.kafkaFactory = kafkaFactory;
    }

    public void startContextLoader(UUID leadId, String groupId) {
        LocalLeadContextLoader loader = new LocalLeadContextLoader(kafkaFactory, leadId, lead, dataRecoveryConfig,
            nodeId, groupId);

        loader = localLoaderRef.getAndSet(loader);
        if (loader != null) {
            loader.cancel();
        }
        ignite.scheduler().runLocal(localLoaderRef.get());
    }

    public void stopContextLoader() {
        LocalLeadContextLoader loader = localLoaderRef.getAndSet(null);

        if (loader != null) {
            loader.cancel();
        }
    }
}

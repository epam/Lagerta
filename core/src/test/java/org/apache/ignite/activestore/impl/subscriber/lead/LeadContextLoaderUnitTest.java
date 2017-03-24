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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.activestore.commons.InMemoryReference;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.DataRecoveryConfig;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.publisher.PublisherKafkaService;
import org.apache.ignite.activestore.impl.util.ClusterGroupService;
import org.apache.ignite.cluster.ClusterGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Aleksandr_Meterko
 * @since 1/16/2017
 */
public class LeadContextLoaderUnitTest {

    private static final long START_TX_ID = 5;

    private LeadContextLoader leadContextLoader;
    @Mock
    private Ignite ignite;
    @Mock
    private KafkaFactory kafkaFactory;
    @Mock
    private DataRecoveryConfig dataRecoveryConfig;
    @Mock
    private ClusterGroupService clusterGroupService;
    @Mock
    private PublisherKafkaService kafkaService;

    private final Reference<Long> lastDenseCommitted = new InMemoryReference<>(START_TX_ID);

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        ClusterGroup group = mock(ClusterGroup.class);
        IgniteCompute compute = mock(IgniteCompute.class);
        doReturn(compute).when(ignite).compute();
        doReturn(compute).when(ignite).compute(group);
        doReturn(group).when(clusterGroupService).getLeadContextLoadingClusterGroup(ignite, kafkaFactory, dataRecoveryConfig);

        leadContextLoader = new LeadContextLoader(ignite, kafkaFactory, UUID.randomUUID(), dataRecoveryConfig,
            lastDenseCommitted, clusterGroupService, kafkaService);
    }

    @Test
    public void kafkaServiceCommitsInitialState() {
        leadContextLoader.start();

        verify(kafkaService).seekToTransaction(eq(dataRecoveryConfig), eq(START_TX_ID), eq(kafkaFactory), anyString());
    }

}

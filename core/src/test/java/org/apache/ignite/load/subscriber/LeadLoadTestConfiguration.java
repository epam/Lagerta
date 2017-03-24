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

package org.apache.ignite.load.subscriber;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Singleton;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.kafka.KafkaFactory;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.activestore.mocked.mocks.KafkaMockFactory;
import org.apache.ignite.load.statistics.StatisticsConfig;
import org.apache.ignite.load.statistics.StatisticsModule;
import org.junit.Assert;

/**
 * @author Evgeniy_Ignatiev
 * @since 20:43 01/19/2017
 */
public class LeadLoadTestConfiguration extends DataCapturerBusConfiguration {
    private static final String LOCAL_ADDRESS = "localhost:9999";

    private static final List<WorkerProvider> WORKER_PROVIDERS = Arrays.asList(
        new WorkerProvider(new long[]{0, 1, 5, 6, 15, 18}),
        new WorkerProvider(new long[]{2, 4, 7, 13, 17}),
        new WorkerProvider(new long[]{3, 8, 9, 10, 16}),
        new WorkerProvider(new long[]{11, 12, 14, 19})
    );

    private Class<? extends LeadResponseProcessor> responseProcessorClass;
    private Class<? extends TxMetadataGenerator> metadataGeneratorClass;

    public void setResponseProcessorClass(Class<? extends LeadResponseProcessor> responseProcessorClass) {
        this.responseProcessorClass = responseProcessorClass;
    }

    public void setMetadataGeneratorClass(Class<? extends TxMetadataGenerator> metadataGeneratorClass) {
        this.metadataGeneratorClass = metadataGeneratorClass;
    }

    @SuppressWarnings("unchecked")
    @Override public Module createModule() {
        assertPatternDensity();
        setSuperClusterAddress(LOCAL_ADDRESS);
        setAddress(LOCAL_ADDRESS);
        return Modules.override(super.createModule()).with(new AbstractModule() {
            @Override protected void configure() {
                StatisticsModule statisticsModule = new StatisticsModule();

                statisticsModule.setStatisticsConfig(getStatisticsConfig());
                install(statisticsModule);
                bind(Lead.class).in(Singleton.class);
                bind(WorkersScheduler.class).in(Singleton.class);
                bind(AtomicLong.class).annotatedWith(Names.named(BaseLeadLoadTest.REQUESTS_PERIOD_BIND_NAME))
                    .toInstance(new AtomicLong(BaseLeadLoadTest.INITIAL_REQUESTS_PERIOD));
                bind(Long.class).annotatedWith(Names.named(Lead.MAIN_PING_CHECK_KEY)).toInstance(60_000L);
                bind(Long.class).annotatedWith(Names.named(Lead.CONSUMER_PING_CHECK_KEY)).toInstance(60_000L);
                bind(LeadResponseProcessor.class).to(responseProcessorClass).in(Singleton.class);
                bind(TxMetadataGenerator.class).to(metadataGeneratorClass).in(Singleton.class);
                bind(new TypeLiteral<List<LeadLoadWorker>>() {})
                    .annotatedWith(Names.named(BaseLeadLoadTest.WORKERS_BIND_NAME))
                    .toProvider(new ListOfProviders<>(WORKER_PROVIDERS))
                    .in(Singleton.class);
                bind(KafkaFactory.class).to(KafkaMockFactory.class).in(Singleton.class);
            }
        });
    }

    private static StatisticsConfig getStatisticsConfig() {
        return new StatisticsConfig()
            .enableDebugReporting(true)
            .enableCsvReporting(true)
            .enableNodeOverloadStop(true)
            .setCsvReportDirectory(BaseLeadLoadTest.REPORT_LOCATION)
            .setReportFrequency(BaseLeadLoadTest.REPORT_FREQUENCY)
            .setLatencyThreshold(BaseLeadLoadTest.LATENCY_THRESHOLD)
            .setQuantile(BaseLeadLoadTest.LATENCY_QUANTILE)
            .setWarmupDuration(BaseLeadLoadTest.WARMUP_DURATION);
    }

    private static void assertPatternDensity() {
        // List instead of set to catch duplicates.
        List<Long> densePattern = new ArrayList<>();

        for (long i = 0; i < BaseLeadLoadTest.ID_PATTERN_PERIOD; i++) {
            densePattern.add(i);
        }
        for (WorkerProvider provider : WORKER_PROVIDERS) {
            for (long i : provider.getPattern()) {
                densePattern.remove(i);
            }
        }
        Assert.assertEquals(densePattern, Collections.<Long>emptyList());
    }
}

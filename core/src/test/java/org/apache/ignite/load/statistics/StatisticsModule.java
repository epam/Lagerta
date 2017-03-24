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

package org.apache.ignite.load.statistics;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.ListOf;
import org.apache.ignite.load.statistics.reporters.DebugReporterProvider;
import org.apache.ignite.load.statistics.reporters.GangliaReporterProvider;
import org.apache.ignite.load.statistics.reporters.HumanReadableCsvReporterProvider;
import org.apache.ignite.load.statistics.reporters.IgniteNodeOverloadReporterProvider;
import org.apache.ignite.load.statistics.reporters.ReporterProvider;
import org.apache.ignite.load.statistics.reporters.ReportersManager;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/20/2017 7:10 PM
 */
public class StatisticsModule extends AbstractModule {
    private static final List<Class<? extends ReporterProvider>> PROVIDERS = Arrays.asList(
        IgniteNodeOverloadReporterProvider.class,
        HumanReadableCsvReporterProvider.class,
        DebugReporterProvider.class,
        GangliaReporterProvider.class
    );

    private StatisticsConfig config = StatisticsConfig.defaultConfig();

    public void setStatisticsConfig(StatisticsConfig config) {
        this.config = config;
    }

    @Override protected void configure() {
        bind(ExecutorManager.class).asEagerSingleton();
        bind(MetricRegistry.class).in(Singleton.class);
        bind(Statistics.class).in(Singleton.class);
        bind(StatisticsCollector.class).in(Singleton.class);
        bind(LocalStatisticsUpdater.class).toProvider(LocalStatisticsUpdaterProvider.class).in(Singleton.class);
        bind(RemoteStatisticsUpdaterManager.class).in(Singleton.class);
        bind(new TypeLiteral<List<ReporterProvider>>() {}).toProvider(new ListOf<>(PROVIDERS)).in(Singleton.class);
        bind(StatisticsDriver.class).in(Singleton.class);
        bind(StatisticsConfig.class).toInstance(config);
        bind(ReportersManager.class).in(Singleton.class);
    }

    private static class LocalStatisticsUpdaterProvider implements Provider<LocalStatisticsUpdater> {
        @Inject
        private Ignite ignite;

        @Inject
        private Statistics stats;

        @Inject
        private StatisticsCollector collector;

        @Override public LocalStatisticsUpdater get() {
            UUID localNodeId = ignite.cluster().localNode().id();
            return new LocalStatisticsUpdater(localNodeId, stats, collector);
        }
    }
}

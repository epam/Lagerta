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

package org.apache.ignite.load.statistics.reporters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ScheduledReporter;
import javax.inject.Inject;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.load.statistics.StatisticsConfig;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/20/2017 2:09 PM
 */
public class ReportersManager implements LifecycleAware {
    private final StatisticsConfig config;
    private final List<ReporterProvider> reporterProviders;

    private final List<ScheduledReporter> reporters = new ArrayList<>();

    @Inject
    public ReportersManager(StatisticsConfig config, List<ReporterProvider> reporterProviders) {
        this.config = config;
        this.reporterProviders = reporterProviders;
    }

    @Override public void start() {
        // Do nothing.
    }

    public void startReporters() {
        for (ReporterProvider provider : reporterProviders) {
            ScheduledReporter reporter = provider.getIfEnabled();

            if (reporter != null) {
                reporters.add(reporter);
                reporter.start(config.getReportFrequency(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override public void stop() {
        for (ScheduledReporter reporter : reporters) {
            reporter.stop();
        }
    }
}

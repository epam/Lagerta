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

package org.apache.ignite.load;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.ignite.activestore.impl.DataCapturerBusConfiguration;
import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.load.statistics.StatisticsModule;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/12/2017 7:08 PM
 */
public class LoadTestDCBConfiguration extends DataCapturerBusConfiguration {
    public static final String KEY_GENERATOR = "keyGenerator";
    public static final String VALUE_GENERATOR = "valueGenerator";

    private Class<? extends LoadTestDriver> loadTestDriverClass;
    private Class<? extends WorkerEntryProcessor> entryProcessorClass;
    private Class<? extends Generator> keyGeneratorClass;
    private Class<? extends Generator> valueGeneratorClass;
    private Class<? extends Lead> leadProxyClass;

    public void setLoadTestDriverClass(Class<? extends LoadTestDriver> loadTestDriverClass) {
        this.loadTestDriverClass = loadTestDriverClass;
    }

    public void setEntryProcessorClass(Class<? extends WorkerEntryProcessor> entryProcessorClass) {
        this.entryProcessorClass = entryProcessorClass;
    }

    public void setKeyGeneratorClass(Class<? extends Generator> keyGeneratorClass) {
        this.keyGeneratorClass = keyGeneratorClass;
    }

    public void setValueGeneratorClass(Class<? extends Generator> valueGeneratorClass) {
        this.valueGeneratorClass = valueGeneratorClass;
    }

    public void setLeadProxyClass(Class<? extends Lead> leadProxyClass) {
        this.leadProxyClass = leadProxyClass;
    }

    private Module createLoadTestModule() {
        return new PrivateModule() {
            @Override protected void configure() {
                bind(LoadTestDriver.class).to(loadTestDriverClass).in(Singleton.class);
                bind(LoadTestDriversCoordinator.class).in(Singleton.class);
                bind(WorkerEntryProcessor.class).to(entryProcessorClass).in(Singleton.class);
                bind(Generator.class).annotatedWith(Names.named(KEY_GENERATOR)).to(keyGeneratorClass)
                    .in(Singleton.class);
                bind(Generator.class).annotatedWith(Names.named(VALUE_GENERATOR)).to(valueGeneratorClass)
                    .in(Singleton.class);
                bind(LoadTestConfig.class).toInstance(LoadTestConfig.defaultConfig());
            }
        };
    }

    @Override public Module createModule() {
        return Modules.override(super.createModule()).with(new AbstractModule() {
            @Override protected void configure() {
                install(new StatisticsModule());
                install(createLoadTestModule());
                if (leadProxyClass != null) {
                    bind(Lead.class).to(leadProxyClass).in(Singleton.class);
                }
            }
        });
    }
}

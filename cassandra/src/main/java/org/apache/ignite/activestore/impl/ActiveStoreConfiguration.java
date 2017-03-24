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

package org.apache.ignite.activestore.impl;

import java.io.Serializable;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.IdSequencer;

/**
 * Exposes specific setters for bindings and properties that are required
 * for {@link ActiveCacheStore} to properly function.
 */
public abstract class ActiveStoreConfiguration extends BaseActiveStoreConfiguration {
    /**
     * Sets custom implementation of {@link Exporter}.
     *
     * @param exporter which should be used in backup/restore process.
     */
    public <T extends Exporter & Serializable> void setExporter(T exporter) {
        factories.put(Exporter.class, FactoryBuilder.factoryOf(exporter));
    }

    /**
     * Sets factory of custom implementation of {@link IdSequencer}
     *
     * @param idSequencerFactory factory that should be used to create id sequencer instance.
     */
    public void setIdSequencerFactory(Factory<IdSequencer> idSequencerFactory) {
        factories.put(IdSequencer.class, idSequencerFactory);
    }
}

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

package org.apache.ignite.activestore.impl.noop;

import java.util.Collections;
import java.util.List;

import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.KeyValueReader;
import org.apache.ignite.activestore.MetadataManager;
import org.apache.ignite.activestore.MetadataProvider;
import org.apache.ignite.activestore.impl.BaseActiveStoreConfiguration;
import org.apache.ignite.activestore.impl.InMemoryIdSequencer;
import org.apache.ignite.activestore.impl.InMemoryMetadataManager;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.activestore.impl.KeyValueManagerImpl;
import org.apache.ignite.activestore.impl.MetadataProviderImpl;
import org.apache.ignite.activestore.impl.export.FileExporter;
import org.apache.ignite.activestore.impl.kv.SnapshotAwareKeyValueReaderListener;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

/**
 * @author Evgeniy_Ignatiev
 * @since 10/4/2016 6:43 PM
 */
public class NoOpStoreConfiguration extends BaseActiveStoreConfiguration {
    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        bindings.put(Exporter.class, FileExporter.class);
        bindings.put(Serializer.class, JavaSerializer.class);
        bindings.put(KeyValueManager.class, KeyValueManagerImpl.class);
        bindings.put(MetadataProvider.class, MetadataProviderImpl.class);
        bindings.put(MetadataManager.class, InMemoryMetadataManager.class);
        bindings.put(KeyValueProvider.class, NoOpKeyValueProvider.class);
        bindings.put(KeyValueReader.class, SnapshotAwareKeyValueReaderListener.class);
        bindings.put(IdSequencer.class, InMemoryIdSequencer.class);

        List classes = Collections.singletonList(SnapshotAwareKeyValueReaderListener.class);
        factories.put(List.class, new Injection.ListOf<>(classes));
    }
}

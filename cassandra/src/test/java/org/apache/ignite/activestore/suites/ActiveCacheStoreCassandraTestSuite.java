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

package org.apache.ignite.activestore.suites;

import javax.cache.configuration.Factory;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.config.SingletonDataSourceFactory;
import org.apache.ignite.activestore.impl.InMemoryIdSequencer;
import org.apache.ignite.activestore.impl.cassandra.CassandraActiveStoreConfiguration;
import org.apache.ignite.activestore.impl.cassandra.CassandraMetadataManager;
import org.apache.ignite.activestore.impl.cassandra.persistence.PublicKeyspacePersistenceSettings;
import org.apache.ignite.activestore.rules.CassandraRule;
import org.apache.ignite.activestore.rules.TestResourceFactory;
import org.apache.ignite.activestore.rules.TestResources;
import org.apache.ignite.activestore.utils.CassandraHelper;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

/**
 * Suite which runs all tests with Cassandra based cache store and Java-based config.
 */
public class ActiveCacheStoreCassandraTestSuite extends BasicTestSuite {
    /** */
    public static TestResources resource = TestResourceFactory.getResource();

    @ClassRule
    public static RuleChain chain = RuleChain
        .outerRule(new CassandraRule(false))
        .around(resource);

    /** */
    static {
        CassandraActiveStoreConfiguration activeStoreConfiguration = new CassandraActiveStoreConfiguration();
        activeStoreConfiguration.setDataSourceFactory(new SingletonDataSourceFactory());
        PublicKeyspacePersistenceSettings keyspaceSettings = new PublicKeyspacePersistenceSettings();
        keyspaceSettings.setKeyspace(CassandraHelper.getTestKeyspaces()[0]);
        activeStoreConfiguration.setKeyspacePersistenceSettings(keyspaceSettings);
        activeStoreConfiguration.setIdSequencerFactory(new Factory<IdSequencer>() {
            @Override public IdSequencer create() {
                return new InMemoryIdSequencer();
            }
        });
        resource.setActiveStoreConfiguration(activeStoreConfiguration);
        resource.setTearDownAfterTestAction(new Runnable() {
            @Override public void run() {
                CassandraHelper.clearTablesWithDefaultKeyspace(CassandraMetadataManager.METADATA_TABLE_NAME);
            }
        });
    }
}

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

package org.apache.ignite.activestore.impl.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import javax.inject.Inject;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.activestore.impl.DefaultMetadataManager;
import org.apache.ignite.activestore.impl.MetadataTree;
import org.apache.ignite.activestore.impl.cassandra.datasource.DataSource;
import org.apache.ignite.activestore.impl.cassandra.persistence.KeyValuePersistenceSettingsRegistry;
import org.apache.ignite.activestore.impl.cassandra.persistence.PersistenceController;
import org.apache.ignite.activestore.impl.cassandra.session.CassandraSession;
import org.apache.ignite.activestore.impl.cassandra.session.adapter.GenericExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.resources.LoggerResource;

/**
 * Manager which provides loading and saving tree from Cassandra tables.
 */
public class CassandraMetadataManager extends DefaultMetadataManager {
    /**
     * Name of Cassandra table which stores metadata.
     */
    public static final String METADATA_TABLE_NAME = "ignite_cassandra_metadata";

    /**
     * Key from the table which refers to metadata.
     */
    private static final String METADATA_KEY = "metadata_tree";

    /**
     * Source for connection to Cassandra.
     */
    private final DataSource dataSrc;

    /**
     * Settings which are used for storing data in Cassandra.
     */
    private final KeyValuePersistenceSettingsRegistry settings;

    /**
     * Logger
     */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Creates manager with specified settings.
     *
     * @param dataSrc source
     * @param settings settings
     */
    @Inject
    public CassandraMetadataManager(DataSource dataSrc, KeyValuePersistenceSettingsRegistry settings) {
        this.dataSrc = dataSrc;
        this.settings = settings;
    }

    /** {@inheritDoc} */
    @Override protected MetadataTree loadTree() {
        CassandraSession ses = dataSrc.session(log == null ? new NullLogger() : log);
        final PersistenceController ctrl = settings.get(METADATA_TABLE_NAME);
        MetadataTree tree;
        try {
            tree = ses.execute(new GenericExecutionAssistant<MetadataTree>(true, "READ_METADATA") {
                @Override public String getStatement() {
                    return ctrl.getLoadStatement(false, true, true);
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return ctrl.bindKey(statement, METADATA_KEY);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return ctrl.getPersistenceSettings();
                }

                @Override public MetadataTree process(Row row) {
                    return row == null ? null : (MetadataTree)ctrl.buildValueObject(row);
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
        if (tree == null) {
            tree = new MetadataTree();
            saveTree(tree);
        }
        return tree;
    }

    /** {@inheritDoc} */
    @Override protected void saveTree(final MetadataTree tree) {
        CassandraSession ses = dataSrc.session(log == null ? new NullLogger() : log);
        final PersistenceController ctrl = settings.get(METADATA_TABLE_NAME);
        try {
            ses.execute(new GenericExecutionAssistant<Void>(true, "WRITE_METADATA") {
                @Override public String getStatement() {
                    return ctrl.getWriteStatement();
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return ctrl.bindKeyValue(statement, METADATA_KEY, tree);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return ctrl.getPersistenceSettings();
                }

                @Override public Void process(Row row) {
                    return null;
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
    }
}

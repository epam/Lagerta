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

package org.apache.ignite.activestore;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import org.apache.ignite.services.Service;

/**
 * Core service which exposes all metadata related logic to consumers.
 */
public interface CommandService extends Service {

    /**
     * Name of this service in Ignite.
     */
    String SERVICE_NAME = "commands.service";

    /**
     * Creates new snapshot with given label.
     *
     * @param label to associate with snapshot. Must be unique.
     * @return created snapshot.
     */
    Metadata createSnapshot(String label);

    /**
     * Performs rollback of snapshot tree and data to specified snapshot. It should fail if there are running user
     * transactions as this operation modifies metadata head.
     *
     * @param label of snapshot to which we should rollback.
     * @return new head.
     */
    Metadata rollback(String label);

    /**
     * Performs rollforward operation to previously created snapshot.
     *
     * @param label of snapshot to which we should rollforward.
     * @return new head.
     */
    Metadata rollforward(String label);

    /**
     * Merges all snapshots withing given path.
     *
     * @param from label of snapshot where to start merging (exclusive).
     * @param to label of snapshot where to end merging (inclusive).
     * @return new snapshot which contains data of merged ones.
     */
    Metadata merge(String from, String to);

    /**
     * Backs up all metadata and data. This leads to saving inner state of underlying storage into some independent
     * format which is specified by {@link Exporter}.
     *
     * @param destination to where backup should be saved.
     * @param snapshotLabel by which created snapshot will be marked.
     * @return backed up metadatas.
     * @throws IOException in case of errors during backup.
     */
    Collection<Metadata> backup(URI destination, String snapshotLabel) throws IOException;

    /**
     * Performs backup of all metadata and partial backup of data. It backs up only data which is newer than given
     * snapsht. This leads to saving inner state of underlying storage into some independent format which is specified
     * by {@link Exporter}.
     *
     * @param destination to where backup should be saved.
     * @param baseLabel from where back up process will start.
     * @param snapshotLabel by which created snapshot will be marked.
     * @return backed up metadatas.
     * @throws IOException in case of errors during backup.
     */
    Collection<Metadata> incrementalBackup(URI destination, String baseLabel, String snapshotLabel) throws IOException;

    /**
     * Performs restore operation on all data which is contained in specified backup. This loads all metadata and data
     * and invalidates all affected data in caches, so it will be properly loaded from underlying storage.
     *
     * @param source where previous backup was saved.
     * @throws IOException in case of errors during restore.
     */
    void restore(URI source) throws IOException;

    /**
     * Returns whole metadata tree.
     *
     * @return tree.
     */
    Tree metadataTree();
}

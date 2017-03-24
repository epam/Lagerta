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

package org.apache.ignite.activestore.cli;

import java.io.IOException;
import java.net.URI;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.CommandService;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.Tree;
import org.apache.ignite.activestore.commons.Injection;
import org.springframework.util.StringUtils;

/**
 * Class which exposes API to command line shell and returns answers in cmd-friendly format.
 */
public class CommandServiceCLIAware {
    /**
     * Service which provides all logic.
     */
    private final CommandService commandService;

    /**
     * Instance of grid.
     */
    private final Ignite ignite;

    /**
     * Creates instance of this class.
     *
     * @param ignite grid to be used.
     */
    public CommandServiceCLIAware(Ignite ignite) {
        this.ignite = ignite;
        this.commandService = ignite.services().serviceProxy(CommandService.SERVICE_NAME, CommandService.class, false);
        Injection.inject(this, ignite);
    }

    /**
     * Transforms string representation to URI.
     *
     * @param fsLocation path on file system.
     * @return URI to this path.
     */
    private static URI toURI(String fsLocation) {
        String uriForm = fsLocation.startsWith("/") ? "file://" + fsLocation : "file:///" + fsLocation;
        return URI.create(uriForm);
    }

    /**
     * Backs up all metadata and data. This leads to saving inner state of underlying storage into some independent
     * format which is specified by {@link Exporter}.
     *
     * @param destination to where backup should be saved.
     * @param snapshotLabel by which created snapshot will be marked.
     * @return backed up metadatas.
     * @throws IOException in case of errors during backup.
     */
    public String backup(String destination, String snapshotLabel) throws IOException {
        URI uri = toURI(destination);

        commandService.backup(uri, snapshotLabel);
        return "Backup successful";
    }

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
    public String incrementalBackup(String destination, String baseLabel, String snapshotLabel) throws IOException {
        URI uri = toURI(destination);

        commandService.incrementalBackup(uri, baseLabel, snapshotLabel);
        return "Incremental backup successful";
    }

    /**
     * Performs restore operation on all data which is contained in specified backup. This loads all metadata and data
     * and invalidates all affected data in caches, so it will be properly loaded from underlying storage.
     *
     * @param source where previous backup was saved.
     * @throws IOException in case of errors during restore.
     * @return per-cache statistics of added/removed entries in data read during restore.
     */
    public String restore(String source) throws IOException {
        URI uri = toURI(source);

        commandService.restore(uri);
        return "Restore successful";
    }

    /**
     * Prints all snapshots which were created.
     * @return string list of snapshots.
     */
    public String snapshots() {
        StringBuilder result = new StringBuilder();
        Tree tree = commandService.metadataTree();
        Iterable<Metadata> metadatas = tree.allMetadatas();
        Metadata head = tree.head();
        for (Metadata metadata : metadatas) {
            if (metadata.equals(head)) {
                result.append("[HEAD] ");
            }
            result.append(stringify(metadata)).append("\n");
        }
        return result.toString();
    }

    /**
     * Formats snapshot to cozy string.
     * @param metadata to get string for.
     * @return string representation.
     */
    private String stringify(Metadata metadata) {
        return "Snapshot(timestamp=" + metadata.getTimestamp() + ", labels=" +
            StringUtils.collectionToCommaDelimitedString(commandService.metadataTree().getLabels(metadata)) + ")";
    }
}

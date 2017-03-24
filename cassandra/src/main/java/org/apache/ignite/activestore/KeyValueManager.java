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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * This manager is intended to work with data stored in underlying storage. It reacts to operations with metadata and
 * performs simmetric operations with data storage.
 */
public interface KeyValueManager {
    /**
     * Allows to perform initialization logic for underlying storage on creation of snapshot. Otherwise this logic will
     * need to be performed during first attempt to use storage.
     *
     * @param newSnapshot which will be associated with data
     */
    void createSnapshot(Metadata newSnapshot);

    /**
     * Performs the given action for each key in caches that were mutated within snapshots in specified subPath.
     *
     * @param subPath metadatas to get changed keys from.
     * @param action to be performed on each key. It obtains cache name and all keys that were changed within it.
     */
    void invalidate(Iterable<Metadata> subPath, IgniteBiInClosure<String, Set<Object>> action);

    /**
     * Merges key-value pairs from specified metadatas into the given one.
     *
     * @param sourcePath to be replaced.
     * @param destination replacement.
     */
    void merge(Iterable<Metadata> sourcePath, Metadata destination);

    /**
     * Get all snapshot ids grouped by cacheName filtered by specified metadatas.
     *
     * @param metadatas to group.
     * @return mapping of cache name to metadatas where this cache name participated.
     */
    Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas);

    /**
     * Performs backup operation for data found by given arguments.
     *
     * @param cacheName name of Ignite cache.
     * @param metadata snapshot to be backed up.
     * @param writer which performs actual saving logic.
     * @throws IOException in case of errors during writing data.
     */
    void backup(String cacheName, Metadata metadata, Exporter.Writer writer) throws IOException;

    /**
     * Obtains closure which is later used in restore process.
     *
     * @param cacheName name of Ignite cache.
     * @param metadata snapshot which is restored.
     * @return closure which obtains key-value pairs written on backup step.
     */
    IgniteInClosure<Map<Object, Object>> getDataWriter(String cacheName, Metadata metadata);
}

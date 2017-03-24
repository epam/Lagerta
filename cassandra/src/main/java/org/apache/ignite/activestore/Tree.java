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

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Represents tree of metadatas and supports read operations on it.
 */
public interface Tree {

    /**
     * Finds metadatas which are located between from (exclusive) and to (inclusive). Supports forks in tree on the
     * way.
     *
     * @param from metadata to start with.
     * @param to metadata to end with.
     * @return collection of metadatas which represent ordered path.
     */
    Iterable<Metadata> subPath(Metadata from, Metadata to);

    /**
     * Finds metadatas which are located between from (exclusive) and to (inclusive). Does not support forks in tree on
     * the way.
     *
     * @param from metadata to start with.
     * @param to metadata to end with.
     * @return collection of metadatas which represent ordered path.
     */
    Iterable<Metadata> subLinePath(Metadata from, Metadata to);

    /**
     * Get current path from metadata store.
     *
     * @param head metadata which acts as the last added leaf in tree or null if we want currenct active path.
     */
    List<Metadata> loadCurrentPath(Metadata head);

    /**
     * Finds metadata by label.
     *
     * @param label of metadata.
     * @return existing metadata instance or null.
     */
    Metadata findByLabel(String label);

    /**
     * Finds metadatas which were created after metadata with specified label.
     *
     * @param baseLabel to look starting metadata.
     * @return newer metadatas.
     */
    Collection<Metadata> getNewerMetadatas(String baseLabel);

    /**
     * Returns the most recent metadata added which is used for writing current operations.
     *
     * @return head.
     */
    Metadata head();

    /**
     * Gets all labels associated with metadata.
     *
     * @param metadata to check.
     * @return its labels.
     */
    Set<String> getLabels(Metadata metadata);

    Collection<Metadata> allMetadatas();
}

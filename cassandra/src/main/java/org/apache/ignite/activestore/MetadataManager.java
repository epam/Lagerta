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
import org.apache.ignite.activestore.Exporter.Writer;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Class which provides control over metadata in application which includes snapshot reading and creation. Each snapshot
 * has a unique label which may be used for finding it in a tree.
 */
public interface MetadataManager {

    /**
     * Creates metadata by specified labels.
     *
     * @param labels to bind to metadata.
     * @return new metadata instance.
     */
    Metadata createMetadata(String... labels);

    /**
     * Finds metadata by label.
     *
     * @param label of metadata.
     * @return existing metadata instance or null.
     */
    Metadata find(String label);

    /**
     * Unbinds label from existing metadata.
     *
     * @param label to unbind.
     * @return metadata which was previously labeled with this label.
     */
    Metadata remove(String label);

    /**
     * Bind labels to specified metadata.
     *
     * @param metadata to label.
     * @param labels to associate with metadata.
     */
    void label(Metadata metadata, String... labels);

    /**
     * Persists detached head to metadata store and link it with parent.
     *
     * @param head new metadata head.
     * @param parent node in tree for this head.
     */
    void putHead(Metadata head, Metadata parent);

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
     * Replaces sub path with replacement.
     *
     * @param subPath path to be replaced in tree.
     * @param replacement metadata which will replace path.
     */
    void replace(Iterable<Metadata> subPath, Metadata replacement);

    /**
     * Get current path from metadata store.
     *
     * @param head metadata which acts as the last added leaf in tree or null if we want currenct active path.
     */
    List<Metadata> loadCurrentPath(Metadata head);

    /**
     * Returns whole tree of metadatas.
     *
     * @return tree.
     */
    Tree tree();

    /**
     * Writes tree structure to the writer. Uses key-value write format.
     *
     * @param writer which will hold information about tree.
     * @return metadatas which were written.
     * @throws IOException in case of writing errors.
     */
    Iterable<Metadata> backup(Writer writer) throws IOException;

    /**
     * Return closure which will be called on deserialized instance in format of key-value pairs. Keys will be the same
     * which were output from backup method.
     *
     * @return closure.
     */
    IgniteInClosure<Map<Object, Object>> restore();
}

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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.Tree;

/**
 * Read/write implementation of {@link Tree}.
 */
public class MetadataTree implements Tree, Serializable {

    /**
     * Key which is used to save tree during backup/restore process.
     */
    private static final String KEY_TREE = "tree";

    /**
     * Mapping from child to parent node.
     */
    private final Map<Metadata, Metadata> tree = new HashMap<>();

    /**
     * Mapping from base metadata to merged metadata. Used as we do not delete metadatas from tree but we should
     * maintain relations between them during merges.
     */
    private final Map<Metadata, Metadata> merged = new HashMap<>();

    /**
     * Head metadata.
     */
    private Metadata lastAdded;

    /**
     * Binds label to specific metadata.
     */
    private final Map<String, Metadata> labels = new HashMap<>();

    /**
     * Restores metadata tree based on what was written during backup procedure.
     *
     * @param map with data from backup.
     * @return intance of restore tree.
     */
    public static MetadataTree restore(Map<Object, Object> map) {
        Object value = map.get(KEY_TREE);
        if (value instanceof MetadataTree) {
            return (MetadataTree)value;
        }
        throw new RuntimeException("Cannot deserialize tree: key" + KEY_TREE + " not found");
    }

    /** {@inheritDoc} */
    @Override public Metadata findByLabel(String label) {
        return labels.get(label);
    }

    /** {@inheritDoc} */
    @Override public Collection<Metadata> getNewerMetadatas(String baseLabel) {
        Collection<Metadata> result = new HashSet<>();
        Metadata base = findByLabel(baseLabel);
        if (base == null) {
            throw new IllegalArgumentException("Metadata with label " + baseLabel + " not found");
        }
        Map<Metadata, Metadata> allData = new HashMap<>(tree);
        allData.putAll(merged);
        long oldestTime = base.getTimestamp();
        for (Metadata metadata : allData.keySet()) {
            if (metadata.getTimestamp() < oldestTime) {
                Metadata current = metadata;
                while (current != null && !current.equals(base)) {
                    current = allData.get(current);
                }
                if (current != null) {
                    oldestTime = metadata.getTimestamp();
                }
            }
        }
        for (Metadata metadata : tree.keySet()) {
            if (metadata.getTimestamp() > oldestTime) {
                result.add(metadata);
            }
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override public Metadata head() {
        return lastAdded;
    }

    /**
     * Persists detached head to metadata store and link it with parent.
     *
     * @param head new metadata head.
     * @param parent node in tree for this head.
     */
    public void putHead(Metadata head, Metadata parent) {
        tree.put(head, parent);
        lastAdded = head;
    }

    /**
     * Unbinds label from existing metadata.
     *
     * @param label to unbind.
     * @return metadata which was previously labeled with this label.
     */
    public Metadata remove(String label) {
        return labels.remove(label);
    }

    /**
     * Bind labels to specified metadata.
     *
     * @param metadata to label.
     * @param dataLabels to associate with metadata.
     */
    public void label(Metadata metadata, String... dataLabels) {
        setLabel(metadata, metadata.getDefaultLabel());
        for (String label : dataLabels) {
            setLabel(metadata, label);
        }
    }

    /**
     * Bind label to specified metadata if it does not clashes with existing ones.
     *
     * @param metadata to label.
     * @param label to associate with metadata.
     */
    private void setLabel(Metadata metadata, String label) {
        if (findByLabel(label) != null) {
            throw new RuntimeException("attempt to override label " + label);
        }
        labels.put(label, metadata);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Metadata> subPath(Metadata from, Metadata to) {
        if (from == null) {
            throw new IllegalArgumentException("From metadata is null");
        }
        if (to == null) {
            throw new IllegalArgumentException("To metadata is null");
        }
        List<Metadata> result = new ArrayList<>();
        Metadata current = to;
        while (!current.equals(from)) {
            result.add(current);
            current = tree.get(current);
            if (current == null) {
                throw new UnsupportedOperationException("Cannot find sub path between " + from + " and " + to);
            }
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override public Iterable<Metadata> subLinePath(Metadata from, Metadata to) {
        if (from == null) {
            throw new IllegalArgumentException("From metadata is null");
        }
        if (to == null) {
            throw new IllegalArgumentException("To metadata is null");
        }
        List<Metadata> result = new ArrayList<>();
        Metadata current = to;
        while (!current.equals(from)) {
            result.add(current);
            current = tree.get(current);
            if (current == null) {
                throw new UnsupportedOperationException("Cannot find sub path between " + from + " and " + to);
            }
            if (hasManyChildren(current)) {
                throw new UnsupportedOperationException("Cannot find liner sub path between " + from + " and " + to +
                    ". Snapshot " + current + " has more than one child snapshot.");
            }
        }
        result.add(current);
        return result;
    }

    /**
     * Checks if metadata has more than one child.
     *
     * @param current metadata to check.
     * @return true if it has more than 1 child. False otherwise.
     */
    private boolean hasManyChildren(Metadata current) {
        boolean first = true;
        for (Metadata metadata : tree.values()) {
            if (current.equals(metadata)) {
                if (first) {
                    first = false;
                }
                else {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Replaces sub path with replacement.
     *
     * @param subPath path to be replaced in tree.
     * @param replacement metadata which will replace path.
     */
    public void replace(Iterable<Metadata> subPath, Metadata replacement) {
        Metadata parent = null;
        for (Metadata metadata : subPath) {
            if (parent == null) {
                for (Map.Entry<Metadata, Metadata> entry : tree.entrySet()) {
                    if (metadata.equals(entry.getValue())) {
                        entry.setValue(replacement);
                    }
                }
            }
            parent = tree.remove(metadata);
            merged.put(metadata, replacement);
            for (String label : getLabels(metadata)) {
                remove(label);
                setLabel(replacement, label);
            }
        }
        tree.put(replacement, parent);
    }

    /** {@inheritDoc} */
    @Override public Set<String> getLabels(Metadata metadata) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, Metadata> entry : labels.entrySet()) {
            if (entry.getValue().equals(metadata)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override public List<Metadata> loadCurrentPath(Metadata head) {
        List<Metadata> result = new ArrayList<>();
        Metadata current = head == null ? lastAdded : head;
        while (current != null) {
            result.add(current);
            current = tree.get(current);
        }
        return result;
    }

    /**
     * Writes tree structure to the writer. Uses key-value write format.
     *
     * @param writer which will hold information about tree.
     * @return metadatas which were written.
     * @throws IOException in case of writing errors.
     */
    public Iterable<Metadata> backup(Exporter.Writer writer) throws IOException {
        writer.write(KEY_TREE, this);
        return tree.keySet();
    }

    @Override
    public Collection<Metadata> allMetadatas() {
        Set<Metadata> result = new HashSet<>();
        result.addAll(tree.keySet());
        result.addAll(merged.keySet());
        return result;
    }
}

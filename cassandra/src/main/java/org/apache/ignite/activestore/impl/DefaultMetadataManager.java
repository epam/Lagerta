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
import java.util.List;
import java.util.Map;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.MetadataManager;
import org.apache.ignite.activestore.Tree;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Default implementation of {@link MetadataManager} which contains all common logic. It is preferred to subclass from
 * this class in case of necessity in modifications.
 */
public abstract class DefaultMetadataManager implements MetadataManager {
    /**
     * Loads tree from underlying storage.
     *
     * @return loaded tree.
     */
    protected abstract MetadataTree loadTree();

    /**
     * Save tree to underlying storage.
     *
     * @param tree to save.
     */
    protected abstract void saveTree(MetadataTree tree);

    /** {@inheritDoc} */
    @Override public Metadata find(String label) {
        MetadataTree tree = loadTree();
        return tree.findByLabel(label);
    }

    /** {@inheritDoc} */
    @Override public void putHead(Metadata head, Metadata parent) {
        MetadataTree tree = loadTree();
        tree.putHead(head, parent);
        saveTree(tree);
    }

    /** {@inheritDoc} */
    @Override public Metadata createMetadata(String... labels) {
        Metadata metadata = new Metadata(System.currentTimeMillis());
        label(metadata, labels);
        return metadata;
    }

    /** {@inheritDoc} */
    @Override public Metadata remove(String label) {
        MetadataTree tree = loadTree();
        Metadata result = tree.remove(label);
        saveTree(tree);
        return result;
    }

    /** {@inheritDoc} */
    @Override public void label(Metadata metadata, String... labels) {
        MetadataTree tree = loadTree();
        tree.label(metadata, labels);
        saveTree(tree);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Metadata> subPath(Metadata from, Metadata to) {
        MetadataTree tree = loadTree();
        return tree.subPath(from, to);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Metadata> subLinePath(Metadata from, Metadata to) {
        MetadataTree tree = loadTree();
        return tree.subLinePath(from, to);
    }

    /** {@inheritDoc} */
    @Override public void replace(Iterable<Metadata> subPath, Metadata replacement) {
        MetadataTree tree = loadTree();
        tree.replace(subPath, replacement);
        saveTree(tree);
    }

    /** {@inheritDoc} */
    @Override public List<Metadata> loadCurrentPath(Metadata head) {
        MetadataTree tree = loadTree();
        return tree.loadCurrentPath(head);
    }

    /** {@inheritDoc} */
    @Override public Iterable<Metadata> backup(Exporter.Writer writer) throws IOException {
        MetadataTree tree = loadTree();
        return tree.backup(writer);
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<Map<Object, Object>> restore() {
        return new IgniteInClosure<Map<Object, Object>>() {
            @Override public void apply(Map<Object, Object> map) {
                MetadataTree tree = MetadataTree.restore(map);
                saveTree(tree);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Tree tree() {
        return loadTree();
    }
}

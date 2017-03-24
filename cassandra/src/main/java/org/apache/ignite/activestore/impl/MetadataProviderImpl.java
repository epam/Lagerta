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

import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.MetadataProvider;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.util.MetadataAtomicsHelper;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.activestore.impl.CommandServiceImpl.HEAD_ATOMIC_NAME;
import static org.apache.ignite.activestore.impl.CommandServiceImpl.PATH_ATOMIC_NAME;
import static org.apache.ignite.activestore.impl.util.MetadataAtomicsHelper.getReference;

/**
 * Platform-agnostic implementation of {@link MetadataProvider}. Works on top of Ignite messaging to receive updates of
 * metadata information.
 */
public class MetadataProviderImpl implements MetadataProvider, LifecycleAware {
    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Reference to current metadata head.
     */
    private transient Reference<Metadata> head;

    /**
     * Path to current head which is store locally on node.
     */
    private transient List<Metadata> localPath;

    /**
     * Specifies status in Ignite lyfecycle.
     */
    private transient boolean active;

    /** {@inheritDoc} */
    @Override public Metadata head() {
        return active
            ? (head == null ? head = getReference(ignite, HEAD_ATOMIC_NAME, false) : head).get()
            : null;
    }

    /** {@inheritDoc} */
    @Override public Iterable<Metadata> path() {
        return localPath == null ? localPath = MetadataAtomicsHelper.<List<Metadata>>getReference(ignite, PATH_ATOMIC_NAME, false).get() : localPath;
    }

    public void setLocalPath(List<Metadata> localPath) {
        this.localPath = localPath;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        active = true;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        active = false;
    }

    /**
     * Performs initialization of this instance.
     */
    @SuppressWarnings("unused")
    @PostConstruct
    public void init() {
    }
}

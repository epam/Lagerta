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
import java.net.URI;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.activestore.impl.util.ExportNameConventionUtil.nameFor;

/**
 * Job which performs backup operation over caches.
 */
class BackupJob extends ComputeJobAdapter {
    /**
     * Where backup should be stored.
     */
    private final URI destination;

    /**
     * Name of cache which is backing up.
     */
    private final String cacheName;

    /**
     * Snapshot to backup.
     */
    private final Metadata metadata;

    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Injected manager.
     */
    @Inject
    private transient KeyValueManager keyValueManager;

    /**
     * Injected exporter.
     */
    @Inject
    private transient Exporter exporter;

    /**
     * Creates instance of job with given settings.
     *
     * @param destination to backup.
     * @param cacheName Ignite cache name.
     * @param metadata snapshot.
     */
    public BackupJob(URI destination, String cacheName, Metadata metadata) {
        this.destination = destination;
        this.cacheName = cacheName;
        this.metadata = metadata;
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        Injection.inject(this, ignite);
        try (Exporter.WriterProvider provider = exporter.write(destination)) {
            try (Exporter.Writer writer = provider.open(nameFor(cacheName, metadata))) {
                keyValueManager.backup(cacheName, metadata, writer);
                return null;
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}

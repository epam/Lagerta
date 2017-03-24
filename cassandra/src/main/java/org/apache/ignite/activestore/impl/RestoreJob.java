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
import javax.cache.Cache;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.activestore.impl.util.ExportNameConventionUtil.cacheAndMetadataFor;
import static org.apache.ignite.activestore.impl.util.ExportNameConventionUtil.isData;

/**
 * Job which performs backup operation over caches.
 */
class RestoreJob extends ComputeJobAdapter {
    /**
     * Location of existing backup.
     */
    private final URI source;

    /**
     * Resource name from backup.
     */
    private final String name;

    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Auto-injected manager.
     */
    @Inject
    private transient KeyValueManager keyValueManager;

    /**
     * Auto-injected exporter.
     */
    @Inject
    private transient Exporter exporter;

    /**
     * Creates instance of job with given settings.
     *
     * @param source path to backup.
     * @param name of specific resource.
     */
    public RestoreJob(URI source, String name) {
        this.source = source;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        Injection.inject(this, ignite);
        try (Exporter.ReaderProvider provider = exporter.read(source)) {
            try (Exporter.Reader reader = provider.open(name)) {
                if (isData(name)) {
                    Cache.Entry<String, Metadata> entry = cacheAndMetadataFor(name);
                    if (entry != null) {
                        reader.read(keyValueManager.getDataWriter(entry.getKey(), entry.getValue()));
                    }
                }
                return null;
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }
}

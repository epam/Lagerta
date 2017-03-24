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

package org.apache.ignite.activestore.impl.util;

import javax.cache.Cache;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;

/**
 * Utility class which provides generation and parsing of names for resources used in backup/restore process.
 */
public final class ExportNameConventionUtil {
    /**
     * Name of metadata resource.
     */
    public static final String METADATA_RESOURCE_NAME = "metadata";

    /**
     * Prefix of data resources.
     */
    private static final String DATA_PREFIX = "data";

    /**
     * Delimiter for resource parts.
     */
    private static final String DELIMITER = "/";

    /**
     * Creating instances is forbidden.
     */
    private ExportNameConventionUtil() {
    }

    /**
     * Generates name for resource.
     *
     * @param cacheName Ignite cache name
     * @param metadata to store.
     * @return name of resource.
     */
    public static String nameFor(String cacheName, Metadata metadata) {
        return DATA_PREFIX + DELIMITER + cacheName + DELIMITER + metadata.getTimestamp();
    }

    /**
     * Checks whether resource represented by this name is data.
     *
     * @param name full name of resource.
     * @return true if resource is data, false otherwise.
     */
    public static boolean isData(String name) {
        return name.startsWith(DATA_PREFIX);
    }

    /**
     * Returns cache name and metadata represented by this resource name.
     *
     * @param name of resource.
     * @return name and metadata if current name is data name or null otherwise.
     */
    public static Cache.Entry<String, Metadata> cacheAndMetadataFor(String name) {
        String[] names = name.replaceAll("\\\\", DELIMITER).split(DELIMITER);
        return names.length == 3
            ? new CacheEntryImpl<>(names[1], new Metadata(Long.parseLong(names[2])))
            : null;
    }
}

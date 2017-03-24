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

package org.apache.ignite.activestore.impl.cassandra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.ignite.activestore.Metadata;

/**
 * Helper class to obtain name of table in Cassandra.
 */
public class SnapshotHelper {
    /**
     * Cached instance of {@link DateFormat}.
     */
    private static final ThreadLocal<DateFormat> FORMAT = new ThreadLocal<>();

    /**
     * Returns formatter which is used for naming a table.
     *
     * @return DateFormat
     */
    private static DateFormat format() {
        DateFormat result = FORMAT.get();
        if (result == null) {
            result = new SimpleDateFormat("MM_dd_yyyy_HH_mm_ss_SSS");
            result.setTimeZone(TimeZone.getTimeZone("UTC"));
            FORMAT.set(result);
        }
        return result;
    }

    /**
     * Returns table name for Cassandra.
     *
     * @param cache Ignite cache name.
     * @param metadata which is saved.
     * @return name of Cassandra table.
     */
    public static String getTableName(String cache, Metadata metadata) {
        return cache + "_" + format().format(metadata.getTimestamp());
    }
}

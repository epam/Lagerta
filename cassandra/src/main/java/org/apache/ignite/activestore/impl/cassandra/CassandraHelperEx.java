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

import java.util.regex.Pattern;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.ignite.cache.store.cassandra.common.CassandraHelper;

/**
 * Helper class for working with Cassandra.
 */
public class CassandraHelperEx {
    /**
     * Pattern for finding errors about missing Cassandra keyspace.
     */
    private static final Pattern KEYSPACE_EXIST_ERROR3 = Pattern.compile("Keyspace [0-9a-zA-Z_]+ doesn't exist");

    /**
     * Checks if Cassandra keyspace absence error occurred.
     *
     * @param e Exception to check.
     * @return {@code true} in case of keyspace absence error.
     */
    public static boolean isKeyspaceAbsenceError(Throwable e) {
        if (CassandraHelper.isKeyspaceAbsenceError(e)) {
            return true;
        }
        while (e != null) {
            if (e instanceof InvalidQueryException && KEYSPACE_EXIST_ERROR3.matcher(e.getMessage()).matches()) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

}

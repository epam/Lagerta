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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Class which provides logic of actual writing/reading during backup/restore procedures.
 */
public interface Exporter {
    /**
     * Prepares writing session for backup.
     *
     * @param destination where backup should be written.
     * @return provider which represents one writing session.
     * @throws IOException in case of error during writing.
     */
    WriterProvider write(URI destination) throws IOException;

    /**
     * Prepares reading session for restore.
     *
     * @param source where existing backup is located.
     * @return provider which represents one reading session.
     * @throws IOException in case of error during reading.
     */
    ReaderProvider read(URI source) throws IOException;

    /**
     * Cleans up any resources which were created during write method call.
     *
     * @param destination where backup was written.
     * @throws IOException in case of errors during cleanup.
     */
    void cleanup(URI destination) throws IOException;

    /**
     * Representation of writing session. Works on top of specific destination allowing to write to sub-destinations.
     */
    interface WriterProvider extends Closeable {
        /**
         * Creates resource with specified name.
         *
         * @param name of resource.
         * @return writer which will write to this resource.
         * @throws IOException in case of errors during writing.
         */
        Writer open(String name) throws IOException;
    }

    /**
     * Class which performs actual writing of data to a resource.
     */
    interface Writer extends Closeable {
        /**
         * Writes data to underlying resource.
         *
         * @param key of data.
         * @param value of data.
         * @throws IOException in case of errors during writing.
         */
        void write(Object key, Object value) throws IOException;
    }

    /**
     * Representation of reading session. Works on top of specific destination allowing to read from sub-destinations.
     */
    interface ReaderProvider extends Closeable {
        /**
         * Returns all full names of sub-destinations available for reading.
         *
         * @return names
         */
        Iterable<String> getNames();

        /**
         * Opens specific resource for reading.
         *
         * @param name name of resource
         * @return reader which will read from this resource.
         */
        Reader open(String name);
    }

    /**
     * Class which performs actual reading of data from a resource.
     */
    interface Reader extends Closeable {
        /**
         * Reads key-value pairs and performs specified action on them.
         *
         * @param action to be called when all key-value pairs are read.
         * @throws IOException in case of error during reading.
         */
        void read(IgniteInClosure<Map<Object, Object>> action) throws IOException;
    }
}

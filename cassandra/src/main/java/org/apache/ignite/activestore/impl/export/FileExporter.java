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

package org.apache.ignite.activestore.impl.export;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.impl.util.ExportNameConventionUtil;
import org.apache.ignite.activestore.impl.util.FileUtils;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Implementation of {@link Exporter} which performs backup/restore using files on file system.
 */
public class FileExporter implements Exporter {

    /**
     * Serializer which specifies strategy of serialization of data.
     */
    private Serializer serializer;

    /**
     * Creates exporter with given serializer.
     *
     * @param serializer to be used.
     */
    @Inject
    public FileExporter(Serializer serializer) {
        this.serializer = serializer;
    }

    /** {@inheritDoc} */
    @Override public WriterProvider write(URI destination) throws IOException {
        Path path = Paths.get(destination);
        return new FileWriterProvider(path, serializer);
    }

    /** {@inheritDoc} */
    @Override public ReaderProvider read(URI source) throws IOException {
        Path path = Paths.get(source);
        return new FileReaderProvider(path, serializer);
    }

    /** {@inheritDoc} */
    @Override public void cleanup(URI destination) throws IOException {
        FileUtils.deleteDir(Paths.get(destination));
    }

    /**
     * Provider which writes to files.
     */
    private static class FileWriterProvider implements WriterProvider {

        /**
         * Serializer which specifies strategy of serialization of data.
         */
        private Serializer serializer;

        /**
         * Base directory of resources
         */
        private Path basePath;

        /**
         * Creates provider with given settings.
         *
         * @param destination base directory
         * @param serializer strategy
         */
        public FileWriterProvider(Path destination, Serializer serializer) {
            this.basePath = destination;
            this.serializer = serializer;
        }

        /** {@inheritDoc} */
        @Override public Writer open(String name) throws IOException {
            Path filePath = basePath.resolve(Paths.get(name));
            return new FileWriter(filePath, serializer);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            // no-op
        }

    }

    private static class FileWriter implements Writer {

        /**
         * Path to specific resource.
         */
        private Path filePath;

        /**
         * Serializer which specifies strategy of serialization of data.
         */
        private Serializer serializer;

        /**
         * Key-value pairs which will be written by this writer.
         */
        private Map<Object, Object> buffer = new HashMap<>();

        /**
         * Creates writer with given settings.
         *
         * @param filePath to resource
         * @param serializer strategy
         */
        public FileWriter(Path filePath, Serializer serializer) {
            this.filePath = filePath;
            this.serializer = serializer;
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object value) throws IOException {
            buffer.put(key, value);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            Files.createDirectories(filePath.getParent());
            byte[] data = serializer.serialize(buffer).array();
            Files.write(filePath, data);
            FileChecksumHelper.writeChecksumForFile(filePath, data);
        }
    }

    private static class FileReaderProvider implements ReaderProvider {

        /**
         * Base directory of resources
         */
        private Path basePath;

        /**
         * Serializer which specifies strategy of serialization of data.
         */
        private Serializer serializer;

        /**
         * Creates provider with given settings.
         *
         * @param basePath base directory
         * @param serializer strategy
         */
        public FileReaderProvider(Path basePath, Serializer serializer) {
            this.basePath = basePath;
            this.serializer = serializer;
        }

        /** {@inheritDoc} */
        @Override public Iterable<String> getNames() {
            final List<String> result = new ArrayList<>();
            try {
                Files.walkFileTree(basePath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (isDataFile(file)) {
                            Path relativePath = basePath.relativize(file);
                            result.add(relativePath.toString());
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        return FileVisitResult.TERMINATE;
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return result;
        }

        /**
         * Checks if this file contains data for caches.
         *
         * @param file to check.
         * @return true if this is file with data. False if it is metadata or something else.
         */
        private boolean isDataFile(Path file) {
            boolean metadata = file.getFileName().toString().equals(ExportNameConventionUtil.METADATA_RESOURCE_NAME);
            boolean checksum = FileChecksumHelper.isChecksum(file);
            return !(metadata || checksum);
        }

        /** {@inheritDoc} */
        @Override public Reader open(String name) {
            Path filePath = basePath.resolve(Paths.get(name));
            return new FileReader(filePath, serializer);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            // no-op
        }
    }

    private static class FileReader implements Reader {

        /**
         * Path to specific resource.
         */
        private Path filePath;

        /**
         * Serializer which specifies strategy of serialization of data.
         */
        private Serializer serializer;

        /**
         * Creates reader with given settings.
         *
         * @param filePath to resource
         * @param serializer strategy
         */
        public FileReader(Path filePath, Serializer serializer) {
            this.filePath = filePath;
            this.serializer = serializer;
        }

        /** {@inheritDoc} */
        @Override public void read(IgniteInClosure<Map<Object, Object>> action) throws IOException {
            byte[] bytes = Files.readAllBytes(filePath);
            byte[] savedChecksum = FileChecksumHelper.readChecksumForFile(filePath);
            byte[] calculatedChecksum = FileChecksumHelper.calculateChecksum(bytes);
            if (!Arrays.equals(savedChecksum, calculatedChecksum)) {
                throw new RuntimeException("Checksum validation failed for " + filePath);
            }
            Map<Object, Object> deserialized = (Map<Object, Object>)serializer.deserialize(ByteBuffer.wrap(bytes));
            action.apply(deserialized);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            // no-op
        }
    }

}

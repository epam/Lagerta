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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

/**
 * Class which handles all the work with checksum calculation and storing.
 */
public class FileChecksumHelper {

    /**
     * Extension for checksum files.
     */
    private static final String CHECKSUM_EXT = ".checksum";

    /**
     * Checks if specified file contains checksum.
     *
     * @param file to check.
     * @return true if it is checksum, false otherwise.
     */
    public static boolean isChecksum(Path file) {
        return file.getFileName().toString().endsWith(CHECKSUM_EXT);
    }

    /**
     * Prepares and writes checksum for specified file.
     *
     * @param file to write checksum for.
     * @param content of this file.
     * @throws IOException when writing files fail.
     */
    public static void writeChecksumForFile(Path file, byte[] content) throws IOException {
        Path checksumFile = getChecksumFile(file);
        Files.write(checksumFile, calculateChecksum(content));
    }

    /**
     * Calculated checksum for given data.
     *
     * @param content of file.
     * @return CRC32 checksum.
     */
    public static byte[] calculateChecksum(byte[] content) {
        CRC32 crc = new CRC32();
        crc.update(content);
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
        return buffer.putLong(crc.getValue()).array();
    }

    /**
     * Reads checksum which validates specified file.
     *
     * @param file to validate.
     * @return checksum for that file.
     * @throws IOException if reading file fails.
     */
    public static byte[] readChecksumForFile(Path file) throws IOException {
        Path checksumFile = getChecksumFile(file);
        return Files.readAllBytes(checksumFile);
    }

    /**
     * Returns file which holds checksum information for given file.
     *
     * @param file to load checksum for.
     * @return checksum file.
     */
    private static Path getChecksumFile(Path file) {
        return file.resolveSibling(file.getFileName().toString() + CHECKSUM_EXT);
    }

}

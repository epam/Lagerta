/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lathgertha.resources;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * @author Evgeniy_Ignatiev
 * @since 14:41 03/22/2017
 */
public class TemporaryDirectory implements Resource {
    private static final String TEMP_DIR_PREFIX = "test-resources-";

    private Path baseDirPath;
    private File baseDir;

    @Override
    public void setUp() throws IOException {
        baseDirPath = Files.createTempDirectory(TEMP_DIR_PREFIX);
        baseDir = baseDirPath.toFile();
    }

    @Override
    public void tearDown() throws IOException {
        Files.walk(baseDirPath)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
    }

    public File mkSubDir(String name) {
        File tempDir = new File(baseDir, name);

        if (tempDir.mkdir()) {
            return tempDir;
        }
        throw new IllegalStateException("Failed to create directory " + tempDir.getAbsolutePath());
    }
}

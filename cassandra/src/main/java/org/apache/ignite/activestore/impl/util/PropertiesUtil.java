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

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author Evgeniy_Ignatiev
 * @since 12/1/2016 6:44 PM
 */
public final class PropertiesUtil {
    private PropertiesUtil() {
    }

    public static Properties excludeProperties(Properties properties, String... names) {
        Properties result = new Properties();
        Set<String> excludedNames = new HashSet<>();

        Collections.addAll(excludedNames, names);
        for (String name : properties.stringPropertyNames()) {
            if (!excludedNames.contains(name)) {
                result.setProperty(name, properties.getProperty(name));
            }
        }
        return result;
    }
}

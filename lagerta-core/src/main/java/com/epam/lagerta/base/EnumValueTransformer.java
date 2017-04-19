/*
 * Copyright 2017 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.lagerta.base;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EnumValueTransformer implements ValueTransformer {
    private static final Map<Class, EnumValueTransformer> MAP = new ConcurrentHashMap<>();

    private final Class<?> clazz;

    private EnumValueTransformer(Class<?> clazz) {
        this.clazz = clazz;
    }

    public static EnumValueTransformer of(Class<?> clazz) {
        return MAP.computeIfAbsent(clazz, EnumValueTransformer::new);
    }

    @Override
    public Object get(ResultSet resultSet, int index) throws SQLException {
        return clazz.getEnumConstants()[resultSet.getInt(index)];
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, Object value) throws SQLException {
        preparedStatement.setInt(index, ((Enum) value).ordinal());
    }

    @Override
    public String toString() {
        return "ENUM{" + clazz.getSimpleName() + '}';
    }
}

/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.util;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class JDBCKeyValueMapper {

    static final String KEY_FIELD_NAME = "key";
    static final String VAL_FIELD_NAME = "val";

    private static final Map<Class<?>, Class<?>> objectToPrimitiveMap = new HashMap<>();

    static {
        objectToPrimitiveMap.put(Integer.class, Integer.TYPE);
        objectToPrimitiveMap.put(Short.class, Short.TYPE);
        objectToPrimitiveMap.put(Byte.class, Byte.TYPE);
        objectToPrimitiveMap.put(Float.class, Float.TYPE);
        objectToPrimitiveMap.put(Double.class, Double.TYPE);
        objectToPrimitiveMap.put(Long.class, Long.TYPE);
        objectToPrimitiveMap.put(Boolean.class, Boolean.TYPE);
        objectToPrimitiveMap.put(Character.class, Character.TYPE);
    }

    private JDBCKeyValueMapper() {
    }

    /**
     * Create Key-Value mapper to Map<String, Object>
     * BinaryObject will be separated by its fields,
     * All other - as "val" -> value.this
     * Also, the result map contain key as "key" -> key.this
     * @param key is field name
     * @param value is field value
     * @return map of "fieldName" -> value
     */
    public static Map<String, Object> keyValueMap(Object key, Object value) {
        if (key == null || value == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new HashMap<>();
        result.put(KEY_FIELD_NAME, key);
        if (value instanceof BinaryObject) {
            result.putAll(mapBinaryObject((BinaryObject)value));
        } else {
            result.put(VAL_FIELD_NAME, value);
        }

        return result;
    }


    private static Map<String, Object> mapBinaryObject(BinaryObject binaryObject) {
        BinaryType type = binaryObject.type();
        Collection<String> fields = type.fieldNames();
        return fields.stream()
                .collect(Collectors.toMap(field -> field, binaryObject::field));
    }

    /**
     * Collection,arrays,maps and other objects - are serializable
     *
     * null, enum, primitive, string and date,
     * also all number bigInt bigDecimal etc - aren't serializable
     * @param value
     * @return
     */
    public static boolean shouldBeSerializedForDB(Object value) {
        if (value == null) {
            return false;
        }
        boolean isEnum = Enum.class.isAssignableFrom(value.getClass());
        boolean isNumber = value instanceof Number;
        boolean isString = value instanceof String || value instanceof Character;
        boolean isDate = value instanceof Date;

        return !(isEnum || isNumber || isString || isDate);
    }


    public static <T> T getObject(Map<String, Object> columnValues, Class<T> targetClass) {
        Object o = columnValues.get(VAL_FIELD_NAME);
        if (o != null) {
            if (getAsPrimitiveType(targetClass) == getAsPrimitiveType(o.getClass())) {
                return (T) o;
            }
            return targetClass.cast(o);
        } else {
            return getPOJOFromMapParams(columnValues, targetClass);
        }
    }

    private static <T> T getPOJOFromMapParams(Map<String, Object> columnValues, Class<T> targetClass) {
        Constructor<T> constructor = null;
        try {
            constructor = targetClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "No found default constructor(without parameters) for class " + targetClass.getName() + " It should be public", e);
        }
        T targetObject;
        try {
            targetObject = constructor.newInstance();
            for (String fieldName : columnValues.keySet()) {
                Object value = columnValues.get(fieldName);
                Field declaredField = targetClass.getDeclaredField(fieldName);
                declaredField.setAccessible(true);
                declaredField.set(targetObject, value);
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            throw new RuntimeException("Can not create new instance of " + targetClass.getSimpleName(), e);
        }
        return targetObject;
    }

    private static Class getAsPrimitiveType(Class clazz) {
        Class<?> o = objectToPrimitiveMap.get(clazz);
        if (o == null) {
            return clazz;
        }
        return o;
    }
}

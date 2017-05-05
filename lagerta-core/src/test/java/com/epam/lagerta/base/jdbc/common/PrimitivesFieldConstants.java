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

package com.epam.lagerta.base.jdbc.common;

import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.FieldDescriptor;
import com.epam.lagerta.base.jdbc.JDBCUtil;

import java.util.List;

import static com.epam.lagerta.base.SimpleValueTransformer.BOOLEAN;
import static com.epam.lagerta.base.SimpleValueTransformer.BYTE;
import static com.epam.lagerta.base.SimpleValueTransformer.DOUBLE;
import static com.epam.lagerta.base.SimpleValueTransformer.FLOAT;
import static com.epam.lagerta.base.SimpleValueTransformer.INTEGER;
import static com.epam.lagerta.base.SimpleValueTransformer.LONG;
import static com.epam.lagerta.base.SimpleValueTransformer.SHORT;
import static com.epam.lagerta.util.DataProviderUtil.list;

final class PrimitivesFieldConstants {
    static final String BOOLEAN_VALUE = "booleanValue";
    static final String BYTE_VALUE = "byteValue";
    static final String SHORT_VALUE = "shortValue";
    static final String INT_VALUE = "intValue";
    static final String LONG_VALUE = "longValue";
    static final String FLOAT_VALUE = "floatValue";
    static final String DOUBLE_VALUE = "doubleValue";

    private static final int KEY_INDEX = 1;
    private static final int VAL_INDEX = 2;
    private static final int BOOLEAN_VALUE_INDEX = 3;
    private static final int BYTE_VALUE_INDEX = 4;
    private static final int SHORT_VALUE_INDEX = 5;
    private static final int INT_VALUE_INDEX = 6;
    private static final int LONG_VALUE_INDEX = 7;
    private static final int FLOAT_VALUE_INDEX = 8;
    private static final int DOUBLE_VALUE_INDEX = 9;

    static final List<FieldDescriptor> FIELD_DESCRIPTORS = list(
            new FieldDescriptor(KEY_INDEX, EntityDescriptor.KEY_FIELD_NAME, INTEGER),
            new FieldDescriptor(VAL_INDEX, EntityDescriptor.VAL_FIELD_NAME, JDBCUtil.BLOB_TRANSFORMER),
            new FieldDescriptor(BOOLEAN_VALUE_INDEX, BOOLEAN_VALUE, BOOLEAN),
            new FieldDescriptor(BYTE_VALUE_INDEX, BYTE_VALUE, BYTE),
            new FieldDescriptor(SHORT_VALUE_INDEX, SHORT_VALUE, SHORT),
            new FieldDescriptor(INT_VALUE_INDEX, INT_VALUE, INTEGER),
            new FieldDescriptor(LONG_VALUE_INDEX, LONG_VALUE, LONG),
            new FieldDescriptor(FLOAT_VALUE_INDEX, FLOAT_VALUE, FLOAT),
            new FieldDescriptor(DOUBLE_VALUE_INDEX, DOUBLE_VALUE, DOUBLE));

}

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

package com.epam.lagerta.base.jdbc;

import com.epam.lagerta.base.EntityDescriptor;
import com.epam.lagerta.base.jdbc.common.OtherTypesHolder;
import com.epam.lagerta.base.jdbc.common.PrimitiveWrappersHolder;
import com.epam.lagerta.base.jdbc.common.PrimitivesHolder;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class EntityDescriptors {
    private static final Map<String, EntityDescriptor> ENTITY_DESCRIPTORS = ImmutableMap
            .<String, EntityDescriptor>builder()
            .put(PrimitivesHolder.CACHE, PrimitivesHolder.ENTITY_DESCRIPTOR)
            .put(PrimitivesHolder.BINARY_KEEPING_CACHE, PrimitivesHolder.ENTITY_DESCRIPTOR)
            .put(PrimitiveWrappersHolder.CACHE, PrimitiveWrappersHolder.ENTITY_DESCRIPTOR)
            .put(PrimitiveWrappersHolder.BINARY_KEEPING_CACHE, PrimitiveWrappersHolder.ENTITY_DESCRIPTOR)
            .put(OtherTypesHolder.CACHE, OtherTypesHolder.ENTITY_DESCRIPTOR)
            .put(OtherTypesHolder.BINARY_KEEPING_CACHE, OtherTypesHolder.ENTITY_DESCRIPTOR)
            .build();

    private EntityDescriptors() {
    }

    public static Map<String, EntityDescriptor> getEntityDescriptors() {
        return ENTITY_DESCRIPTORS;
    }
}

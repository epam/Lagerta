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

import com.epam.lagerta.base.jdbc.common.OtherTypesHolder;
import com.epam.lagerta.base.jdbc.common.PrimitiveWrappersHolder;
import com.epam.lagerta.base.jdbc.common.PrimitivesHolder;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

public class DataProviders {
    public static final String KV_META_PROVIDER = "kvMetaProvider";
    public static final String KV_META_LIST_PROVIDER = "kvMetaListProvider";

    public static final Set<Integer> KEYS = Sets.newHashSet(1, 2, 3);

    public static final PrimitivesHolder PH_1 =
            new PrimitivesHolder(false, (byte) 0, (short) 0, 0, 0, 0, 0);
    public static final PrimitivesHolder PH_2 =
            new PrimitivesHolder(true, (byte) 1, (short) 1, 1, 1, 1, 1);
    public static final PrimitiveWrappersHolder PWH_1 =
            new PrimitiveWrappersHolder(false, (byte) 0, (short) 0, 0, 0L, 0F, 0D);
    public static final PrimitiveWrappersHolder PWH_2 =
            new PrimitiveWrappersHolder(true, (byte) 1, (short) 1, 1, 1L, 1F, 1D);
    public static final PrimitiveWrappersHolder PWH_3 =
            new PrimitiveWrappersHolder(null, null, null, null, null, null, null);
    public static final OtherTypesHolder OTH_1 =
            new OtherTypesHolder(new byte[]{1}, new BigDecimal(1), new Date(), new Timestamp(1));
    public static final OtherTypesHolder OTH_2 =
            new OtherTypesHolder(null, null, null, null);

    // ToDo: Uncomment all commented out cases after fixes to issue #180.
    public static Object[][] provideKVMeta(Ignite ignite) {
        return new Object[][]{
                {PrimitivesHolder.withMetaData(1, PH_1)},
                {PrimitivesHolder.withMetaData(1, PH_2)},
                {PrimitivesHolder.withMetaData(1, binary(ignite, PH_1))},
                {PrimitivesHolder.withMetaData(1, binary(ignite, PH_2))},
                {PrimitiveWrappersHolder.withMetaData(1, PWH_1)},
                {PrimitiveWrappersHolder.withMetaData(2, PWH_2)},
                {PrimitiveWrappersHolder.withMetaData(3, PWH_3)},
                {PrimitiveWrappersHolder.withMetaData(1, binary(ignite, PWH_1))},
                {PrimitiveWrappersHolder.withMetaData(2, binary(ignite, PWH_2))},
                {PrimitiveWrappersHolder.withMetaData(3, binary(ignite, PWH_3))},
                {OtherTypesHolder.withMetaData(1, OTH_1)},
                {OtherTypesHolder.withMetaData(1, OTH_2)},
                //{OtherTypesHolder.withMetaData(1, binary(ignite, OTH_1))},
                {OtherTypesHolder.withMetaData(1, binary(ignite, OTH_2))}
        };
    }

    public static Object[][] provideKVMetaList(Ignite ignite) {
        return new Object[][]{
                {asList(PrimitivesHolder.withMetaData(1, PH_1),
                        PrimitivesHolder.withMetaData(2, PH_2))},
                {asList(PrimitivesHolder.withMetaData(1, binary(ignite, PH_1)),
                        PrimitivesHolder.withMetaData(2, binary(ignite, PH_2)))},
                {asList(PrimitiveWrappersHolder.withMetaData(1, PWH_1),
                        PrimitiveWrappersHolder.withMetaData(2, PWH_2),
                        PrimitiveWrappersHolder.withMetaData(3, PWH_3))},
                {asList(PrimitiveWrappersHolder.withMetaData(1, binary(ignite, PWH_1)),
                        PrimitiveWrappersHolder.withMetaData(2, binary(ignite, PWH_2)),
                        PrimitiveWrappersHolder.withMetaData(3, binary(ignite, PWH_3)))},
                {asList(OtherTypesHolder.withMetaData(1, OTH_1),
                        OtherTypesHolder.withMetaData(2, OTH_2))},
                {asList(//OtherTypesHolder.withMetaData(1, binary(ignite, OTH_1)),
                        OtherTypesHolder.withMetaData(2, binary(ignite, OTH_2)))}
        };
    }

    public static Object[][] provideDBModes() {
        return new Object[][]{{"REGULAR"}, {"POSTGRESQL"}};
    }

    public static Object[][] combineProviders(Object[][] leftProvider, Object[][] rightProvider) {
        List<Object[]> combination = new LinkedList<>();
        Arrays.stream(leftProvider)
                .forEach(objects1 -> Arrays.stream(rightProvider)
                        .forEach(objects2 -> combination.add(concatAll(objects1, objects2))));
        return combination.toArray(new Object[][]{});
    }

    @SafeVarargs
    private static <T> T[] concatAll(T[] first, T[]... rest) {
        Integer totalLenght = Arrays.stream(rest)
                .map(array -> array.length)
                .reduce(first.length, (left, right) -> left + right);
        T[] result = Arrays.copyOf(first, totalLenght);
        int offset = first.length;
        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    private DataProviders() {
    }

    private static <T> T binary(Ignite ignite, T obj) {
        return ignite.binary().toBinary(obj);
    }
}

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

package com.epam.lagerta.jmh.utils;

import com.epam.lagerta.capturer.TransactionScope;

import java.util.List;

import static com.epam.lagerta.jmh.utils.DataUtil.cacheScope;
import static com.epam.lagerta.jmh.utils.DataUtil.txScope;

public interface TransactionGenerator {

    default TransactionScope generate(long id) {
        cacheScope(DataUtil.CACHE_NAME, generateKeys(id));
        return txScope(id, cacheScope(DataUtil.CACHE_NAME, generateKeys(id)));
    }

    List generateKeys(long id);
}

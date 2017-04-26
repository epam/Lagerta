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

package com.epam.lagerta.resources;

import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.capturer.DataCapturerLoader;
import com.epam.lagerta.subscriber.Committer;

public final class DBResourceFactory {

    private static final String DB_NAME = "testDB";
    private static DBResource DB_RESOURCE = new DBResource(DB_NAME);

    public static DBResource getDBResource() {
        return DB_RESOURCE;
    }

    @SuppressWarnings("unused")
    public static Committer getJDBCCommitter() {
        return JDBCUtil.getJDBCCommitter(DB_RESOURCE.getDataSource());
    }

    @SuppressWarnings("unused")
    private static DataCapturerLoader getJDBCDataCapturerLoader() {
        return JDBCUtil.getJDBCDataCapturerLoader(DB_RESOURCE.getDataSource());
    }
}

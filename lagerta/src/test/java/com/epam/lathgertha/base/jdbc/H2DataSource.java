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

package com.epam.lathgertha.base.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;

public class H2DataSource extends BasicDataSource {

    public static H2DataSource create(String dbUrl) {
        H2DataSource dataSource = new H2DataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl(dbUrl);
        dataSource.setUsername("");
        dataSource.setPassword("");
        dataSource.setMaxTotal(8);
        return dataSource;
    }
}

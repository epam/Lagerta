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

package com.epam.lagerta.base.jdbc.committer;

import com.epam.lagerta.BaseFunctionalTest;
import com.epam.lagerta.base.jdbc.DataProviders;
import com.epam.lagerta.base.jdbc.JDBCUtil;
import com.epam.lagerta.resources.DBResource;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import javax.sql.DataSource;
import java.sql.SQLException;

public abstract class JDBCBaseFunctionalTest extends BaseFunctionalTest {
    private static final String DATA_BASE_NAME = "h2_functional_test";

    @DataProvider(name = DataProviders.KV_META_LIST_PROVIDER)
    public static Object[][] provideKVMetaList() {
        return DataProviders.provideKVMetaList(ignite);
    }

    private final DBResource dbResource = new DBResource(DATA_BASE_NAME);

    protected DataSource dataSource;

    @BeforeClass
    public void init() throws Exception {
        dbResource.setUp();
    }

    @AfterClass
    public void clean() throws Exception {
        dbResource.tearDown();
    }

    @BeforeMethod()
    public void initState() throws SQLException {
        dbResource.initState(JDBCUtil.CREATE_TABLE_SQL_RESOURCE);
        dataSource = dbResource.getDataSource();
    }

    @AfterMethod
    public void clearBase() throws SQLException {
        dbResource.clearState(JDBCUtil.DROP_TABLE_SQL_RESOURCE);
    }
}

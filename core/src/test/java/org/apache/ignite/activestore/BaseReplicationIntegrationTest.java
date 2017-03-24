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

package org.apache.ignite.activestore;

import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.rules.FullClusterTestResourcesFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;

/**
 * @author Aleksandr_Meterko
 * @since 12/14/2016
 */
public class BaseReplicationIntegrationTest {

    @ClassRule
    public static RuleChain allResourcesRule = FullClusterTestResourcesFactory.getAllResourcesRule(14 * 60_000);

    @Rule
    public RuleChain perTestMethodRules = FullClusterTestResourcesFactory.getPerTestMethodRules();

    protected static Ignite mainGrid() {
        return FullClusterTestResourcesFactory.getMainClusterResource().ignite();
    }

    protected static Ignite drGrid() {
        return FullClusterTestResourcesFactory.getDrClusterResource().ignite();
    }

}

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

package org.apache.ignite.load.subscriber;

import java.util.Collections;
import java.util.List;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/23/2017 7:54 PM
 */
public class IndependentTxGenerator extends TxMetadataGenerator {
    @Override protected List generateTxScope(long txId) {
        return Collections.singletonList(txId);
    }
}

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

package org.apache.ignite.activestore.commons.injection;

import org.apache.ignite.lang.IgniteCallable;

/**
 * @author Aleksandr_Meterko
 * @since 1/13/2017
 */
public abstract class ActiveStoreIgniteCallable<V> implements IgniteCallable<V> {

    @Override public V call() throws Exception {
        Injection.inject(this);
        return callInjected();
    }

    protected abstract V callInjected() throws Exception;

}

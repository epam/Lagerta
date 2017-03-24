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

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.commons.injection.Injection;

/**
 * @author Evgeniy_Ignatiev
 * @since 11:25 01/25/2017
 */
public class ListOfProviders<T> implements Provider<List<T>> {
    private final List<? extends Provider<T>> providers;

    @Inject
    private Ignite ignite;

    public ListOfProviders(List<? extends Provider<T>> providers) {
        this.providers = providers;
    }

    @Override public List<T> get() {
        for (Provider<T> provider : providers) {
            Injection.inject(provider, ignite);
        }
        List<T> result = new ArrayList<>(providers.size());

        for (Provider<T> provider : providers) {
            result.add(provider.get());
        }
        return result;
    }
}

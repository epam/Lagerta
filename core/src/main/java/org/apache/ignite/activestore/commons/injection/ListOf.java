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

import java.util.ArrayList;
import java.util.List;
import com.google.inject.Provider;
import javax.inject.Inject;
import org.apache.ignite.Ignite;

/**
 * @author Aleksandr_Meterko
 * @since 1/13/2017
 */
public class ListOf<T> implements Provider<List<T>> {
    private final List<Class<? extends T>> list;

    /** Auto-injected ignite instance. */
    @Inject
    private transient Ignite ignite;

    public ListOf(List<Class<? extends T>> list) {
        this.list = list;
    }

    @Override
    public List<T> get() {
        List<T> result = new ArrayList<>(list.size());
        DependencyContainer container = Injection.container(ignite);
        for (Class<? extends T> clazz : list) {
            result.add(container.get(clazz));
        }
        return result;
    }
}

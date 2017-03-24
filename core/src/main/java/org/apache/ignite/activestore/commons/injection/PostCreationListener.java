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

import java.util.Collection;
import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 * @author Aleksandr_Meterko
 * @since 1/12/2017
 */
class PostCreationListener implements TypeListener {

    private final LifecycleQueue lifecycleBeans;
    private final Ignite ignite;

    public PostCreationListener(LifecycleQueue lifecycleBeans, Ignite ignite) {
        this.lifecycleBeans = lifecycleBeans;
        this.ignite = ignite;
    }

    @Override public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
        encounter.register(new InjectionListener<I>() {
            @Override public void afterInjection(I injectee) {
                if (!(injectee instanceof Ignite)) {
                    Injection.postCreate(injectee, ignite);
                    extractLifecycle(injectee, lifecycleBeans);
                }
            }
        });
    }

    private void extractLifecycle(Object instance, LifecycleQueue lifecycleBeans) {
        boolean singleObject = !(instance instanceof Collection);
        if (singleObject) {
            addSingleInstance(instance, lifecycleBeans);
        } else {
            Collection collection = (Collection)instance;
            for (Object collInstance : collection) {
                addSingleInstance(collInstance, lifecycleBeans);
            }
        }
    }

    private void addSingleInstance(Object instance, LifecycleQueue lifecycleBeans) {
        if (instance instanceof LifecycleAware) {
            LifecycleAware lifecycleAware = (LifecycleAware)instance;
            lifecycleBeans.add(lifecycleAware);
        }
    }
};

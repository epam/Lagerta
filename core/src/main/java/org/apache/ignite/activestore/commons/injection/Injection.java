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

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.matcher.Matchers;
import com.google.inject.util.Modules;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.commons.BaseActiveStoreConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;

/**
 * Inner implementation of dependency injection container which utilizes Ignite mechanisms to distribute beans across
 * nodes. It works on top of Spring context providing access to both Spring beans and to own class bindings. Injection
 * container processes both {@link Inject} annotation and Ignite-specific annotations.
 */
public class Injection {

    private static final Logger LOGGER = LoggerFactory.getLogger(Injection.class);
    /**
     * Key in map which is used to lookup DI container.
     */
    public static final String CONTAINER = "INJECTION_CONTAINER";

    public static <T> T postCreate(T result, Ignite ignite) {
        result = DependencyContainer.injectIgnite(result, ignite);
        return result;
    }

    /**
     * Initializes all fields of specified instance which need injection.
     *
     * @param object to set fields to.
     * @param <T> instance of that class.
     * @return initialized instance.
     */
    static <T> T inject(T object) {
        return inject(object, Ignition.localIgnite());
    }

    /**
     * Initializes all fields of specified instance which need injection.
     *
     * @param object to set fields to.
     * @param ignite grid where this class is injected.
     * @param <T> instance of that class.
     * @return initialized instance.
     */
    public static <T> T inject(T object, Ignite ignite) {
        return container(ignite).injectForManuallyCreated(object);
    }

    /**
     * Provides injection container for specified grid.
     *
     * @param ignite grid to get container.
     * @return instance of container for grid.
     */
    @SuppressWarnings("unchecked")
    static DependencyContainer container(Ignite ignite) {
        Map<Object, Object> nodeLocalMap = ignite.cluster().nodeLocalMap();
        if (!nodeLocalMap.containsKey(CONTAINER)) {
            LOGGER.debug("[G] Initializing dependency container");
            Map<String, Object> userAttributes = ignite.cluster().localNode().attributes();
            BaseActiveStoreConfiguration config = (BaseActiveStoreConfiguration)
                userAttributes.get(BaseActiveStoreConfiguration.CONFIG_USER_ATTR);
            DependencyContainer container = create(ignite, config.createModule());
            nodeLocalMap.put(CONTAINER, DependencyContainer.injectIgnite(container, ignite));
        }
        return (DependencyContainer)nodeLocalMap.get(CONTAINER);
    }

    /**
     * Stops current container.
     *
     * @param ignite grid where container is used
     */
    public static void stop(Ignite ignite) {
        Map<String, Object> localMap = ignite.cluster().nodeLocalMap();
        DependencyContainer container = (DependencyContainer)localMap.remove(Injection.CONTAINER);

        if (container != null) {
            LOGGER.debug("[G] Stopping container");
            container.stop();
        }
    }

    private static DependencyContainer create(final Ignite ignite, Module configuration) {
        final LifecycleQueue lifecycleBeans = new LifecycleQueue();
        Module module = Modules.override(configuration).with(new AbstractModule() {
            @Override protected void configure() {
                bind(Ignite.class).toInstance(ignite);
                bindListener(Matchers.any(), new PostCreationListener(lifecycleBeans, ignite));
            }
        });
        return new DependencyContainer(module, lifecycleBeans);
    }
}

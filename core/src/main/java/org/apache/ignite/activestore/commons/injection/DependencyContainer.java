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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * @author Aleksandr_Meterko
 * @since 1/12/2017
 */
class DependencyContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DependencyContainer.class);

    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private Ignite ignite;
    /**
     * Auto-injected Spring context.
     */
    @SpringApplicationContextResource
    private ApplicationContext appContext;

    private final LifecycleQueue lifecycleQueue;

    private final Injector injector;

    /** */
    public DependencyContainer(Module module, LifecycleQueue lifecycleQueue) {
        injector = Guice.createInjector(module);
        this.lifecycleQueue = lifecycleQueue;
        lifecycleQueue.start();
    }

    protected <T> T get(Class<T> clazz) {
        T result = getFromSpring(clazz);
        if (result != null) {
            return result;
        }
        Key<T> key = Key.get(clazz);
        return injector.getInstance(key);
    }

    /**
     * Tries to get implementation of specified class from Spring context.
     *
     * @param <T> any type of your instance.
     * @param clazz to get implementation for.
     * @return bean from Spring or null if Spring is not used or there are errors on getting Spring bean.
     */
    private <T> T getFromSpring(Class<T> clazz) {
        T result = null;
        if (appContext != null) {
            try {
                result = appContext.getBean(clazz);
            }
            catch (Exception e) {
                // ignored
            }
        }
        LOGGER.debug("[G] Found {} instance in spring.", clazz);
        return result;
    }

    /**
     * Performs main injection logic of class fields.
     *
     * @param result to inject into.
     * @param <T> any type of your instance.
     * @return initialized instance.
     */
    public <T> T injectForManuallyCreated(T result) {
        injector.injectMembers(result);
        return injectIgnite(result, ignite);
    }

    /**
     * Stops lifecycle aware beans of this container.
     */
    public void stop() {
        lifecycleQueue.stop();
    }

    /**
     * Initializes only fields of specified instance which are marked with Ignite-specific annotations like {@link
     * IgniteInstanceResource}.
     *
     * @param object to set fields to.
     * @param ignite grid where this class is injected.
     * @param <T> instance of that class.
     * @return initialized instance.
     */
    public static <T> T injectIgnite(T object, Ignite ignite) {
        try {
            LOGGER.debug("[G] Injecting ignite into {}", object);
            IgniteKernal igniteKernal = (IgniteKernal)ignite;
            GridCacheSharedContext context = igniteKernal.context().cache().context();
            context.kernalContext().resource().injectGeneric(object);
            return object;
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException("Cannot inject ignite resources into " + object.getClass(), e);
        }
    }

}

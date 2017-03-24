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

package org.apache.ignite.activestore.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.activestore.ActiveCacheStore;
import org.apache.ignite.activestore.CommandService;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.ManagedMap;

/**
 * Base configuration class which wires necessary dependencies for specific underlying storage of {@link
 * ActiveCacheStore}.
 */
public abstract class BaseActiveStoreConfiguration implements Factory<Injection>, BeanFactoryPostProcessor,
    BeanNameAware, InitializingBean, Serializable {

    /**
     * Key in user attributes which contains initial configuration for container.
     */
    public static final String CONFIG_USER_ATTR = "injectionConfig";

    /**
     * Spring property name of user attributes on {@link IgniteConfiguration}.
     */
    private static final String USER_ATTRS_PROP_NAME = "userAttributes";

    /**
     * Factory for {@link ActiveCacheStore}.
     */
    private static final Factory ACTIVE_CACHE_STORE_FACTORY = new Factory<ActiveCacheStore>() {
        /**
         * Auto-injected ignite instance.
         */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** {@inheritDoc} */
        @Override public ActiveCacheStore create() {
            return Injection.create(ActiveCacheStore.class, ignite);
        }
    };

    /**
     * Binds generic class to factory which creates its implementation instance.
     */
    protected final Map<Class, Factory> factories = new HashMap<>();

    /**
     * Binds generic class to implementation.
     */
    protected final Map<Class, Class> bindings = new HashMap<>();

    /**
     * Binds generic class to factory which creates its implementation instance with special name.
     */
    protected final Map<Map.Entry<Class, String>, Factory> namedFactories = new HashMap<>();

    /**
     * Binds generic class to implementation with special name.
     */
    protected final Map<Map.Entry<Class, String>, Class> namedBindings = new HashMap<>();

    /**
     * Name of the bean which is used for this class in users Spring configuration.
     */
    private transient String beanName;

    /** {@inheritDoc} */
    @Override public Injection create() {
        return new Injection(factories, bindings, namedFactories, namedBindings);
    }

    /**
     * Obtains instance of factory {@link ActiveCacheStore}.
     *
     * @return instance of cache store factory to be set into cache configuration.
     */
    public <K, V> Factory<? extends CacheStore<? super K, ? super V>> activeCacheStoreFactory() {
        return ACTIVE_CACHE_STORE_FACTORY;
    }

    /**
     * Creates core service of active cache store to be deployed as cluster singleton.
     *
     * @return configuration for {@link IgniteConfiguration}.
     */
    public CommandService commandService() {
        return new CommandServiceImpl();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void postProcessBeanFactory(
        ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
        String[] igniteConfigNames = configurableListableBeanFactory.getBeanNamesForType(IgniteConfiguration.class);
        if (igniteConfigNames.length != 1) {
            throw new IllegalArgumentException("Spring config must contain exactly one ignite configuration!");
        }
        String[] activeStoreConfigNames = configurableListableBeanFactory.getBeanNamesForType(BaseActiveStoreConfiguration.class);
        if (activeStoreConfigNames.length != 1) {
            throw new IllegalArgumentException("Spring config must contain exactly one active store configuration!");
        }
        BeanDefinition igniteConfigDef = configurableListableBeanFactory.getBeanDefinition(igniteConfigNames[0]);
        MutablePropertyValues propertyValues = igniteConfigDef.getPropertyValues();
        if (!propertyValues.contains(USER_ATTRS_PROP_NAME)) {
            propertyValues.add(USER_ATTRS_PROP_NAME, new ManagedMap());
        }
        PropertyValue propertyValue = propertyValues.getPropertyValue(USER_ATTRS_PROP_NAME);
        Map userAttrs = (Map)propertyValue.getValue();
        TypedStringValue key = new TypedStringValue(CONFIG_USER_ATTR);
        RuntimeBeanReference value = new RuntimeBeanReference(beanName);
        userAttrs.put(key, value);
    }

    /** {@inheritDoc} */
    @Override public void setBeanName(String name) {
        this.beanName = name;
    }

}

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

package org.apache.ignite.activestore.commons;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.cache.configuration.Factory;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.activestore.impl.BaseActiveStoreConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Inner implementation of dependency injection container which utilizes Ignite mechanisms to distribute beans across
 * nodes. It works on top of Spring context providing access to both Spring beans and to own class bindings. Injection
 * container processes both {@link Inject} annotation and Ignite-specific annotations.
 */
public class Injection extends Container {
    /**
     * Key in map which is used to lookup DI container.
     */
    public static final String CONTAINER = "INJECTION_CONTAINER";
    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;
    /**
     * Specifies status of this container in Ignite lifecycle.
     */
    private boolean active;

    /** */
    public Injection(Map<Class, Factory> factories, Map<Class, Class> bindings,
                     Map<Map.Entry<Class, String>, Factory> namedFactories,
                     Map<Map.Entry<Class, String>, Class> namedBindings) {
        super(combine(factories, bindings, namedFactories, namedBindings));
        active = true;
    }

    private static Map<Id, Binding> combine(Map<Class, Factory> factories, Map<Class, Class> bindings,
                                            Map<Map.Entry<Class, String>, Factory> namedFactories,
                                            Map<Map.Entry<Class, String>, Class> namedBindings) {
        Map<Id, Binding> result = new HashMap<>(factories.size() + bindings.size() + namedFactories.size() +
                namedBindings.size());
        for (Map.Entry<Class, Factory> entry : factories.entrySet()) {
            result.put(new Id(entry.getKey(), null), new Binding(null, entry.getValue()));
        }
        for (Map.Entry<Class, Class> entry : bindings.entrySet()) {
            result.put(new Id(entry.getKey(), null), new Binding(entry.getValue(), null));
        }
        for (Map.Entry<Map.Entry<Class, String>, Factory> entry : namedFactories.entrySet()) {
            result.put(new Id(entry.getKey().getKey(), entry.getKey().getValue()), new Binding(null, entry.getValue()));
        }
        for (Map.Entry<Map.Entry<Class, String>, Class> entry : namedBindings.entrySet()) {
            result.put(new Id(entry.getKey().getKey(), entry.getKey().getValue()), new Binding(entry.getValue(), null));
        }
        return result;
    }

    /**
     * Gets existing implementation instance of specified class's implementation or creates new.
     *
     * @param clazz genetic class to get.
     * @param ignite grid where this class needs to be instantiated.
     * @param <T> any type of your instance.
     * @return instance of that class.
     */
    public static <T> T get(Class<T> clazz, Ignite ignite) {
        return container(ignite).get(clazz, (String) null);
    }

    /**
     * Creates new instance of specified class's implementation.
     *
     * @param clazz generic class to create.
     * @param ignite grid where this class needs to be instantiated.
     * @param <T> any type of your instance.
     * @return new instance of that class.
     */
    public static <T> T create(Class<T> clazz, Ignite ignite) {
        return container(ignite).create(clazz);
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
        return container(ignite).inject(object, false);
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
            IgniteKernal igniteKernal = (IgniteKernal)ignite;
            GridCacheSharedContext context = igniteKernal.context().cache().context();
            context.kernalContext().resource().injectGeneric(object);
            return object;
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException("Cannot inject ignite resources into " + object.getClass(), e);
        }
    }

    /**
     * Checks if container on specific node is active within Ignite lifecycle.
     *
     * @param ignite node to check container.
     * @return true if container is active and can be used, false otherwise.
     */
    public static boolean isActive(Ignite ignite) {
        return container(ignite).isActive();
    }

    /**
     * Provides injection container for specified grid.
     *
     * @param ignite grid to get container.
     * @return instance of container for grid.
     */
    @SuppressWarnings("unchecked")
    private static Injection container(Ignite ignite) {
        Map<Object, Object> nodeLocalMap = ignite.cluster().nodeLocalMap();
        if (!nodeLocalMap.containsKey(CONTAINER)) {
            Map<String, Object> userAttributes = ignite.cluster().localNode().attributes();
            Factory<Injection> factory = (Factory<Injection>)userAttributes.get(BaseActiveStoreConfiguration.CONFIG_USER_ATTR);
            nodeLocalMap.put(CONTAINER, injectIgnite(factory.create(), ignite));
        }
        return (Injection)nodeLocalMap.get(CONTAINER);
    }

    /**
     * Finds appropriate constructor which is marked with {@link Inject} annotations or no-arg one.
     *
     * @param clazz to get constructor for.
     * @param <T> any type of your instance.
     * @return constructor for creation.
     */
    private static <T> Constructor<?> findConstructor(Class<T> clazz) {
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (constructor.getAnnotation(Inject.class) != null) {
                return constructor;
            }
        }
        try {
            return clazz.getDeclaredConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Cannot find default constructor or constructor with Inject for " + clazz, e);
        }
    }

    /**
     * Marks that in current lifecycle container should be stopped.
     */
    public void stop() {
        for (Object o : beans.values()) {
            if (o instanceof LifecycleAware) {
                ((LifecycleAware)o).stop();
            }
        }
        active = false;
    }

    /**
     * Check if current container instance is active in lifecycle.
     *
     * @return true if active, false otherwise.
     */
    private boolean isActive() {
        return active;
    }

    @Override
    protected Object createByClass(Class clazz) {
        Object result;
        Constructor<?> constructor = findConstructor(clazz);
        try {
            result = constructor.newInstance(getInput(constructor.getParameterTypes(), constructor.getParameterAnnotations()));
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Cannot find appropriate constructor for " + clazz, e);
        }
        return inject(result, true);
    }

    @Override
    protected Object createByFactory(Factory factory) {
        inject(factory, false);
        Object result = factory.create();
        return inject(result, true);
    }

    /**
     * Performs main injection logic of class fields.
     *
     * @param result to inject into.
     * @param callPostConstruct true if methods annotated with {@link PostConstruct} should be called after instance
     * creation.
     * @param <T> any type of your instance.
     * @return initialized instance.
     */
    private <T> T inject(T result, boolean callPostConstruct) {
        Class<?> clazz = result.getClass();
        Deque<Method> postConstructs = new ArrayDeque<>();
        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                if (field.getAnnotation(Inject.class) != null) {
                    if (Modifier.isFinal(field.getModifiers())) {
                        throw new RuntimeException("Field " + field.getName() + " in " + clazz + " is final.");
                    }
                    field.setAccessible(true);
                    Qualifier qualifier = field.getAnnotation(Qualifier.class);
                    String name = qualifier == null ? null : qualifier.value();
                    try {
                        field.set(result, get(field.getType(), name));
                    }
                    catch (IllegalAccessException e) {
                        throw new RuntimeException("Cannot inject value into field " + field.getName() + " in class " + clazz, e);
                    }
                }
            }
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getAnnotation(Inject.class) != null) {
                    Class[] parameterTypes = method.getParameterTypes();
                    if (parameterTypes.length > 0) {
                        method.setAccessible(true);
                        try {
                            method.invoke(result, getInput(parameterTypes, method.getParameterAnnotations()));
                        }
                        catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException("Cannot inject value into method " + method.getName() + " in class " + clazz, e);
                        }
                    }
                }
                else if (callPostConstruct && method.getAnnotation(PostConstruct.class) != null) {
                    if (method.getParameterTypes().length == 0) {
                        method.setAccessible(true);
                        postConstructs.addFirst(method);
                    }
                }
            }
            clazz = clazz.getSuperclass();
        }
        injectIgnite(result, ignite);
        for (Method postConstruct : postConstructs) {
            try {
                postConstruct.invoke(result);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Cannot execute PostConstruct method", e);
            }
        }
        if (callPostConstruct && result instanceof LifecycleAware) {
            ((LifecycleAware)result).start();
        }
        return result;
    }

    /**
     * Gets intances which will match specified input parameters for method call.
     *
     * @param types to match.
     * @param annotations parameters annotations
     * @return instances.
     */
    private Object[] getInput(Class<?>[] types, Annotation[][] annotations) {
        Object[] input = new Object[types.length];
        String[] names = extractNames(annotations);
        for (int i = 0; i < types.length; i++) {
            input[i] = get(types[i], names[i]);
        }
        return input;
    }

    private String[] extractNames(Annotation[][] annotationss) {
        String[] result = new String[annotationss.length];
        for (int i = 0; i < annotationss.length; i++) {
            Annotation[] annotations = annotationss[i];
            String name = null;
            for (Annotation annotation : annotations) {
                if (annotation instanceof Qualifier) {
                    name = ((Qualifier) annotation).value();
                    break;
                }

            }
            result[i] = name;
        }
        return result;
    }

    public static class ListOf<T> implements Factory<List<T>> {
        private final List<Class<? extends T>> list;

        /** Auto-injected ignite instance. */
        @IgniteInstanceResource
        private transient Ignite ignite;

        public ListOf(List<Class<? extends T>> list) {
            this.list = list;
        }

        @Override
        public List<T> create() {
            List<T> result = new ArrayList<>(list.size());
            for (Class<? extends T> clazz : list) {
                result.add(Injection.get(clazz, ignite));
            }
            return result;
        }
    }
}

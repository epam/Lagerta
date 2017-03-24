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

import java.util.HashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.springframework.context.ApplicationContext;

/**
 * @author Andrei_Yakushin
 * @since 10/31/2016 11:01 AM
 */
abstract class Container {

    private final Map<Id, Binding> bindings;

    protected final Map<Id, Object> beans;

    /**
     * Auto-injected Spring context.
     */
    @SpringApplicationContextResource
    private transient ApplicationContext appContext;

    public Container(Map<Id, Binding> bindings) {
        this.bindings = bindings;
        this.beans = new HashMap<>();
    }

    protected  <T> T get(Class<T> clazz, String name) {
        T result = getFromSpring(clazz, name);
        if (result != null) {
            return result;
        }
        Id id = getFinalId(new Id(clazz, name));
        result = (T)beans.get(id);
        if (result == null) {
            synchronized (this) {
                result = (T)beans.get(id);
                if (result == null) {
                    beans.put(id, result = create(id));
                }
            }
        }
        return result;
    }

    protected  <T> T create(Class<T> clazz) {
        T result = getFromSpring(clazz, null);
        return result == null ? (T) create(getFinalId(new Id(clazz, null))) : result;
    }

    private Id getFinalId(Id original) {
        Id result = original;
        Binding current = bindings.get(result);
        while (current != null) {
            if (current.factory == null) {
                result = new Id(current.clazz, null);
                current = bindings.get(result);
            } else {
                break;
            }
        }
        return result;
    }

    private <T> T create(Id id) {
        Binding binding = bindings.get(id);
        return binding == null ? (T) createByClass(id.clazz) : (T)createByFactory(binding.factory);
    }

    protected abstract Object createByClass(Class clazz);

    protected abstract Object createByFactory(Factory factory);


    /**
     * Tries to get implementation of specified class from Spring context.
     *
     * @param <T> any type of your instance.
     * @param clazz to get implementation for.
     * @param name bean name, optional argument
     * @return bean from Spring or null if Spring is not used or there are errors on getting Spring bean.
     */
    private <T> T getFromSpring(Class<T> clazz, String name) {
        T result = null;
        if (appContext != null) {
            try {
                result = name == null ? appContext.getBean(clazz) : appContext.getBean(name, clazz);
            }
            catch (Exception e) {
                // ignored
            }
        }
        return result;
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class Binding {
        public final Class clazz;
        public final Factory factory;

        public Binding(Class clazz, Factory factory) {
            this.clazz = clazz;
            this.factory = factory;
        }

        @Override public String toString() {
            return "Binding{" +
                "clazz=" + clazz +
                ", factory=" + factory +
                '}';
        }
    }

    public static class Id {
        public final Class clazz;
        public final String name;

        public Id(Class clazz, String name) {
            this.clazz = clazz;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Id id = (Id) o;
            return clazz.equals(id.clazz) && (name != null ? name.equals(id.name) : id.name == null);
        }

        @Override
        public int hashCode() {
            return 31 * clazz.hashCode() + (name != null ? name.hashCode() : 0);
        }

        @Override public String toString() {
            return "Id{" +
                "clazz=" + clazz +
                ", name='" + name + '\'' +
                '}';
        }
    }
}

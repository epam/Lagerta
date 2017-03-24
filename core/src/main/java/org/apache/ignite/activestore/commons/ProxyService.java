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

import java.lang.reflect.Proxy;
import com.google.inject.Provider;
import org.apache.ignite.Ignite;
import org.apache.ignite.services.Service;

/**
 * @author Andrei_Yakushin
 * @since 1/19/2017 5:27 PM
 */
public class ProxyService<T extends Service> implements Provider<T> {
    private final Ignite ignite;
    private final Class<T> clazz;
    private final String name;

    private T service;

    public ProxyService(Ignite ignite, Class<T> clazz, String name) {
        this.ignite = ignite;
        this.clazz = clazz;
        this.name = name;
    }

    @Override public T get() {
        if (service == null || !Proxy.isProxyClass(service.getClass())) {
            service = ignite.services().serviceProxy(name, clazz, false);
        }
        return service;
    }
}

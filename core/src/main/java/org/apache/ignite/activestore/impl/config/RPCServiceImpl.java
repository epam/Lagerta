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

package org.apache.ignite.activestore.impl.config;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jws.WebService;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import org.apache.ignite.activestore.commons.EndpointUtils;
import org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreServiceImpl;

/**
 * @author Andrei_Yakushin
 * @since 12/13/2016 5:06 PM
 */
public class RPCServiceImpl implements RPCService {
    private static final Map<Class, QName> MAPPING = createMapping(
        SimpleSuperCluster.class,
        ActiveCacheStoreServiceImpl.class
    );

    private static final String MAGIC_STRING = "Service";

    private final String address;

    public RPCServiceImpl(String address) {
        this.address = address;
    }

    @Override
    public <T> T get(Class<T> clazz, String name) {
        QName qName = MAPPING.get(clazz);
        if (qName == null) {
            return null;
        }
        try {
            URL url = new URL(EndpointUtils.formatEndpoint(address, name));
            return Service.create(url, qName).getPort(clazz);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Class, QName> createMapping(Class... implementations) {
        Map<Class, QName> result = new HashMap<>(implementations.length);
        for (Class implementation : implementations) {
            Deque<Class> toProcess = new ArrayDeque<>();
            toProcess.add(implementation);
            Class webServiceInterface = null;
            do {
                Class clazz = toProcess.poll();
                if (clazz.getAnnotation(WebService.class) != null && clazz.isInterface()) {
                    webServiceInterface = clazz;
                    break;
                }
                if (clazz.getSuperclass() != null) {
                    toProcess.add(clazz.getSuperclass());
                }
                if (clazz.getInterfaces() != null) {
                    toProcess.addAll(Arrays.asList(clazz.getInterfaces()));
                }
            }
            while (!toProcess.isEmpty());
            if (webServiceInterface != null) {
                WebService webService = (WebService)webServiceInterface.getAnnotation(WebService.class);
                String targetNamespace = webService.targetNamespace();
                if (targetNamespace.isEmpty()) {
                    targetNamespace = getDefaultTargetNamespace(implementation);
                }
                String serviceName = webService.serviceName();
                if (serviceName.isEmpty()) {
                    serviceName = getDefaultServiceName(implementation);
                }
                result.put(webServiceInterface, new QName(targetNamespace, serviceName));
            }
        }
        return result;
    }

    private static String getDefaultTargetNamespace(Class clazz) {
        List<String> parts = Arrays.asList(clazz.getPackage().getName().split("\\."));
        Collections.reverse(parts);
        StringBuilder builder = new StringBuilder("http://");
        boolean first = true;
        for (String part : parts) {
            if (first) {
                first = false;
            }
            else {
                builder.append(".");
            }
            builder.append(part);
        }
        builder.append("/");
        return builder.toString();
    }

    private static String getDefaultServiceName(Class clazz) {
        return clazz.getSimpleName() + MAGIC_STRING;
    }
}

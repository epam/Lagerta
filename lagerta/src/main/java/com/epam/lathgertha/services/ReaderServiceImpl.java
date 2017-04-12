/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lathgertha.services;

import com.epam.lathgertha.subscriber.Reader;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.services.ServiceContext;
import org.springframework.context.ApplicationContext;

public class ReaderServiceImpl implements ReaderService {
    @SpringApplicationContextResource
    private transient ApplicationContext context;

    private transient Reader reader;

    @Override
    public void cancel(ServiceContext ctx) {
        if (reader != null) {
            reader.stop();
        }
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        //do nothing
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        if (context != null) {
            reader = context.getBean(Reader.class);
            reader.execute();
        }
    }

    @Override
    public void resendReadTransactions() {
        if (reader != null) {
            reader.resendReadTransactions();
        }
    }
}
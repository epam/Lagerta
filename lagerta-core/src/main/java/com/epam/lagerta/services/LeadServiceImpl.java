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
package com.epam.lagerta.services;

import com.epam.lagerta.capturer.TransactionScope;
import com.epam.lagerta.subscriber.lead.Lead;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.services.ServiceContext;
import org.springframework.context.ApplicationContext;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LeadServiceImpl implements LeadService {
    @SpringApplicationContextResource
    private transient ApplicationContext context;

    private transient Lead lead;

    @Override
    public void cancel(ServiceContext ctx) {
        if (lead != null) {
            lead.stop();
        }
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        //do nothing
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        if (context != null) {
            lead = context.getBean(Lead.class);
            lead.execute();
        }
    }

    @Override
    public List<Long> notifyRead(UUID readerId, List<TransactionScope> txScopes) {
        return lead == null ? Collections.emptyList() : lead.notifyRead(readerId, txScopes);
    }

    @Override
    public void notifyCommitted(UUID readerId, List<Long> ids) {
        if (lead != null) {
            lead.notifyCommitted(readerId, ids);
        }
    }

    @Override
    public void notifyFailed(UUID readerId, Long id) {
        if (lead != null) {
            lead.notifyFailed(readerId, id);
        }
    }

    @Override
    public long getLastDenseCommitted() {
        return lead == null ? -1 : lead.getLastDenseCommitted();
    }
}
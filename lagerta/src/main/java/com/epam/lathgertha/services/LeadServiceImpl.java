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

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.subscriber.lead.Lead;
import com.epam.lathgertha.subscriber.lead.LeadImpl;
import org.apache.ignite.services.ServiceContext;

import java.util.List;
import java.util.UUID;

public class LeadServiceImpl implements LeadService {

    private transient Lead lead;

    @Override
    public void cancel(ServiceContext ctx) {
        lead.stop();
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        lead = new LeadImpl();
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        lead.execute();
    }

    @Override
    public List<Long> notifyRead(UUID consumerId, List<TransactionScope> txScopes) {
        return lead.notifyRead(consumerId, txScopes);
    }

    @Override
    public void notifyCommitted(List<Long> ids) {
        lead.notifyCommitted(ids);
    }

    @Override
    public void notifyFailed(Long id) {
        lead.notifyFailed(id);
    }
}
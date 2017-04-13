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

package org.apache.ignite.load.subscriber;

import org.apache.ignite.activestore.impl.subscriber.lead.Lead;
import org.apache.ignite.activestore.impl.subscriber.lead.LeadResponse;
import org.eclipse.collections.api.list.primitive.LongList;

import javax.inject.Inject;
import java.util.UUID;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/24/2017 1:11 PM
 */
public class ImmediateCommitResponseProcessor implements LeadResponseProcessor {
    private final Lead lead;

    @Inject
    public ImmediateCommitResponseProcessor(Lead lead) {
        this.lead = lead;
    }

    @Override public void processResponse(UUID consumerId, LeadResponse response) {
        LongList ids = response.getToCommitIds();

        if (ids != null) {
            lead.notifyTransactionsCommitted(consumerId, ids);
        }
    }
}

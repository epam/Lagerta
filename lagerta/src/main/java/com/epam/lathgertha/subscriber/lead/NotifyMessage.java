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

package com.epam.lathgertha.subscriber.lead;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NotifyMessage implements Serializable {
    static final NotifyMessage EMPTY_NOTIFY_MESSAGE = new NotifyMessage(Collections.emptyList(), Collections.emptyList());

    private final List<Long> toCommit;
    private final List<Long> toRemove;

    public NotifyMessage() {
        this(new ArrayList<Long>(), new ArrayList<Long>());
    }

    private NotifyMessage(List<Long> notifyTxsId, List<Long> removeTxsId) {
        this.toCommit = notifyTxsId;
        this.toRemove = removeTxsId;
    }

    public List<Long> getToCommit() {
        return toCommit;
    }

    public List<Long> getToRemove() {
        return toRemove;
    }

    public NotifyMessage append(NotifyMessage other) {
        toCommit.addAll(other.toCommit);
        toRemove.addAll(other.toRemove);
        return this;
    }

    //reader side
    public void sort() {
        Collections.sort(toCommit);
    }

    public void add(Long id) {
        toCommit.add(id);
    }

}

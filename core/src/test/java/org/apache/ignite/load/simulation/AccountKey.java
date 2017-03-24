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

package org.apache.ignite.load.simulation;

import java.io.Serializable;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/** */
public class AccountKey implements Serializable {
    /** */
    private final int accountId;

    /** */
    @AffinityKeyMapped
    private final int partition;

    /** */
    public AccountKey(int accountId, int partition) {
        this.accountId = accountId;
        this.partition = partition;
    }

    /** */
    public int getAccountId() {
        return accountId;
    }

    /** */
    public int getPartition() {
        return partition;
    }

    public int hashCode() {
        return accountId;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AccountKey)) {
            return false;
        }
        return accountId == (((AccountKey) obj).accountId);
    }
}

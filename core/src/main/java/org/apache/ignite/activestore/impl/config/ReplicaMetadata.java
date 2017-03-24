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

import java.io.Serializable;

import org.apache.ignite.activestore.impl.config.ReplicaConfig;

/**
 * @author Evgeniy_Ignatiev
 * @since 2/1/2017 2:29 PM
 */
public class ReplicaMetadata implements Serializable {
    private final ReplicaConfig config;
    private final boolean outOfOrder;

    public ReplicaMetadata(ReplicaConfig config, boolean outOfOrder) {
        this.config = config;
        this.outOfOrder = outOfOrder;
    }

    public ReplicaConfig config() {
        return config;
    }

    public boolean isOutOfOrder() {
        return outOfOrder;
    }
}

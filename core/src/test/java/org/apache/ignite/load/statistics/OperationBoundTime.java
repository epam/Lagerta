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

package org.apache.ignite.load.statistics;

import java.io.Serializable;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/11/2017 4:14 PM
 */
public class OperationBoundTime implements Serializable {
    private final long operationId;
    private final long time;
    private final OperationBoundType boundType;

    public OperationBoundTime(long operationId, long time, OperationBoundType boundType) {
        this.operationId = operationId;
        this.time = time;
        this.boundType = boundType;
    }

    public long getOperationId() {
        return operationId;
    }

    public long getTime() {
        return time;
    }

    public OperationBoundType getBoundType() {
        return boundType;
    }
}

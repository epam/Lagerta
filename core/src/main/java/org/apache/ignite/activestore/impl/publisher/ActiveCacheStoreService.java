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

package org.apache.ignite.activestore.impl.publisher;

import java.util.UUID;
import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;

/**
 * @author Andrei_Yakushin
 * @since 12/6/2016 11:16 AM
 */

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface ActiveCacheStoreService {
    String NAME = "register_dr";

    @WebMethod
    void register(UUID replicaId, ReplicaConfig replicaConfig);

    @WebMethod
    void registerAll(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId);

    @WebMethod
    int ping();

    @WebMethod
    void startReconciliation(UUID replicaId, long startTransactionId, long endTransactionId);

    @WebMethod
    void stopReconciliation(UUID replicaId);

    @WebMethod
    long lastProcessedTxId();
}

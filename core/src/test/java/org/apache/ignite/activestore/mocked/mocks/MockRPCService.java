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

package org.apache.ignite.activestore.mocked.mocks;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;
import org.apache.ignite.activestore.impl.config.RPCService;
import org.apache.ignite.activestore.impl.config.ReplicaConfig;
import org.apache.ignite.activestore.impl.publisher.ActiveCacheStoreService;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

/**
 * @author Aleksandr_Meterko
 * @since 1/17/2017
 */
@Singleton
public class MockRPCService implements RPCService, LifecycleAware {
    private volatile Map<Class, Object> services;

    @Override
    public void start() {
        services = new ConcurrentHashMap<>();
        services.put(ActiveCacheStoreService.class, new NormalMain());
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override public <T> T get(Class<T> clazz, @Nullable String name) {
        return (T)services.get(clazz);
    }

    public void crashMain() {
        services.put(ActiveCacheStoreService.class, new CrashedMain());
    }

    public void restoreMain() {
        services.put(ActiveCacheStoreService.class, new NormalMain());
    }

    public boolean reconcilliationWasCalled() {
        SpyReconciler reconciler = (SpyReconciler)services.get(ActiveCacheStoreService.class);
        return reconciler.isReconcillitaionCalled();
    }

    private abstract static class SpyReconciler implements ActiveCacheStoreService {

        private boolean reconcillitaionCalled;

        public boolean isReconcillitaionCalled() {
            return reconcillitaionCalled;
        }

        @Override public void startReconciliation(UUID replicaId, long startTransactionId, long endTransactionId) {
            reconcillitaionCalled = true;
        }

        @Override public void stopReconciliation(UUID replicaId) {
            // no-op
        }

        @Override public long lastProcessedTxId() {
            return 0;
        }
    }

    private static class NormalMain extends SpyReconciler {
        @Override public void register(UUID replicaId, ReplicaConfig replicaConfig) {
            // no-op
        }

        @Override public void registerAll(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId) {
            // no-op
        }

        @Override public int ping() {
            return 0;
        }
    }

    private static class CrashedMain extends SpyReconciler {

        @Override public void register(UUID replicaId, ReplicaConfig replicaConfig) {
            // no-op
        }

        @Override public void registerAll(UUID[] ids, ReplicaConfig[] configs, UUID mainClusterId) {
            // no-op
        }

        @Override public int ping() {
            throw new RuntimeException();
        }
    }


}

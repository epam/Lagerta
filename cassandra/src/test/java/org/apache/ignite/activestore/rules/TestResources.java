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

package org.apache.ignite.activestore.rules;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.CommandService;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.IdSequencer;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.Tree;
import org.apache.ignite.activestore.cluster.IgniteClusterManager;
import org.apache.ignite.activestore.cluster.OneProcessClusterManager;
import org.apache.ignite.activestore.config.InMemoryActiveStoreConfiguration;
import org.apache.ignite.activestore.impl.BaseActiveStoreConfiguration;
import org.apache.ignite.activestore.impl.CommandServiceImpl;
import org.apache.ignite.activestore.impl.InMemoryMetadataManager;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.activestore.impl.util.MetadataAtomicsHelper;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.activestore.impl.BaseActiveStoreConfiguration.CONFIG_USER_ATTR;

/**
 * Core class for tests which performs set up and tear down of cluster and exposes some useful methods.
 */
public class TestResources extends MeteredResource {
    /**
     * Number of nodes.
     */
    private static final int NODES = 3;

    /**
     * Root node which is used for submitting tasks.
     */
    private Ignite root;

    /**
     * Cluster manager to use in tests.
     */
    private IgniteClusterManager clusterManager = new OneProcessClusterManager();

    /**
     * Configuration for active store.
     */
    private BaseActiveStoreConfiguration activeStoreConfiguration = new InMemoryActiveStoreConfiguration();

    /**
     * Action which performs tear down logic.
     */
    private Runnable tearDownAfterTestAction = new Runnable() {
        @Override public void run() {
            InMemoryMetadataManager.cleanup();
        }
    };

    /**
     * Sets action for tear down.
     */
    public void setTearDownAfterTestAction(Runnable tearDownAfterTestAction) {
        this.tearDownAfterTestAction = tearDownAfterTestAction;
    }

    /**
     * Calls tear down action.
     */
    public void callSpecificTearDown() {
        if (tearDownAfterTestAction != null) {
            tearDownAfterTestAction.run();
        }
    }

    /**
     * Set configuration of active store.
     */
    public void setActiveStoreConfiguration(BaseActiveStoreConfiguration activeStoreConfiguration) {
        this.activeStoreConfiguration = activeStoreConfiguration;
    }

    /**
     * Set cluster manager.
     */
    public void setClusterManager(IgniteClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Deploys used services synchronously.
     */
    public void deployServices() {
        ignite().compute().broadcast(new IgniteRunnable() {
            /** Auto-injected ignite instance. */
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                ignite.cluster().nodeLocalMap().remove(Injection.CONTAINER);
            }
        });
        ignite().services().deployClusterSingleton(CommandService.SERVICE_NAME, activeStoreConfiguration.commandService());

        do {
            try {
                sleep(100);
            }
            catch (InterruptedException e) {
                break;
            }
        }
        while (head() == null);
    }

    /**
     * Cancels used services.
     */
    public void cancelServices() {
        ignite().compute().broadcast(new IgniteRunnable() {
            /** Auto-injected ignite instance. */
            @IgniteInstanceResource
            private transient Ignite ignite;

            @Override public void run() {
                Injection container = (Injection)ignite.cluster().nodeLocalMap().get(Injection.CONTAINER);
                if (container != null) {
                    container.stop();
                }
            }
        });
        ignite().services().cancel(CommandService.SERVICE_NAME);
    }

    /**
     * Creates new snapshot with give label.
     *
     * @param label to associate with snapshot. Must be unique.
     */
    public void createSnapshot(String label) {
        getCommandService().createSnapshot(label);
    }

    /**
     * Performs rollback of snapshot tree and data to specified snapshot. It should fail if there are running user
     * transactions as this operation modifies metadata head.
     *
     * @param label of snapshot to which we should rollback.
     */
    public void rollback(String label) {
        getCommandService().rollback(label);
    }

    /**
     * Merges all snapshots withing given path.
     *
     * @param from label of snapshot where to start merging (exclusive).
     * @param to label of snapshot where to end merging (inclusive).
     */
    public void merge(String from, String to) {
        getCommandService().merge(from, to);
    }

    /**
     * Returns whole metadata tree.
     *
     * @return tree.
     */
    public Tree metadataTree() {
        return getCommandService().metadataTree();
    }

    /**
     * Returns proxy to {@link CommandService}
     */
    private CommandService getCommandService() {
        return Ignition.localIgnite().services().serviceProxy(CommandService.SERVICE_NAME, CommandService.class, true);
    }

    /**
     * Performs restore operation on all data which is contained in specified backup. This loads all metadata and data
     * and invalidates all affected data in caches, so it will be properly loaded from underlying storage.
     *
     * @param source where previous backup was saved.
     */
    public void restore(URI source) {
        try {
            getCommandService().restore(source);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns current metadata head.
     */
    public Metadata head() {
        Reference<Metadata> head = MetadataAtomicsHelper.getReference(ignite(), CommandServiceImpl.HEAD_ATOMIC_NAME, false);
        return head == null ? null : head.get();
    }

    /**
     * Returns randomly generated snapshot label obtained as string representation of a randomly generated UUID
     * with dashes substituted with underscores.
     */
    private String generateRandomLabel() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    /**
     * Backs up all metadata and data. This leads to saving inner state of underlying storage into some independent
     * format which is specified by {@link Exporter}.
     *
     * @param destination to where backup should be saved.
     */
    public void backup(URI destination) {
        try {
            getCommandService().backup(destination, generateRandomLabel());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs backup of all metadata and partial backup of data. It backs up only data which is newer than given
     * snapsht. This leads to saving inner state of underlying storage into some independent format which is specified
     * by {@link Exporter}.
     *
     * @param destination to where backup should be saved.
     * @param baseLabel from where back up process will start.
     */
    public void incrementalBackup(URI destination, String baseLabel) {
        try {
            getCommandService().incrementalBackup(destination, baseLabel, generateRandomLabel());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns used {@link KeyValueProvider}.
     */
    public KeyValueProvider getKVProvider() {
        return Injection.get(KeyValueProvider.class, ignite());
    }

    /**
     * Returns root grid node.
     */
    public Ignite ignite() {
        return root;
    }

    public long nextTransactionId() {
        return Injection.get(IdSequencer.class, ignite()).getNextId();
    }

    /** {@inheritDoc} */
    @Override protected void setUp() {
        try {
            activeStoreConfiguration.afterPropertiesSet();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        root = clusterManager.startCluster(NODES, new IgniteConfiguration() {{
            setUserAttributes(new HashMap<>(singletonMap(CONFIG_USER_ATTR, activeStoreConfiguration)));
        }});
    }

    /** {@inheritDoc} */
    @Override protected void tearDown() {
        clusterManager.stopCluster();
    }

    /**
     * Creates cache with minimal configuration. Active store is not used.
     *
     * @param cacheName name of cache.
     * @param atomicityMode atomicity.
     * @param cacheMode mode.
     * @param base configuration to copy settings from.
     * @param <K> type of key.
     * @param <V> type of value
     * @return cache instance.
     */
    public <K, V> IgniteCache<K, V> createSimpleCache(String cacheName, CacheAtomicityMode atomicityMode,
        CacheMode cacheMode, CacheConfiguration<K, V> base) {
        CacheConfiguration<K, V> realConfiguration = base == null
            ? new CacheConfiguration<K, V>()
            : new CacheConfiguration<K, V>(base);
        realConfiguration.setName(cacheName);
        realConfiguration.setAtomicityMode(atomicityMode);
        realConfiguration.setCacheMode(cacheMode);
        realConfiguration.setBackups(2);
        return ignite().createCache(realConfiguration);
    }

    /**
     * Creates cache with full configuration. Active store is used.
     *
     * @param cacheName name of cache.
     * @param atomicityMode atomicity.
     * @param cacheMode mode.
     * @param writeBehindEnabled should write behind be used for active store.
     * @param flushFreq flush frequency for write behind.
     * @param base configuration to copy settings from.
     * @param <K> type of key.
     * @param <V> type of value
     * @return cache instance.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteCache<K, V> createCache(String cacheName, CacheAtomicityMode atomicityMode, CacheMode cacheMode,
        boolean writeBehindEnabled, int flushFreq,
        CacheConfiguration<K, V> base) {
        CacheConfiguration<K, V> realConfiguration = base == null
            ? new CacheConfiguration<K, V>()
            : new CacheConfiguration<K, V>(base);
        realConfiguration.setName(cacheName);
        realConfiguration.setAtomicityMode(atomicityMode);
        realConfiguration.setCacheMode(cacheMode);
        realConfiguration.setBackups(2);
        realConfiguration.setWriteThrough(true);
        realConfiguration.setReadThrough(true);
        realConfiguration.setWriteBehindEnabled(writeBehindEnabled);
        realConfiguration.setWriteBehindFlushFrequency(flushFreq);
        realConfiguration.setCacheStoreFactory(activeStoreConfiguration.activeCacheStoreFactory());
        return ignite().createCache(realConfiguration);
    }

    /**
     * Destroys previously created caches if they exist.
     *
     * @param cacheNames to destroy.
     */
    public void destroyCachesIfExist(String... cacheNames) {
        Collection<String> existingCacheNames = ignite().cacheNames();
        for (String cacheName : cacheNames) {
            if (existingCacheNames.contains(cacheName)) {
                ignite().cache(cacheName).withAsync().destroy();
            }
        }
    }
}

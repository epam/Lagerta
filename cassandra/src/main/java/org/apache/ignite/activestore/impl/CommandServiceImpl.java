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

package org.apache.ignite.activestore.impl;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.CommandService;
import org.apache.ignite.activestore.Exporter;
import org.apache.ignite.activestore.Exporter.Reader;
import org.apache.ignite.activestore.Exporter.ReaderProvider;
import org.apache.ignite.activestore.KeyValueManager;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.MetadataManager;
import org.apache.ignite.activestore.Tree;
import org.apache.ignite.activestore.commons.Injection;
import org.apache.ignite.activestore.commons.Reference;
import org.apache.ignite.activestore.impl.util.MetadataAtomicsHelper;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.activestore.impl.util.MetadataAtomicsHelper.getReference;
import static org.apache.ignite.activestore.impl.util.ExportNameConventionUtil.METADATA_RESOURCE_NAME;

/**
 * Implementaion of {@link CommandService} which performs all operations on metadata.
 */
public class CommandServiceImpl implements CommandService {
    /**
     * Name of atomic variable which holds current metadata head
     */
    public static final String HEAD_ATOMIC_NAME = "metadata.head";

    /**
     * Name of atomic variable which holds current metadata path
     */
    public static final String PATH_ATOMIC_NAME = "metadata.path";

    /**
     * Auto-injected ignite instance.
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Auto-injected logger
     */
    @LoggerResource
    private transient IgniteLogger log;

    /**
     * Atomic which holds current metadata head.
     */
    private transient Reference<Metadata> head;

    /**
     * Atomic which holds current metadata active path.
     */
    private transient Reference<List<Metadata>> path;

    /**
     * Auto-injected manager.
     */
    @Inject
    private transient KeyValueManager keyValueManager;

    /**
     * Auto-injected manager.
     */
    @Inject
    private transient MetadataManager metadataManager;

    /**
     * Auto-injected exporter.
     */
    @Inject
    private transient Exporter exporter;

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        if (head != null) {
            head.set(null);
            path.set(null);
        }
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        /* no-op */
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        if (ignite != null && Injection.isActive(ignite)) {
            ignite.addCacheConfiguration(MetadataAtomicsHelper.getConfig());

            path = getReference(ignite, PATH_ATOMIC_NAME, true);
            head = getReference(ignite, HEAD_ATOMIC_NAME, true);
            Injection.inject(this, ignite);
            List<Metadata> path = metadataManager.loadCurrentPath(null);
            Metadata head;
            if (path.isEmpty()) {
                head = metadataManager.createMetadata();
                metadataManager.putHead(head, null);
                keyValueManager.createSnapshot(head);
                path.add(head);
            }
            else {
                head = path.get(0);
            }
            setPath(path);
            this.head.set(head);
        }
    }

    /**
     * Checks if there are currently running user transaction and fails if any.
     */
    public void failOnExistingTransactions() {
        Collection<Boolean> activeTransactions = ignite.compute().broadcast(new TransactionCheckerJob());
        for (Boolean activeTransaction : activeTransactions) {
            if (activeTransaction) {
                throw new RuntimeException("There are still ongoing transactions");
            }
        }
    }

    /**
     * Updates path in all services.
     *
     * @param newPath path to set.
     */
    private void setPath(List<Metadata> newPath) {
        path.set(newPath);
        ignite.compute().withAsync().broadcast(new LocalPathModifier(newPath));
    }

    /**
     * Creates metadata with given labels.
     *
     * @param labels to associate with metadata.
     * @return created metadata.
     */
    private Metadata create(String... labels) {
        Metadata newHead = metadataManager.createMetadata(labels);
        metadataManager.putHead(newHead, head.get());
        keyValueManager.createSnapshot(newHead);
        setPath(metadataManager.loadCurrentPath(newHead));
        head.set(newHead);
        return newHead;
    }

    /** {@inheritDoc} */
    @Override public Metadata createSnapshot(String label) {
        return create(label);
    }

    /** {@inheritDoc} */
    @Override public Metadata rollback(String label) {
        failOnExistingTransactions();
        Metadata from = metadataManager.find(label);
        Metadata oldHead = this.head.get();
        Metadata newHead = metadataManager.createMetadata();
        metadataManager.putHead(newHead, from);
        keyValueManager.createSnapshot(newHead);
        setPath(metadataManager.loadCurrentPath(newHead));
        Iterable<Metadata> subPath = metadataManager.subPath(from, oldHead);
        if (subPath != null && subPath.iterator().hasNext()) {
            keyValueManager.invalidate(subPath, new Invalidator());
        }
        head.set(newHead);
        return newHead;
    }

    /** {@inheritDoc} */
    @Override public Metadata rollforward(String label) {
        //todo implement
        throw new UnsupportedOperationException();
        //transactionLogger.log("rollforward", timestamp);
    }

    /** {@inheritDoc} */
    @Override public Metadata merge(String from, String to) {
        Metadata fromMeta = metadataManager.find(from);
        Metadata toMeta = metadataManager.find(to);
        Metadata headMeta = head.get();
        if (headMeta.equals(fromMeta) || headMeta.equals(toMeta)) {
            throw new IllegalArgumentException("Cannot merge head metadata. Please exclude head from merge");
        }
        Metadata replacement = metadataManager.createMetadata();
        Iterable<Metadata> subLinePath = metadataManager.subLinePath(fromMeta, toMeta);
        if (subLinePath != null && subLinePath.iterator().hasNext()) {
            keyValueManager.merge(subLinePath, replacement);
        }
        metadataManager.replace(subLinePath, replacement);
        setPath(metadataManager.loadCurrentPath(head.get()));
        return replacement;
    }

    /** {@inheritDoc} */
    @Override public Collection<Metadata> backup(URI destination, String snapshotLabel) throws IOException {
        backupWithFiltering(destination, new IgnitePredicate<Metadata>() {
            @Override public boolean apply(Metadata metadata) {
                return true;
            }
        }, snapshotLabel);
        return metadataTree().allMetadatas();
    }

    /** {@inheritDoc} */
    @Override public Collection<Metadata> incrementalBackup(URI destination, String baseLabel,
        String snapshotLabel) throws IOException {
        final Collection<Metadata> metadatasToSave = metadataTree().getNewerMetadatas(baseLabel);
        backupWithFiltering(destination, new IgnitePredicate<Metadata>() {
            @Override public boolean apply(Metadata metadata) {
                return metadatasToSave.contains(metadata);
            }
        }, snapshotLabel);
        return metadatasToSave;
    }

    /**
     * Performs main backup logic.
     *
     * @param destination to where backup should be saved.
     * @param filteringCondition specifies which data (based on metadata) out of all available ones should be included
     * in backup
     * @throws IOException in case of errors during backup
     */
    private void backupWithFiltering(URI destination, IgnitePredicate<Metadata> filteringCondition,
        String snapshotLabel) throws IOException {
        try {
            Iterable<Metadata> metadatas;
            try (Exporter.WriterProvider provider = exporter.write(destination)) {
                try (Exporter.Writer writer = provider.open(METADATA_RESOURCE_NAME)) {
                    metadatas = metadataManager.backup(writer);
                }
            }
            Collection<Metadata> filteredMetadatas = new ArrayList<>();
            for (Metadata metadata : metadatas) {
                if (filteringCondition.apply(metadata)) {
                    filteredMetadatas.add(metadata);
                }
            }
            create(snapshotLabel); // create new head
            Map<String, List<Metadata>> byCache = keyValueManager.getSnapshotsByCache(filteredMetadatas);
            if (byCache.isEmpty()) {
                return;
            }
            ignite.compute().execute(new BackupTaskSplitAdapter(destination), byCache);
        }
        catch (Exception e) {
            exporter.cleanup(destination);
            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void restore(URI source) throws IOException {
        Tree oldTree = metadataTree();
        try (ReaderProvider provider = exporter.read(source)) {
            Iterable<String> names = provider.getNames();
            if (names.iterator().hasNext()) {
                restoreData(source, names);
            }
            try (Reader reader = provider.open(METADATA_RESOURCE_NAME)) {
                reader.read(metadataManager.restore());
                setPath(metadataManager.loadCurrentPath(null));
            }
        }
        Tree newTree = metadataTree();
        invalidateCaches(oldTree, newTree);
    }

    /**
     * Invalidates values in caches for all metadatas which were between two specified in tree.
     *
     * @param old start metadata to check difference
     * @param last end metadata to check difference
     */
    private void invalidateCaches(Tree old, Tree last) {
        Metadata oldestHead = old.head().compareTo(last.head()) < 0 ? old.head() : last.head();
        String oldestHeadLabel = String.valueOf(oldestHead.getTimestamp());
        Collection<Metadata> diff = new HashSet<>();
        if (old.findByLabel(oldestHeadLabel) == null) {
            diff.addAll(old.allMetadatas());
        }
        else {
            diff.addAll(old.getNewerMetadatas(oldestHeadLabel));
        }
        if (last.findByLabel(oldestHeadLabel) != null) {
            diff.addAll(last.getNewerMetadatas(oldestHeadLabel));
        }
        if (!diff.isEmpty()) {
            keyValueManager.invalidate(diff, new Invalidator());
        }
    }

    /**
     * Performs restore operaton on given resource names.
     *
     * @param source base path to existing backup.
     * @param names of resources included in this backup.
     */
    private void restoreData(final URI source, Iterable<String> names) {
        failOnExistingTransactions();
        ignite.compute().execute(new ComputeTaskSplitAdapter<Iterable<String>, Object>() {
            /** {@inheritDoc} */
            @Override protected Collection<? extends ComputeJob> split(int gridSize,
                Iterable<String> arg) throws IgniteException {
                List<ComputeJob> result = new ArrayList<>();
                for (String name : arg) {
                    result.add(new RestoreJob(source, name));
                }
                return result;
            }

            /** {@inheritDoc} */
            @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
                return null;
            }
        }, names);
    }

    /** {@inheritDoc} */
    @Override public Tree metadataTree() {
        return metadataManager.tree();
    }

    /**
     * Performs invalidation of data in caches.
     */
    private static class Invalidator implements IgniteBiInClosure<String, Set<Object>> {
        /**
         * Auto-injected ignite instance.
         */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** {@inheritDoc} */
        @Override public void apply(String cacheName, Set<Object> keys) {
            if (ignite == null) {
                Injection.injectIgnite(this, Ignition.localIgnite());
            }
            if (keys == null) {
                ignite.cache(cacheName).clear();
            }
            else {
                ignite.cache(cacheName).clearAll(keys);
            }
        }
    }

    /**
     * Splits whole backup task into smaller ones by caches.
     */
    private static class BackupTaskSplitAdapter extends ComputeTaskSplitAdapter<Map<String, List<Metadata>>, Object> {
        /**
         * Path to where backup should be saved.
         */
        private final URI destination;

        /**
         * Creates adapter.
         *
         * @param destination to backup.
         */
        public BackupTaskSplitAdapter(URI destination) {
            this.destination = destination;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Map<String, List<Metadata>> byCache)
            throws IgniteException {
            List<ComputeJob> result = new ArrayList<>();
            for (Map.Entry<String, List<Metadata>> entry : byCache.entrySet()) {
                String key = entry.getKey();
                for (Metadata metadata : entry.getValue()) {
                    result.add(new BackupJob(destination, key, metadata));
                }
            }
            return result;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /**
     * Job which checks node for running user transactions.
     */
    private static class TransactionCheckerJob implements IgniteCallable<Boolean> {
        /**
         * Auto-injected ignite instance.
         */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            GridKernalContext ctx = ((IgniteKernal)ignite).context();
            GridCacheSharedContext cctx = ctx.cache().context();
            Collection<GridCacheContext> contexts = cctx.cacheContexts();
            for (GridCacheContext context : contexts) {
                if (context.userCache() && hasUserTransactions(context.tm().activeTransactions())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Checks transaction in search for user created ones.
         *
         * @param runningTransactions all transactions.
         * @return true if there are user transactions running. False otherwise.
         */
        private boolean hasUserTransactions(Collection<IgniteInternalTx> runningTransactions) {
            for (IgniteInternalTx tx : runningTransactions) {
                if (tx.user()) {
                    return true;
                }
            }
            return false;
        }
    }

    /** */
    private static class LocalPathModifier implements IgniteRunnable {
        /** */
        private final List<Metadata> newPath;

        /** Auto-injected ignite instance. */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        @Inject
        private transient MetadataProviderImpl metadataProvider;

        public LocalPathModifier(List<Metadata> newPath) {
            this.newPath = newPath;
        }

        /** */
        @Override public void run() {
            Injection.inject(this, ignite);
            metadataProvider.setLocalPath(newPath);
        }
    }
}

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

package org.apache.ignite.activestore.simple;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.Tree;
import org.apache.ignite.activestore.impl.export.FileChecksumHelper;
import org.apache.ignite.activestore.impl.util.ExportNameConventionUtil;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.transactions.Transaction;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Checks backup/restore operations.
 */
public class ExportTest extends CommandServiceOperationsTest {
    /** */
    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder(Paths.get("").toAbsolutePath().toFile());

    /** */
    private static CyclicBarrier barrier = new CyclicBarrier(2);

    /** */
    @Test
    public void exportFilesCreated() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);

        URI backupDir = folder.newFolder().toURI();
        String destination = backupDir.toString();
        backupInCompute(destination);
        assertNotEquals(new File(backupDir).listFiles().length, 0);
    }

    /** */
    @Test
    public void checkImportTreeStructure() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);

        String destination = folder.newFolder().toURI().toString();
        backupInCompute(destination);

        Metadata snapshot3 = createSnapshotInCompute("3");
        writeKV("a", "3", firstCache, snapshot3);

        writeKV("a", "555", firstCache, snapshot2);

        assertNotNull(getTreeInCompute().findByLabel("3"));
        restoreInCompute(destination);
        assertNull(getTreeInCompute().findByLabel("3"));
    }

    /** */
    @Test
    public void backupShouldIncludeHeadSnapshot() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);

        String destination = folder.newFolder().toURI().toString();
        backupInCompute(destination);

        writeKV("a", "555", firstCache, snapshot2);

        assertEquals("555", readKV("a", firstCache, getTreeInCompute().findByLabel("2")));
        restoreInCompute(destination);
        assertEquals("2", readKV("a", firstCache, getTreeInCompute().findByLabel("2")));
    }

    /** */
    @Test
    public void emptyExportShouldNotFail() throws IOException {
        String destination = folder.newFolder().toURI().toString();
        backupInCompute(destination);
    }

    /** */
    @Test
    public void incrementalBackupShouldNotContainWholeData() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "1", secondCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("3");
        writeKV("a", "3", firstCache, snapshot3);

        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "2"); // 1 and 2 are discarded
        // change values in kv and then check that only some of them were restored
        writeKV("b", "666", secondCache, snapshot2);
        writeKV("a", "555", firstCache, snapshot3);

        Map<Object, Object> result = loadKeysFromCache();
        assertEquals("555", result.get("a"));
        assertEquals("666", result.get("b"));

        restoreInCompute(destination);
        // clear caches
        resource.ignite().compute().run(new IgniteRunnable() {
            @Override public void run() {
                Ignition.localIgnite().cache(firstCache).clear();
                Ignition.localIgnite().cache(secondCache).clear();
            }
        });

        result = loadKeysFromCache();
        assertEquals("3", result.get("a")); // restored
        assertEquals("666", result.get("b")); // not restored
    }

    /** */
    private Map<Object, Object> loadKeysFromCache() {
        return resource.ignite().compute().call(new IgniteCallable<Map<Object, Object>>() {
            @Override public Map<Object, Object> call() throws Exception {
                Map<Object, Object> result = new HashMap<>();
                result.put("a", Ignition.localIgnite().cache(firstCache).get("a"));
                result.put("b", Ignition.localIgnite().cache(secondCache).get("b"));
                return result;
            }
        });
    }

    /** */
    @Test(expected = IgniteException.class)
    public void incrementalBackupShouldFailWhenHeadNotFound() throws IOException {
        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "someNonExistentSnapshot");
    }

    /**
     * Tree before merge: 1 |-2 |-3 <p/> Tree after merge 1 and 2 |-3 (head) \-merged (newest) <p/>
     */
    @Test
    public void mergeDuringPreviousHeadInIncrementalBackup() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "1", secondCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("3");
        writeKV("a", "3", firstCache, snapshot3);

        Metadata merged = mergeInCompute(snapshot1, snapshot2);

        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "2");
        writeKV("b", "666", secondCache, merged);
        writeKV("a", "555", firstCache, snapshot3);

        Map<Object, Object> result = loadKeysFromCache();
        assertEquals("555", result.get("a"));
        assertEquals("666", result.get("b"));

        restoreInCompute(destination);
        result = loadKeysFromCache();
        assertEquals("3", result.get("a"));
        assertEquals("1", result.get("b"));
    }

    /**
     * Tree: 1 |-2 |-3 |-head We are merging 1 and 2 and previous head for backup == 3. Merged is included due to the
     * fact that it is newer.
     */
    @Test
    public void mergeBeforePreviousHeadInIncrementalBackup() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "1", secondCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("3");
        writeKV("a", "3", firstCache, snapshot3);

        Metadata merged = mergeInCompute(snapshot1, snapshot2);

        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "3");

        writeKV("b", "666", secondCache, merged);
        writeKV("a", "555", firstCache, merged);

        assertEquals("555", readKV("a", firstCache, merged));
        assertEquals("666", readKV("b", secondCache, merged));

        restoreInCompute(destination);
        Metadata restoredMerged = getTreeInCompute().findByLabel("2");
        assertEquals("2", readKV("a", firstCache, restoredMerged));
        assertEquals("1", readKV("b", secondCache, restoredMerged));
    }

    /**
     * Tree: 1 |-2 |-3 |-head Tree like in previous test but merging 2 and 3, previous head == 1. Everything should work
     * as usual - merged must be included in backup
     */
    @Test
    public void mergeAfterPreviousHeadInIncrementalBackup() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "1", secondCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);
        writeKV("b", "2", secondCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("3");
        writeKV("a", "3", firstCache, snapshot3);
        Metadata head = createSnapshotInCompute("head");
        writeKV("a", "4", firstCache, head);

        Metadata merged = mergeInCompute(snapshot2, snapshot3);

        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "1");

        writeKV("b", "666", secondCache, merged);
        writeKV("a", "555", firstCache, merged);

        assertEquals("555", readKV("a", firstCache, merged));
        assertEquals("666", readKV("b", secondCache, merged));

        restoreInCompute(destination);
        Metadata restoredMerged = getTreeInCompute().findByLabel("2");
        assertEquals("3", readKV("a", firstCache, restoredMerged));
        assertEquals("2", readKV("b", secondCache, restoredMerged));
    }

    /**
     * Create snapshots 1 and 2, then rollback to 1. Incremental backup with previous head == 2 will backup rollbacked
     * head.
     */
    @Test
    public void backupAfterRollback() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);
        Metadata rollbacked = rollbackInCompute("1");
        writeKV("a", "3", firstCache, rollbacked);

        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "2");

        writeKV("a", "555", firstCache, rollbacked);
        String rollbackId = String.valueOf(rollbacked.getTimestamp());
        assertEquals("555", readKV("a", firstCache, getTreeInCompute().findByLabel(rollbackId)));
        restoreInCompute(destination);
        assertEquals("3", readKV("a", firstCache, getTreeInCompute().findByLabel(rollbackId)));
    }

    /** */
    @Test
    public void cacheChangesInTransactionWhileBackup() throws Exception {
        IgniteCompute igniteCompute = resource.ignite().compute().withAsync();
        igniteCompute.run(new IgniteRunnable() {
            @Override public void run() {
                Ignite ignite = Ignition.localIgnite();
                try (Transaction tx = ignite.transactions().txStart()) {
                    ignite.cache(firstCache).put("a", "1");
                    barrier.await();
                    ignite.cache(secondCache).put("b", "2");
                    tx.commit();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        String destination1 = folder.newFolder("firstExport").toURI().toString();
        backupInCompute(destination1);
        barrier.await(); // unblock closure
        igniteCompute.future().get(); // end of transaction
        String destination2 = folder.newFolder("secondExport").toURI().toString();
        backupInCompute(destination2);

        // first backup should not contain data
        restoreInCompute(destination1);
        Map<Object, Object> result = loadKeysFromCache();
        assertNull(result.get("a"));
        assertNull(result.get("b"));
        // second backup should contain all
        restoreInCompute(destination2);
        result = loadKeysFromCache();
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
    }

    /**
     * Backup data, then corrupt some of it and try to restore. There should not be any partial restores.
     */
    @Test
    public void restoreShouldBeTransactional() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("b", "2", firstCache, snapshot2);

        URI exportDirUri = folder.newFolder().toURI();
        String destination = exportDirUri.toString();
        backupInCompute(destination);

        Metadata head = createSnapshotInCompute("head");
        writeKV("a", "3", firstCache, head);
        writeKV("b", "3", firstCache, head);

        assertNotNull(getTreeInCompute().findByLabel("head"));
        Map<Object, Object> result = readFromCache(firstCache, "a", "b");
        assertEquals("3", result.get("a"));
        assertEquals("3", result.get("b"));

        corruptDataFile(exportDirUri);
        try {
            restoreInCompute(destination);
        }
        catch (Exception e) {
            // just ignore it to check cache state
        }

        // due to error during restore nothing should be restored
        assertNotNull(getTreeInCompute().findByLabel("head"));
        result = readFromCache(firstCache, "a", "b");
        assertEquals("3", result.get("a"));
        assertEquals("3", result.get("b"));
    }

    /** */
    @Test(expected = IgniteException.class)
    public void failIfChecksumDoesNotMatch() throws IOException {
        URI exportDirUri = folder.newFolder().toURI();
        String destination = exportDirUri.toString();
        backupInCompute(destination);

        Files.walkFileTree(Paths.get(exportDirUri), new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (FileChecksumHelper.isChecksum(file)) {
                    Files.write(file, "corrupted".getBytes());
                }
                return FileVisitResult.CONTINUE;
            }
        });
        restoreInCompute(destination);
    }

    /** */
    private void corruptDataFile(URI exportDirUri) throws IOException {
        Files.walkFileTree(Paths.get(exportDirUri), new SimpleFileVisitor<Path>() {

            private boolean firstFile;

            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String fileName = file.getFileName().toString();
                if (!fileName.equals(ExportNameConventionUtil.METADATA_RESOURCE_NAME)) {
                    // skip first data file
                    if (!firstFile) {
                        firstFile = true;
                        return FileVisitResult.CONTINUE;
                    }
                    Files.write(file, "corruption".getBytes());
                    return FileVisitResult.TERMINATE;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Currently contains information about file with metadata as it is required. I think it is OK
     */
    @Test(expected = IgniteException.class)
    public void restoreFromNonExistentDir() {
        restoreInCompute(Paths.get("nonExistedDir").toUri().toString());
    }

    /**
     * Each merge creates the most recent metadata. We should check that after 2 consecutive backups on same metadatas
     * we will take the oldest of metadatas as the base line Tree structure: 0 |-1 |-2 |-3 |-4 |-head We are merging 1
     * and 2, then 2 and 3 (result is m123). After that we do rollback to m123 (r1): 0 |-m123 |-4 \-r1
     *
     * When we will select backup both 4 and r1 should be included in backup. 0 should be excluded.
     */
    @Test
    public void incrementalBackupAfterConsecutiveMerges() throws IOException {
        Metadata snapshot0 = createSnapshotInCompute("0");
        writeKV("0", "0", firstCache, snapshot0);
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("1", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("2", "2", firstCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("3");
        writeKV("3", "3", firstCache, snapshot3);
        Metadata snapshot4 = createSnapshotInCompute("4");
        writeKV("4", "4", firstCache, snapshot4);
        Metadata head = createSnapshotInCompute("head");
        writeKV("5", "5", firstCache, head);
        Metadata m12 = mergeInCompute(snapshot1, snapshot2);
        Metadata m123 = mergeInCompute(m12, snapshot3);

        String destination = folder.newFolder().toURI().toString();
        incrementalBackupInCompute(destination, "1");

        writeKV("0", "555", firstCache, snapshot0);
        writeKV("4", "555", firstCache, snapshot4);
        writeKV("3", "555", firstCache, m123);

        restoreInCompute(destination);
        Tree tree = getTreeInCompute();
        assertEquals("555", readKV("0", firstCache, tree.findByLabel("0"))); // excluded
        assertEquals("4", readKV("4", firstCache, tree.findByLabel("4")));
        assertEquals("3", readKV("3", firstCache, tree.findByLabel("3")));
    }

    /** */
    @Test
    public void restoreInvalidatesKeysForNewSnapshots() throws IOException {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);

        String destination1 = folder.newFolder().toURI().toString(); // does not contain snapshot2
        backupInCompute(destination1);

        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("b", "2", firstCache, snapshot2);

        String destination2 = folder.newFolder().toURI().toString(); // contains snapshot2
        backupInCompute(destination2);

        Map<Object, Object> result = readFromCache(firstCache, "a", "b");
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));

        restoreInCompute(destination1); // snapshot2 deleted from tree

        result = readFromCache(firstCache, "a", "b");
        assertEquals("1", result.get("a"));
        assertNull(result.get("b"));

        restoreInCompute(destination2); // snapshot2 added to tree

        result = readFromCache(firstCache, "a", "b");
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
    }

}

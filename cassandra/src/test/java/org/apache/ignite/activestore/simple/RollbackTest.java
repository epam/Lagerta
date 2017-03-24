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

import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.Tree;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Checks rollback operations.
 */
public class RollbackTest extends CommandServiceOperationsTest {
    /** */
    @Test
    public void simpleRollbackOnPreviousSnapshot() {
        Metadata snapshot1 = createSnapshotInCompute("snapshot1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "2", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("laterSnapshot");
        writeKV("b", "3", firstCache, snapshot2);
        writeKV("c", "4", firstCache, snapshot2);

        Map result = readFromCache(firstCache, "a", "b", "c");

        assertEquals("1", result.get("a"));
        assertEquals("3", result.get("b"));
        assertEquals("4", result.get("c"));

        rollbackInCompute("snapshot1");

        result = readFromCache(firstCache, "a", "b", "c");

        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        Assert.assertNull(result.get("c"));
    }

    /**
     * Change plan: a b c d e f 1 2 3 4 5 6 7       8 9
     */
    @Test
    public void rollbackFrom3Snapshots() {
        Metadata snapshot1 = createSnapshotInCompute("snapshot1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "2", firstCache, snapshot1);
        writeKV("c", "3", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("snapshot2");
        writeKV("c", "4", firstCache, snapshot2);
        writeKV("d", "5", firstCache, snapshot2);
        writeKV("e", "6", firstCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("snapshot3");
        writeKV("a", "7", firstCache, snapshot3);
        writeKV("e", "8", firstCache, snapshot3);
        writeKV("f", "9", firstCache, snapshot3);

        Map result = readFromCache(firstCache, "a", "b", "c", "d", "e", "f");
        assertEquals("7", result.get("a"));
        assertEquals("2", result.get("b"));
        assertEquals("4", result.get("c"));
        assertEquals("5", result.get("d"));
        assertEquals("8", result.get("e"));
        assertEquals("9", result.get("f"));

        rollbackInCompute("snapshot1");
        result = readFromCache(firstCache, "a", "b", "c", "d", "e", "f");
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
        assertNull(result.get("d"));
        assertNull(result.get("e"));
        assertNull(result.get("f"));
    }

    /** */
    @Test(expected = IgniteException.class)
    public void shouldFailIfTransactionExist() {
        createSnapshotInCompute("snapshot1");
        createSnapshotInCompute("snapshot2");

        try (Transaction ignored = resource.ignite().transactions().txStart()) {
            rollbackInCompute("snapshot1");
        }
    }

    /**
     * Plan of tree: / - - 1 |     2 - - \ |     3     | |           4 (rollbacked) |           5 6 (rollbacked)
     *
     * 1 |-2 | |-3 | \-4 |   \-5 \-6
     */
    @Test
    public void treeStructureOnRollback() {
        Metadata snapshot1 = createSnapshotInCompute("snapshot1");
        Metadata snapshot2 = createSnapshotInCompute("snapshot2");
        Metadata snapshot3 = createSnapshotInCompute("snapshot3");
        Metadata snapshot4 = rollbackInCompute("snapshot2");
        Metadata snapshot5 = createSnapshotInCompute("snapshot5");
        Metadata snapshot6 = rollbackInCompute("snapshot1");

        Tree tree = getTreeInCompute();
        // center branch
        List<Metadata> branch1 = Lists.newArrayList(tree.subPath(snapshot1, snapshot3));
        assertEquals(2, branch1.size());
        assertTrue(branch1.contains(snapshot2));
        assertTrue(branch1.contains(snapshot3));
        // right branch
        List<Metadata> branch2 = Lists.newArrayList(tree.subPath(snapshot1, snapshot5));
        assertEquals(3, branch2.size());
        assertTrue(branch2.contains(snapshot2));
        assertTrue(branch2.contains(snapshot4));
        assertTrue(branch2.contains(snapshot5));
        // left branch
        List<Metadata> branch3 = Lists.newArrayList(tree.subPath(snapshot1, snapshot6));
        assertEquals(1, branch3.size());
        assertTrue(branch3.contains(snapshot6));
    }

    /**
     * Snapshot plan: cache1 | cache2 a b   c d 1     2 3   4 5
     */
    @Test
    public void writeIn2Caches() {
        Metadata snapshot1 = createSnapshotInCompute("snapshot1");
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("c", "2", secondCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("laterSnapshot");
        writeKV("b", "3", firstCache, snapshot2);
        writeKV("c", "4", secondCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("anotherSnapshot");
        writeKV("d", "5", secondCache, snapshot3);

        Map result1 = readFromCache(firstCache, "a", "b");
        assertEquals("1", result1.get("a"));
        assertEquals("3", result1.get("b"));
        Map result2 = readFromCache(secondCache, "c", "d");
        assertEquals("4", result2.get("c"));
        assertEquals("5", result2.get("d"));

        rollbackInCompute("snapshot1");

        result1 = readFromCache(firstCache, "a", "b");
        assertEquals("1", result1.get("a"));
        assertNull(result1.get("b"));
        result2 = readFromCache(secondCache, "c", "d");
        assertEquals("2", result2.get("c"));
        assertNull(result2.get("d"));
    }

    /** */
    @Test
    public void writeToKVAfterRollback() {
        Metadata snapshot1 = createSnapshotInCompute("snapshot1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("laterSnapshot");
        writeKV("a", "2", firstCache, snapshot2);

        Metadata snapshot3 = rollbackInCompute("snapshot1");
        writeKV("a", "3", firstCache, snapshot3);

        Map result = readFromCache(firstCache, "a");
        assertEquals("3", result.get("a"));
    }

    /**
     * Plan of tree: / - - 1 |     2 - - \ |     3     | |           4 (rollbacked) |           5 6 (rollbacked)
     *
     * 1 |-2 | |-3 | \-4 |   \-5 \-6
     */
    @Test(expected = IgniteException.class)
    public void treeRollbackOnDifferentBranch() {
        createSnapshotInCompute("snapshot1");
        createSnapshotInCompute("snapshot2");
        createSnapshotInCompute("snapshot3");
        rollbackInCompute("snapshot2");
        createSnapshotInCompute("snapshot5");
        rollbackInCompute("snapshot1");

        rollbackInCompute("snapshot5");
    }
}

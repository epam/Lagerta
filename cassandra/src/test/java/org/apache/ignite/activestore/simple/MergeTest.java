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

import org.apache.ignite.IgniteException;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.activestore.Tree;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Checks merge operations.
 */
public class MergeTest extends CommandServiceOperationsTest {
    /** */
    @Test
    public void simpleMerge() {
        Metadata snapshot1 = resource.head();
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "2", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("laterSnapshot");
        writeKV("b", "3", firstCache, snapshot2);
        writeKV("c", "4", firstCache, snapshot2);
        createSnapshotInCompute("head");

        Metadata merged = mergeInCompute(snapshot1, snapshot2);

        assertEquals("1", readKV("a", firstCache, merged));
        assertEquals("3", readKV("b", firstCache, merged));
        assertEquals("4", readKV("c", firstCache, merged));
    }

    /** */
    @Test
    public void consecutiveMerges() {
        Metadata snapshot1 = createSnapshotInCompute("1");
        writeKV("a", "1", firstCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("2");
        writeKV("a", "2", firstCache, snapshot2);
        writeKV("b", "3", firstCache, snapshot2);
        Metadata snapshot3 = createSnapshotInCompute("3");
        Metadata merged12 = mergeInCompute(snapshot1, snapshot2);
        writeKV("a", "3", firstCache, snapshot3);
        Metadata head = createSnapshotInCompute("head");
        writeKV("a", "4", firstCache, head);

        Metadata merged = mergeInCompute(merged12, snapshot3);

        assertEquals("3", readKV("a", firstCache, merged));
        assertEquals("3", readKV("b", firstCache, merged));
    }

    /**
     * 1 |-2 |-3 \-4
     */
    @Test(expected = IgniteException.class)
    public void shouldFailOnManyChildren() {
        Metadata snapshot1 = createSnapshotInCompute("1");
        Metadata snapshot2 = createSnapshotInCompute("2");
        Metadata snapshot3 = createSnapshotInCompute("3");
        Metadata snapshot4 = rollbackInCompute("2");
        createSnapshotInCompute("head");

        mergeInCompute(snapshot3, snapshot4);
    }

    /** */
    @Test
    public void mergeSnapshotsFor2Caches() {
        Metadata snapshot1 = resource.head();
        writeKV("a", "1", firstCache, snapshot1);
        writeKV("b", "2", secondCache, snapshot1);
        Metadata snapshot2 = createSnapshotInCompute("laterSnapshot");
        writeKV("a", "3", firstCache, snapshot2);
        writeKV("c", "4", secondCache, snapshot2);
        createSnapshotInCompute("head");

        Metadata merged = mergeInCompute(snapshot1, snapshot2);

        assertEquals("3", readKV("a", firstCache, merged));
        assertEquals("2", readKV("b", secondCache, merged));
        assertEquals("4", readKV("c", secondCache, merged));
    }

    /** */
    @Test
    public void mergeSameSnapshotWithItself() {
        Metadata snapshot1 = resource.head();
        writeKV("a", "1", firstCache, snapshot1);
        createSnapshotInCompute("head");

        Metadata merged = mergeInCompute(snapshot1, snapshot1);

        assertEquals("1", readKV("a", firstCache, merged));
    }

    /** */
    @Test
    public void mergedSnapshotHasAllCombinedLabels() {
        Metadata snapshot1 = createSnapshotInCompute("1");
        Metadata snapshot2 = createSnapshotInCompute("something");
        createSnapshotInCompute("head");

        Metadata merged = mergeInCompute(snapshot1, snapshot2);
        Tree tree = getTreeInCompute();
        assertEquals(merged, tree.findByLabel("1"));
        assertEquals(merged, tree.findByLabel("something"));
        assertEquals(merged, tree.findByLabel(String.valueOf(snapshot1.getTimestamp())));
        assertEquals(merged, tree.findByLabel(String.valueOf(snapshot2.getTimestamp())));
        assertEquals(merged, tree.findByLabel(String.valueOf(merged.getTimestamp())));
    }

    /** */
    @Test(expected = IgniteException.class)
    public void shouldFailWhenTryToMergeHead() {
        Metadata snapshot1 = createSnapshotInCompute("1");
        Metadata snapshot2 = createSnapshotInCompute("head");

        mergeInCompute(snapshot1, snapshot2);
    }

    /** */
    @Test(expected = IgniteException.class)
    public void mergeRollbackAndMergeAgainShouldFail() {
        Metadata snapshot1 = createSnapshotInCompute("1");
        Metadata snapshot2 = createSnapshotInCompute("2");
        Metadata snapshot3 = createSnapshotInCompute("3");
        Metadata head = createSnapshotInCompute("head");
        Metadata merged12 = mergeInCompute(snapshot1, snapshot2);
        Metadata anotherBranch = rollbackInCompute("1");
        Metadata newHead = createSnapshotInCompute("newHead");
        // should fail as merged12 has 2 children - snapshot3 and anotherBranch
        mergeInCompute(merged12, anotherBranch);
    }

}

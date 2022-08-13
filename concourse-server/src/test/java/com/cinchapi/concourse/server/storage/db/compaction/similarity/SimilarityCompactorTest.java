/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.compaction.similarity;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.storage.db.SegmentStorageSystem;
import com.cinchapi.concourse.server.storage.db.compaction.CompactorTests;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for {@link SimilarityCompactor}.
 *
 * @author Jeff Nelson
 */
public class SimilarityCompactorTest {

    @Test
    public void testSanityCheck() {
        SegmentStorageSystem storage = CompactorTests.getStorageSystem();
        Segment a = Segment.create();
        a.acquire(Write.add("name", Convert.javaToThrift("jeff"), 1));
        a.acquire(Write.add("name", Convert.javaToThrift("ashleah"), 2));
        a.acquire(Write.add("age", Convert.javaToThrift(33), 2));
        a.acquire(Write.add("company", Convert.javaToThrift("Cinchapi"), 1));
        a.transfer(Paths.get(TestData.getTemporaryTestFile()));

        Segment b = Segment.create();
        b.acquire(Write.add("name", Convert.javaToThrift("Jeff"), 1));
        b.acquire(Write.remove("name", Convert.javaToThrift("jeff"), 1));
        b.acquire(Write.remove("name", Convert.javaToThrift("ashleah"), 2));
        b.acquire(Write.add("name", Convert.javaToThrift("Jeffery"), 1));
        b.acquire(Write.add("company", Convert.javaToThrift("Know Full Well"),
                2));
        b.acquire(Write.add("age", Convert.javaToThrift(33), 1));
        b.acquire(Write.add("age", Convert.javaToThrift(33.0), 1));
        b.transfer(Paths.get(TestData.getTemporaryTestFile()));

        storage.segments().add(a);
        storage.segments().add(b);
        storage.segments().add(Segment.create()); // seg0

        SimilarityCompactor compactor = new SimilarityCompactor(storage);
        compactor.minimumSimilarityThreshold(0);
        List<Write> expected = storage.segments().stream()
                .flatMap(segment -> segment.writes())
                .collect(Collectors.toList());
        compactor.executeFullCompaction();

        List<Write> actual = storage.segments().stream()
                .flatMap(segment -> segment.writes())
                .collect(Collectors.toList());
        Assert.assertEquals(2, storage.segments().size());
        Assert.assertTrue(
                expected.size() == actual.size() && expected.containsAll(actual)
                        && actual.containsAll(expected)); // assert that no data
        actual.forEach(System.out::println); // lost...
        for (int i = 0; i < actual.size(); ++i) {
            if(i > 0) {
                Identifier previous = actual.get(i - 1).getRecord();
                Identifier current = actual.get(i).getRecord();
                // Assert that, from table view, all records are grouped
                // together
                Assert.assertTrue(current.longValue() == previous.longValue()
                        || current.longValue() == previous.longValue() + 1);
            }
        }
        Assert.assertEquals(ImmutableList.of(a, b), compactor.garbage());
    }

}

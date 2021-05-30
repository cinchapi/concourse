/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.compaction;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link MergeSortCompactor}.
 *
 * @author Jeff Nelson
 */
public class MergeSortCompactorTest {

    @Test
    public void testStaticMergeSortSanityCheck() {
        List<Segment> segments = Lists.newArrayList();
        List<Segment> garbage = Lists.newArrayList();

        Segment a = Segment.create();
        a.transfer(Write.add("name", Convert.javaToThrift("jeff"), 1));
        a.transfer(Write.add("name", Convert.javaToThrift("ashleah"), 2));
        a.transfer(Write.add("age", Convert.javaToThrift(33), 2));
        a.transfer(Write.add("company", Convert.javaToThrift("Cinchapi"), 1));
        a.fsync(Paths.get(TestData.getTemporaryTestFile()));

        Segment b = Segment.create();
        b.transfer(Write.add("name", Convert.javaToThrift("Jeff"), 1));
        b.transfer(Write.remove("name", Convert.javaToThrift("jeff"), 1));
        b.transfer(Write.remove("name", Convert.javaToThrift("ashleah"), 2));
        b.transfer(Write.add("name", Convert.javaToThrift("Jeffery"), 1));
        b.transfer(Write.add("company", Convert.javaToThrift("Know Full Well"),
                2));
        b.transfer(Write.add("age", Convert.javaToThrift(33), 1));
        b.transfer(Write.add("age", Convert.javaToThrift(33.0), 1));
        b.fsync(Paths.get(TestData.getTemporaryTestFile()));

        segments.add(a);
        segments.add(b);
        segments.add(Segment.create()); // seg0

        Compactor compactor = Compactor.builder().withTestDefaults()
                .segments(segments).garbage(garbage)
                .type(MergeSortCompactor.class).build();
        ((MergeSortCompactor) compactor).minimumSimilarityThreshold(0);
        List<Write> expected = segments.stream()
                .flatMap(segment -> segment.writes())
                .collect(Collectors.toList());
        compactor.run(0, 2);

        List<Write> actual = segments.stream()
                .flatMap(segment -> segment.writes())
                .collect(Collectors.toList());
        Assert.assertEquals(2, segments.size());
        Assert.assertTrue(
                expected.size() == actual.size() && expected.containsAll(actual)
                        && actual.containsAll(expected)); // assert that no data
        actual.forEach(System.out::println); // lost...
        for (int i = 0; i < actual.size(); ++i) {
            if(i > 0) {
                PrimaryKey previous = actual.get(i - 1).getRecord();
                PrimaryKey current = actual.get(i).getRecord();
                // Assert that, from table view, all records are grouped
                // together
                Assert.assertTrue(current.longValue() == previous.longValue()
                        || current.longValue() == previous.longValue() + 1);
            }
        }
        Assert.assertEquals(ImmutableList.of(a, b), garbage);
    }

}

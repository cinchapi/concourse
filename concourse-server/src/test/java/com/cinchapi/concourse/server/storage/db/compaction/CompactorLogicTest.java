/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.storage.db.compaction.Compactor.Shift;
import com.cinchapi.concourse.server.storage.db.format.Segment;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Unit test for internal logic of {@link Compactor}.
 *
 * @author Jeff Nelson
 */
public class CompactorLogicTest {

    @Test
    public void testRunMergeShift() {
        List<Segment> segments = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            segments.add(createTestSegment());
        }
        List<Segment> garbage = Lists.newArrayList();
        Compactor compactor = Compactor.builder().type(MergeCompactor.class)
                .environment("test").segments(segments).garbage(garbage)
                .lock(new ReentrantLock())
                .fileProvider(() -> Paths.get(TestData.getTemporaryTestFile()))
                .build();
        Shift shift;
        shift = compactor.run(0, 1);
        Assert.assertEquals(1, shift.index);
        Assert.assertEquals(1, shift.count);
        shift = compactor.run(0, 3);
        Assert.assertEquals(2, shift.index);
        Assert.assertEquals(3, shift.count);
        shift = compactor.run(9, 1);
        Assert.assertEquals(0, shift.index);
        Assert.assertEquals(2, shift.count);
        shift = compactor.run(0, 10); // start over
        Assert.assertEquals(0, shift.index);
        Assert.assertEquals(1, shift.count);
        shift = compactor.run(4, 5);
        Assert.assertEquals(0, shift.index);
        Assert.assertEquals(6, shift.count);
    }

    @Test
    public void testRunFailShift() {
        List<Segment> segments = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            segments.add(createTestSegment());
        }
        List<Segment> garbage = Lists.newArrayList();
        Compactor compactor = Compactor.builder().type(FailCompactor.class)
                .environment("test").segments(segments).garbage(garbage)
                .lock(new ReentrantLock())
                .fileProvider(() -> Paths.get(TestData.getTemporaryTestFile()))
                .build();
        Shift shift;
        shift = compactor.run(0, 6);
        Assert.assertEquals(1, shift.index);
        Assert.assertEquals(6, shift.count);
        shift = compactor.run(shift.index, shift.count);
        Assert.assertEquals(2, shift.index);
        Assert.assertEquals(6, shift.count);
        shift = compactor.run(shift.index, shift.count);
        Assert.assertEquals(3, shift.index);
        Assert.assertEquals(6, shift.count);
        shift = compactor.run(shift.index, shift.count);
        Assert.assertEquals(0, shift.index);
        Assert.assertEquals(7, shift.count);
    }

    private final Segment createTestSegment() {
        Segment segment = Segment.create();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            segment.transfer(TestData.getWriteAdd());
        }
        return segment;
    }

    static class FailCompactor extends Compactor {

        /**
         * Construct a new instance.
         * 
         * @param environment
         * @param segments
         * @param garbage
         * @param lock
         * @param fileProvider
         * @param minorInitialDelayInSeconds
         * @param minorRunFrequencyInSeconds
         * @param majorInitialDelayInSeconds
         * @param majorRunFrequencyInSeconds
         */
        protected FailCompactor(String environment, List<Segment> segments,
                List<Segment> garbage, Lock lock, Supplier<Path> fileProvider,
                long minorInitialDelayInSeconds,
                long minorRunFrequencyInSeconds,
                long majorInitialDelayInSeconds,
                long majorRunFrequencyInSeconds) {
            super(environment, segments, garbage, lock, fileProvider,
                    minorInitialDelayInSeconds, minorRunFrequencyInSeconds,
                    majorInitialDelayInSeconds, majorRunFrequencyInSeconds);
        }

        @Override
        protected List<Segment> compact(StorageContext context,
                Segment... segments) {
            return null;
        }

    }

    static class MergeCompactor extends Compactor {

        /**
         * Construct a new instance.
         * 
         * @param environment
         * @param segments
         * @param garbage
         * @param lock
         * @param fileProvider
         * @param minorInitialDelayInSeconds
         * @param minorRunFrequencyInSeconds
         * @param majorInitialDelayInSeconds
         * @param majorRunFrequencyInSeconds
         */
        protected MergeCompactor(String environment, List<Segment> segments,
                List<Segment> garbage, Lock lock, Supplier<Path> fileProvider,
                long minorInitialDelayInSeconds,
                long minorRunFrequencyInSeconds,
                long majorInitialDelayInSeconds,
                long majorRunFrequencyInSeconds) {
            super(environment, segments, garbage, lock, fileProvider,
                    minorInitialDelayInSeconds, minorRunFrequencyInSeconds,
                    majorInitialDelayInSeconds, majorRunFrequencyInSeconds);
        }

        @Override
        protected List<Segment> compact(StorageContext context,
                Segment... segments) {
            if(segments.length >= 2) {
                Segment merged = Segment.create();
                for (Segment segment : segments) {
                    segment.writes().forEach(write -> merged.transfer(write));
                }
                return ImmutableList.of(merged);
            }
            else {
                return null;
            }
        }
    }

    static class SplitCompactor extends Compactor {

        /**
         * Construct a new instance.
         * 
         * @param environment
         * @param segments
         * @param garbage
         * @param lock
         * @param fileProvider
         * @param minorInitialDelayInSeconds
         * @param minorRunFrequencyInSeconds
         * @param majorInitialDelayInSeconds
         * @param majorRunFrequencyInSeconds
         */
        protected SplitCompactor(String environment, List<Segment> segments,
                List<Segment> garbage, Lock lock, Supplier<Path> fileProvider,
                long minorInitialDelayInSeconds,
                long minorRunFrequencyInSeconds,
                long majorInitialDelayInSeconds,
                long majorRunFrequencyInSeconds) {
            super(environment, segments, garbage, lock, fileProvider,
                    minorInitialDelayInSeconds, minorRunFrequencyInSeconds,
                    majorInitialDelayInSeconds, majorRunFrequencyInSeconds);
        }

        @Override
        protected List<Segment> compact(StorageContext context,
                Segment... segments) {
            // TODO Auto-generated method stub
            return null;
        }

    }

}

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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.storage.db.SegmentStorageSystem;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableList;

/**
 * Unit test for internal logic of {@link Compactor}.
 *
 * @author Jeff Nelson
 */
public class CompactorLogicTest {

    @Test
    public void testRunMergeShift() {
        SegmentStorageSystem storage = CompactorTests.getStorageSystem();
        for (int i = 0; i < 10; ++i) {
            storage.segments().add(createTestSegment());
        }
        Compactor compactor = new MergeCompactor(storage);
        compactor.runShift(0, 1);
        Assert.assertEquals(1, compactor.getShiftIndex());
        Assert.assertEquals(1, compactor.getShiftCount());
        compactor.runShift(0, 3);
        Assert.assertEquals(2, compactor.getShiftIndex());
        Assert.assertEquals(3, compactor.getShiftCount());
        compactor.runShift(9, 1);
        Assert.assertEquals(0, compactor.getShiftIndex());
        Assert.assertEquals(2, compactor.getShiftCount());
        compactor.runShift(0, 10); // start over
        Assert.assertEquals(0, compactor.getShiftIndex());
        Assert.assertEquals(1, compactor.getShiftCount());
        compactor.runShift(4, 5);
        Assert.assertEquals(0, compactor.getShiftIndex());
        Assert.assertEquals(6, compactor.getShiftCount());
    }

    @Test
    public void testRunFailShift() {
        SegmentStorageSystem storage = CompactorTests.getStorageSystem();
        for (int i = 0; i < 10; ++i) {
            storage.segments().add(createTestSegment());
        }
        Compactor compactor = new FailCompactor(storage);
        compactor.runShift(0, 6);
        Assert.assertEquals(1, compactor.getShiftIndex());
        Assert.assertEquals(6, compactor.getShiftCount());
        compactor.runShift(compactor.getShiftIndex(),
                compactor.getShiftCount());
        Assert.assertEquals(2, compactor.getShiftIndex());
        Assert.assertEquals(6, compactor.getShiftCount());
        compactor.runShift(compactor.getShiftIndex(),
                compactor.getShiftCount());
        Assert.assertEquals(3, compactor.getShiftIndex());
        Assert.assertEquals(6, compactor.getShiftCount());
        compactor.runShift(compactor.getShiftIndex(),
                compactor.getShiftCount());
        Assert.assertEquals(4, compactor.getShiftIndex());
        Assert.assertEquals(6, compactor.getShiftCount());
        compactor.runShift(compactor.getShiftIndex(),
                compactor.getShiftCount());
        Assert.assertEquals(5, compactor.getShiftIndex());
        Assert.assertEquals(6, compactor.getShiftCount());
        compactor.runShift(compactor.getShiftIndex(),
                compactor.getShiftCount());
        Assert.assertEquals(0, compactor.getShiftIndex());
        Assert.assertEquals(7, compactor.getShiftCount());
    }

    private final Segment createTestSegment() {
        Segment segment = Segment.create();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            segment.acquire(TestData.getWriteAdd());
        }
        segment.transfer(Paths.get(TestData.getTemporaryTestFile()));
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
        protected FailCompactor(SegmentStorageSystem storage) {
            super(storage);
        }

        @Override
        protected List<Segment> compact(Segment... segments) {
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
        protected MergeCompactor(SegmentStorageSystem storage) {
            super(storage);
        }

        @Override
        protected List<Segment> compact(Segment... segments) {
            if(segments.length >= 2) {
                Segment merged = Segment.create();
                for (Segment segment : segments) {
                    segment.writes().forEach(write -> merged.acquire(write));
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
        protected SplitCompactor(SegmentStorageSystem storage) {
            super(storage);
        }

        @Override
        protected List<Segment> compact(Segment... segments) {
            return null;
        }

    }

}

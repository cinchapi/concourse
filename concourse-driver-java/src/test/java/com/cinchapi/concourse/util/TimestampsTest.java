/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link com.cinchapi.concourse.util.Timestamps}.
 *
 * @author knd
 */
public class TimestampsTest {

    @Test
    public void testFindNearestSuccessorForTimestampWithStartTimestampLessThanFirstTimestampInChronology() {
        Set<Timestamp> timestamps = new LinkedHashSet<Timestamp>();
        timestamps.add(Timestamp.fromMicros(1000L));
        for (int i = 0; i < Random.getScaleCount(); i++) {
            long increment = 0;
            while (increment == 0) {
                increment = Math.abs(Random.getScaleCount());
            }
            timestamps.add(Timestamp.fromMicros(
                    Iterables.getLast(timestamps).getMicros() + increment));
        }
        Timestamp startTimestamp = Timestamp.epoch();
        Assert.assertEquals(0, Timestamps
                .findNearestSuccessorForTimestamp(timestamps, startTimestamp));
    }

    @Test
    public void testFindNearestSuccessorForTimestampWithStartTimestampGreaterThanLastTimestampInChronology() {
        Set<Timestamp> timestamps = new LinkedHashSet<Timestamp>();
        timestamps.add(Timestamp.fromMicros(1000L));
        for (int i = 0; i < Random.getScaleCount(); i++) {
            long increment = 0;
            while (increment == 0) {
                increment = Math.abs(Random.getScaleCount());
            }
            timestamps.add(Timestamp.fromMicros(
                    Iterables.getLast(timestamps).getMicros() + increment));
        }
        Timestamp startTimestamp = Timestamp
                .fromMicros(Iterables.getLast(timestamps).getMicros() + 1000L);
        Assert.assertEquals(timestamps.size(), Timestamps
                .findNearestSuccessorForTimestamp(timestamps, startTimestamp));
    }

    @Test
    public void testFindNearestSuccessorForTimestampWithStartTimestampEqualToATimestampInChronology() {
        Set<Timestamp> timestamps = new LinkedHashSet<Timestamp>();
        timestamps.add(Timestamp.fromMicros(1000L));
        for (int i = 0; i < Random.getScaleCount(); i++) {
            long increment = 0;
            while (increment == 0) {
                increment = Math.abs(Random.getScaleCount());
            }
            timestamps.add(Timestamp.fromMicros(
                    Iterables.getLast(timestamps).getMicros() + increment));
        }
        Timestamp startTimestamp = Timestamp
                .fromMicros(Iterables.getFirst(timestamps, null).getMicros());
        Assert.assertEquals(0, Timestamps
                .findNearestSuccessorForTimestamp(timestamps, startTimestamp));
        startTimestamp = Timestamp.fromMicros(
                Iterables.get(timestamps, timestamps.size() / 2).getMicros());
        Assert.assertEquals(timestamps.size() / 2, Timestamps
                .findNearestSuccessorForTimestamp(timestamps, startTimestamp));
        startTimestamp = Timestamp
                .fromMicros(Iterables.getLast(timestamps).getMicros());
        Assert.assertEquals(timestamps.size() - 1, Timestamps
                .findNearestSuccessorForTimestamp(timestamps, startTimestamp));
    }

    @Test
    public void testFindNearestSuccessorForTimestampWithStartTimestampGreaterThanFirstTimestampAndLessThanLastTimestampInChronology() {
        Set<Timestamp> timestamps = new LinkedHashSet<Timestamp>();
        timestamps.add(Timestamp.fromMicros(1000L));
        for (int i = 0; i < Random.getScaleCount(); i++) {
            long increment = 0;
            while (increment == 0) {
                increment = Math.abs(Random.getScaleCount()) + 100L;
            }
            timestamps.add(Timestamp.fromMicros(
                    Iterables.getLast(timestamps).getMicros() + increment));
        }
        Timestamp abitrary = Iterables.get(timestamps, timestamps.size() / 3);
        Timestamp abitrarySuccessor = Iterables.get(timestamps,
                timestamps.size() / 3 + 1);
        Timestamp startTimestamp = Timestamp.fromMicros(
                (abitrary.getMicros() + abitrarySuccessor.getMicros()) / 2);
        Assert.assertEquals(timestamps.size() / 3 + 1, Timestamps
                .findNearestSuccessorForTimestamp(timestamps, startTimestamp));
    }

    @Test
    public void testTimestampComparableInChronologicalOrder() {
        List<Timestamp> timestamps = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            timestamps.add(Timestamp.now());
            Random.tinySleep();
        }
        java.util.Collections.shuffle(timestamps);
        Set<Timestamp> sorted = Sets.newTreeSet(timestamps);
        Timestamp previous = null;
        for (Timestamp timestamp : sorted) {
            if(previous != null) {
                Assert.assertTrue(
                        previous.getInstant().isBefore(timestamp.getInstant()));
            }
            previous = timestamp;
        }

    }

}

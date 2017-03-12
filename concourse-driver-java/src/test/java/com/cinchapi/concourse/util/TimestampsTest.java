/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Timestamps;
import com.google.common.collect.Iterables;

/**
 * Unit tests for {@link Timestamps}.
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
        assertEquals(0, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
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
        Timestamp startTimestamp = Timestamp.fromMicros(
                Iterables.getLast(timestamps).getMicros() + 1000L);
        assertEquals(timestamps.size(), Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
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
        Timestamp startTimestamp = Timestamp.fromMicros(
                Iterables.getFirst(timestamps, null).getMicros());
        assertEquals(0, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
        startTimestamp = Timestamp.fromMicros(
                Iterables.get(timestamps, timestamps.size()/2).getMicros());
        assertEquals(timestamps.size()/2, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
        startTimestamp = Timestamp.fromMicros(
                Iterables.getLast(timestamps).getMicros());
        assertEquals(timestamps.size()-1, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));   
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
        Timestamp abitrary = Iterables.get(timestamps, timestamps.size()/3);
        Timestamp abitrarySuccessor = Iterables.get(timestamps, timestamps.size()/3+1);
        Timestamp startTimestamp = Timestamp.fromMicros(
                (abitrary.getMicros() + abitrarySuccessor.getMicros()) / 2);
        assertEquals(timestamps.size()/3 + 1, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
    }

}

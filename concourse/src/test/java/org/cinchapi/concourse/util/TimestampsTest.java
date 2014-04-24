/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashSet;
import java.util.Set;

import org.cinchapi.concourse.Timestamp;
import org.junit.Test;

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
        assertEquals(1, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
        startTimestamp = Timestamp.fromMicros(
                Iterables.get(timestamps, timestamps.size()/2).getMicros());
        assertEquals(timestamps.size()/2+1, Timestamps.
                findNearestSuccessorForTimestamp(timestamps, startTimestamp));
        startTimestamp = Timestamp.fromMicros(
                Iterables.getLast(timestamps).getMicros());
        assertEquals(timestamps.size(), Timestamps.
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

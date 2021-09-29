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
package com.cinchapi.concourse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Iterables;

/**
 * Tests new API named Audit which returns a time difference greater than
 * Start time provided and less than end time
 * 
 * @author Vijay
 *
 */
public class ReviewTest extends ConcourseIntegrationTest {

    @Test
    public void testAuditRangeSanityCheck() {
        String key = "foo";
        long record = 1;
        client.add(key, 1, record);
        client.add(key, 2, record);
        client.add(key, 3, record);
        Map<Timestamp, List<String>> reviewing = client.review(key, record);
        Timestamp preStart = Iterables.get(reviewing.keySet(), 0);
        Timestamp start = Iterables.get(reviewing.keySet(), 1);
        reviewing = client.review(key, record, start, Timestamp.now());
        assertFalse(reviewing.keySet().contains(preStart));
        assertEquals(2, reviewing.size());
    }

    @Test
    public void testAuditTimestampReturnGreaterThanStartAndLessThanEndCheck() {
        String key = "foo";
        long record = 1;
        client.add(key, 1, record);
        client.add(key, 2, record);
        client.add(key, 3, record);
        client.add(key, 4, record);
        client.add(key, 5, record);
        client.add(key, 6, record);
        Map<Timestamp, List<String>> reviewing = client.review(key, record);
        Timestamp preStart = Iterables.get(reviewing.keySet(), 1);
        Timestamp start = Iterables.get(reviewing.keySet(), 2);
        Timestamp end = Iterables.get(reviewing.keySet(), 4);
        Timestamp postend1 = Iterables.get(reviewing.keySet(), 5);
        reviewing = client.review(key, record, start, end);
        assertFalse(reviewing.keySet().contains(preStart));
        assertFalse(reviewing.keySet().contains(postend1));
        assertEquals(2, reviewing.size());
        Entry<Timestamp, List<String>> entry = null;
        for (int i = 0; i < reviewing.size(); i++) {
            entry = Iterables.get(reviewing.entrySet(), i);
            if(entry.getKey().getMicros() > end.getMicros()) {
                System.out.println("Error\n");
            }
            if(entry.getKey().getMicros() < start.getMicros()) {
                System.out.println("Error\n");
            }
        }
    }

    @Test
    public void testAuditTimestampReturnGreaterThanStartAndLessThanDefaultEndCheck() {
        String key = "foo";
        long record = 1;
        client.add(key, 1, record);
        client.add(key, 2, record);
        client.add(key, 3, record);
        client.add(key, 4, record);
        Map<Timestamp, List<String>> reviewing = client.review(key, record);
        Timestamp preStart = Iterables.get(reviewing.keySet(), 1);
        Timestamp start = Iterables.get(reviewing.keySet(), 2);
        Timestamp end = Iterables.get(reviewing.keySet(), 3);
        reviewing = client.review(key, record, start);
        client.add(key, 5, record);
        client.add(key, 6, record);
        Map<Timestamp, List<String>> newreview = client.review(key, record);
        Timestamp postend1 = Iterables.get(newreview.keySet(), 5);
        assertFalse(reviewing.keySet().contains(preStart));
        assertFalse(reviewing.keySet().contains(postend1));
        assertEquals(2, reviewing.size());
        Entry<Timestamp, List<String>> entry = null;
        for (int i = 0; i < reviewing.size(); i++) {
            entry = Iterables.get(reviewing.entrySet(), i);
            if(entry.getKey().getMicros() > end.getMicros()) {
                System.out.println("Error\n");
            }
            if(entry.getKey().getMicros() < start.getMicros()) {
                System.out.println("Error\n");
            }
        }
    }

    @Test
    public void testAuditTimestampReturnGreaterThanStartAndLessThanEndCheckWithoutKey() {
        String key = "foo";
        long record = 1;
        client.add(key, 1, record);
        client.add(key, 2, record);
        client.add(key, 3, record);
        client.add(key, 4, record);
        client.add(key, 5, record);
        client.add(key, 6, record);
        Map<Timestamp, List<String>> reviewing = client.review(key, record);
        Timestamp preStart = Iterables.get(reviewing.keySet(), 1);
        Timestamp start = Iterables.get(reviewing.keySet(), 2);
        Timestamp end = Iterables.get(reviewing.keySet(), 4);
        Timestamp postend1 = Iterables.get(reviewing.keySet(), 5);
        reviewing = client.review(record, start, end);
        assertFalse(reviewing.keySet().contains(preStart));
        assertFalse(reviewing.keySet().contains(postend1));
        assertEquals(2, reviewing.size());
        Entry<Timestamp, List<String>> entry = null;
        for (int i = 0; i < reviewing.size(); i++) {
            entry = Iterables.get(reviewing.entrySet(), i);
            if(entry.getKey().getMicros() > end.getMicros()) {
                System.out.println("Error\n");
            }
            if(entry.getKey().getMicros() < start.getMicros()) {
                System.out.println("Error\n");
            }
        }
    }

    @Test
    public void testAuditTimestampReturnGreaterThanStartAndLessThanDefaultEndCheckWithoutKey() {
        String key = "foo";
        long record = 1;
        client.add(key, 1, record);
        client.add(key, 2, record);
        client.add(key, 3, record);
        client.add(key, 4, record);
        Map<Timestamp, List<String>> reviewing = client.review(key, record);
        Timestamp preStart = Iterables.get(reviewing.keySet(), 1);
        Timestamp start = Iterables.get(reviewing.keySet(), 2);
        Timestamp end = Iterables.get(reviewing.keySet(), 3);
        reviewing = client.review(record, start);
        client.add(key, 5, record);
        client.add(key, 6, record);
        Map<Timestamp, List<String>> newreview = client.review(key, record);
        Timestamp postend1 = Iterables.get(newreview.keySet(), 5);
        assertFalse(reviewing.keySet().contains(preStart));
        assertFalse(reviewing.keySet().contains(postend1));
        assertEquals(2, reviewing.size());
        Entry<Timestamp, List<String>> entry = null;
        for (int i = 0; i < reviewing.size(); i++) {
            entry = Iterables.get(reviewing.entrySet(), i);
            if(entry.getKey().getMicros() > end.getMicros()) {
                System.out.println("Error\n");
            }
            if(entry.getKey().getMicros() < start.getMicros()) {
                System.out.println("Error\n");
            }
        }
    }
}

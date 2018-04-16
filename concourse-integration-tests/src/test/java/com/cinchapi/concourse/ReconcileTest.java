/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.util.ArrayList;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Sets;

/**
 * Unit test for API method that sets the values of
 * the key in record exactly same as the input values
 */
public class ReconcileTest extends ConcourseIntegrationTest {

    @Test
    public void testReconcileEmptyValues() {
        client.reconcile("foo", 17, Sets.newHashSet());
        Assert.assertTrue(client.select("foo", 17).isEmpty());
    }

    @Test
    public void testReconcile() {
        String field = "testKey"; // key name
        long r = 1; // record
        client.add(field, "A", r);
        client.add(field, "C", r);
        client.add(field, "D", r);
        client.add(field, "E", r);
        client.add(field, "F", r);
        Set<String> values = Sets.newHashSet("A", "B", "D", "G");
        client.reconcile(field, r, values);
        Set<String> actual = client.select(field, r);
        Assert.assertEquals(values, actual);
    }

    @Test
    public void testReconcileVarargs() {
        String field = "testKey2";
        long r = 2;
        client.add(field, 100, r);
        client.add(field, 101, r);
        client.add(field, 102, r);
        client.reconcile(field, r, 102, 103, 104);
        Set<Integer> actual = client.select(field, r);
        Set<Integer> expected = Sets.newHashSet();
        expected.add(102);
        expected.add(103);
        expected.add(104);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReconcileDuplicates() {
        ArrayList<Integer> values = new ArrayList<>();
        values.add(1);
        values.add(1);
        client.reconcile("testKey", 5, values);
        Assert.assertEquals(1, client.select("testKey", 5).size());
    }
}

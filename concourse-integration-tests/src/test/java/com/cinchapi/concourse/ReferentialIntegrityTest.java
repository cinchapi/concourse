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
package com.cinchapi.concourse;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests to verify referential integrity
 * 
 * @author Jeff Nelson
 */
public class ReferentialIntegrityTest extends ConcourseIntegrationTest {

    @Test
    public void testCannotAddCircularLinks() {
        Assert.assertFalse(client.link("foo", 1, 1));
    }

    @Test
    public void testCanAddNonCircularLinks() {
        Assert.assertTrue(client.link("foo", 2, 1));
    }

    @Test
    public void testCannotManuallyAddCircularLinks() {
        Assert.assertFalse(client.add("foo", Link.to(1), 1));
    }

    @Test
    public void testCannotManuallyAddCircularLinksManyRecords() {
        Map<Long, Boolean> result = client.add("foo", Link.to(1),
                ImmutableSet.of(1L, 2L));
        Assert.assertTrue(result.get(2L));
        Assert.assertFalse(result.get(1L));
    }

    @Test
    public void testCannotAddCircularLinksManyRecords() {
        Map<Long, Boolean> result = client.link("foo", ImmutableSet.of(1L, 2L),
                1);
        Assert.assertTrue(result.get(2L));
        Assert.assertFalse(result.get(1L));
    }

}

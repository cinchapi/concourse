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
package com.cinchapi.concourse;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for findOrInsert methods.
 *
 * @author Jeff Nelson
 */
public class FindOrInsertTest extends ConcourseIntegrationTest {

    @Test
    public void testFindOrInsertCclWithLocalResolutionNoMatch() { // CON-609
        String ccl = "ssn = $ssn";
        Map<String, Object> data = ImmutableMap.of("name", "Jeff Nelson", "ssn",
                1);
        long expected = client.add("name", "Jeff Nelson");
        long actual = client.findOrInsert(ccl, data);
        Assert.assertNotEquals(expected, actual);
    }

    @Test
    public void testFindOrInsertCclWithLocalResolutionYesMatch() { // CON-609
        String ccl = "ssn = $ssn";
        Map<String, Object> data = ImmutableMap.of("name", "Jeff Nelson", "ssn",
                1);
        long expected = client.insert(data);
        long actual = client.findOrInsert(ccl, data);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = ParseException.class)
    public void testFindOrInsertCclWithLocalResolutionBadVariable() {
        String ccl = "ssn = $id";
        Map<String, Object> data = ImmutableMap.of("name", "Jeff Nelson", "ssn",
                1);
        long expected = client.insert(data);
        long actual = client.findOrInsert(ccl, data);
        Assert.assertEquals(expected, actual);
    }

}

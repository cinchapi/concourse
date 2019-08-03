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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Sets;

/**
 * Unit tests for the describe API methods.
 * 
 * @author Gerald Nash
 */
public class DescribeTest extends ConcourseIntegrationTest {

    @Test
    public void testDescribeNoTimestamp() {
        client.add("name", "Gerald");
        client.add("address", "123 Main St");
        client.add("title", "Person");

        Set<String> expected = Sets.newHashSet("name", "address", "title");
        Set<String> actual = client.describe();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testDescribeWithTimestamp() {
        client.add("name", "Gerald");
        client.add("address", "123 Main St");
        Timestamp stamp = Timestamp.now();
        client.add("title", "Person");

        Set<String> expected = Sets.newHashSet("name", "address");
        Set<String> actual = client.describe(stamp);
        Assert.assertEquals(expected, actual);
    }

}

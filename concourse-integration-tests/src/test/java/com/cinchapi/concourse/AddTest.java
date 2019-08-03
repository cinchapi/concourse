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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;

/**
 * Unit test for API method that adds data to an empty record.
 */
public class AddTest extends ConcourseIntegrationTest {

    @Test
    public void testAdd() {
        long value = TestData.getLong();
        long record = client.add("foo", value);
        Assert.assertEquals(value, (long) client.get("foo", record));
        Assert.assertEquals("foo",
                Iterables.getOnlyElement(client.describe(record)));
    }

}

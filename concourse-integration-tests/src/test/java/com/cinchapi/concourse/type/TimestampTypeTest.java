/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.type;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;

/**
 * Unit tests to verify writing and reading timestamps as a data type.
 * 
 * @author Jeff Nelson
 */
public class TimestampTypeTest extends ConcourseIntegrationTest {

    @Test
    public void testWriteTimestampFromMicros() {
        Timestamp expected = Timestamp.fromMicros(1234);
        long record = client.add("join_date", expected);
        Timestamp actual = client.get("join_date", record);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testWriteTimestampFromString() {
        Timestamp expected = Timestamp.fromString("last week");
        long record = client.add("join_date", expected);
        Timestamp actual = client.get("join_date", record);
        Assert.assertEquals(expected, actual);
        // TODO: problem here is that hallow timestamps have their microseconds
        // evaluated client side whereas in any other case the microseconds
        // would be resolved server side
    }

}
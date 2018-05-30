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
package com.cinchapi.concourse.type;

import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.InvalidArgumentException;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.ImmutableMap;

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

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteTimestampFromString() {
        Timestamp expected = Timestamp.fromString("last week");
        long record = client.add("join_date", expected);
        Timestamp actual = client.get("join_date", record);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testInsertJsonTimestamp() {
        Map<String, Object> data = ImmutableMap.of("birthdate",
                "|December 30, 1987|");
        String json = Convert.mapToJson(data);
        long record = client.insert(json).iterator().next();
        Assert.assertTrue(client.get("birthdate", record) instanceof Timestamp);
    }

    @Test
    public void testInsertJsonTimestampWithFormatter() {
        Map<String, Object> data = ImmutableMap.of("birthdate",
                "|December 30, 1987|MMM DD, YYYY|");
        String json = Convert.mapToJson(data);
        long record = client.insert(json).iterator().next();
        Timestamp expected = Timestamp.parse("December 30, 1987",
                DateTimeFormat.forPattern("MMM DD, YYYY"));
        Assert.assertEquals(expected, client.get("birthdate", record));
    }

    @Test(expected = InvalidArgumentException.class)
    public void testInsertJsonTimestampIllegalArgumentExceptionPropagated() {
        Map<String, Object> data = ImmutableMap.of("birthdate",
                "|December 30, 1987|dfasdfasfas|");
        String json = Convert.mapToJson(data);
        long record = client.insert(json).iterator().next();
        Timestamp expected = Timestamp.parse("December 30, 1987",
                DateTimeFormat.forPattern("MMM DD, YYYY"));
        Assert.assertEquals(expected, client.get("birthdate", record));
    }

}
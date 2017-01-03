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
package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;

/**
 * Unit tests for {@link Timestamp}.
 * 
 * @author Jeff Nelson
 */
public class TimestampTest extends ConcourseBaseTest {

    @Test
    public void testStringTimestamp() {
        String expected = "yesterday";
        Timestamp time = Timestamp.fromString(expected);
        Assert.assertTrue(time.isString());
        Assert.assertEquals(expected, time.toString());
    }

    @Test(expected = IllegalStateException.class)
    public void testStringTimestampCannotGetMicros() {
        Timestamp.fromString("yesterday").getMicros();
    }

    @Test(expected = IllegalStateException.class)
    public void testStringTimestampCannotGetJoda() {
        Timestamp.fromString("yesterday").getJoda();
    }

}

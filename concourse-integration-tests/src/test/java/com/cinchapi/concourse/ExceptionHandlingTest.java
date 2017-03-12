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

import com.cinchapi.concourse.InvalidArgumentException;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;

/**
 * Unit tests to ensure that error scenarios are handled and properly propagated
 * to the client. Proper handling basically means that the client connection
 * does not die because of an exception that can be caught.
 * 
 * @author Jeff Nelson
 */
public class ExceptionHandlingTest extends ConcourseIntegrationTest {

    @Test
    public void testHandleIllegalKey() {
        try {
            client.add("key with space", 1);
            Assert.fail("Expecting an InvalidArgumentException");
        }
        catch (InvalidArgumentException e) {
            // Make sure the client connection didn't die
            long record = client.add("foo", "bar");
            Assert.assertEquals("bar", client.get("foo", record));
        }
    }

}

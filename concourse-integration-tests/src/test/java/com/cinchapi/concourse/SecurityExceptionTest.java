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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.SecurityException;

/**
 * Test security exception which occurs when user session
 * is invalidated from concourse server and the user is kicked
 * out of CaSH session.
 * 
 * @author knd
 *
 */
public class SecurityExceptionTest extends ConcourseIntegrationTest {

    @Test
    public void testTSecurityExceptionIsThrown() {
        try {
            grantAccess("admin", "admin2");
            client.add("name", "brad", 1); // this should throw
                                           // SecurityException
            Assert.fail("Expecting SecurityException");
        }
        catch (Exception e) {
            if(e.getCause() != null
                    & e.getCause() instanceof SecurityException) {
                Assert.assertTrue(true);
            }
            else {
                throw e;
            }
        }
    }

}

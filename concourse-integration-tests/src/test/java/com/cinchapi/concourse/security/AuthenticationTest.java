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
package com.cinchapi.concourse.security;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.TestData;

/**
 * Tests for authentication workflows.
 * 
 * @author Jeff Nelson
 */
public class AuthenticationTest extends ConcourseIntegrationTest {

    @Test
    public void testDisabledUserCannotAuthenticate() {
        String username = TestData.getSimpleString();
        grantAccess(username, username);
        disableAccess(username);
        try {
            Concourse.connect(SERVER_HOST, SERVER_PORT, username, username);
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testDisabledUserSessionsEndedImmediately() {
        String username = TestData.getSimpleString();
        grantAccess(username, username);
        Concourse con = Concourse.connect(SERVER_HOST, SERVER_PORT, username,
                username);
        con.getServerEnvironment();
        disableAccess(username);
        try {
            con.getServerEnvironment();
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }

    }

}

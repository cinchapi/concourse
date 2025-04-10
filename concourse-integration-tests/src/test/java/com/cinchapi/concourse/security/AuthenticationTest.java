/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.AccessToken;
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
        createUser(username, username, "admin");
        disableUser(username);
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
        createUser(username, username, "admin");
        Concourse con = Concourse.connect(SERVER_HOST, SERVER_PORT, username,
                username);
        con.getServerEnvironment();
        disableUser(username);
        try {
            con.getServerEnvironment();
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testCannotInvokeMethodWithInvalidAccessToken() {
        AccessToken token = new AccessToken(
                ByteBuffers.fromUtf8String(ByteBuffers.encodeAsHexString(
                        ByteBuffers.fromUtf8String("fake"))));
        Reflection.set("creds", token, client);
        Reflection.set("password", ByteBuffers.fromUtf8String(""), client); // must
                                                                            // change
                                                                            // the
                                                                            // password
                                                                            // so
                                                                            // the
                                                                            // automatic
                                                                            // re-authentication
                                                                            // doesn't
                                                                            // work
        try {
            client.getServerEnvironment();
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

}

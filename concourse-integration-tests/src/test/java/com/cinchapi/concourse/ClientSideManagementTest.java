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

/**
 * Unit tests that verify client-side management operations
 *
 * @author Jeff Nelson
 */
public class ClientSideManagementTest extends ConcourseIntegrationTest {

    @Test
    public void testSuccesfulInvocation() {
        client.manage().createUser("jeff", "jeff", "user");
    }

    @Test(expected = ManagementException.class)
    public void testNonAdminUserInvocationThrowsSecurityException() {
        createUser("jeff", "jeff", "user");
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "jeff",
                "jeff");
        client2.manage().createUser("foo", "foofoo", "user");
    }

    @Test
    public void testCreateUserAndSetRole() {
        client.manage().createUser("jeff", "jeff", "user");
        client.manage().grant("jeff", "write");
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "jeff",
                "jeff");
        client2.verifyOrSet("name", "jeff", 1);
        Assert.assertEquals("jeff", client.get("name", 1));
    }

}

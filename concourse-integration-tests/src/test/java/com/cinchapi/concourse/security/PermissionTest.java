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
package com.cinchapi.concourse.security;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.PermissionException;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableSet;

/**
 * Unit test for permission enforcement
 *
 * @author Jeff Nelson
 */
public class PermissionTest extends ConcourseIntegrationTest {

    @Test(expected = PermissionException.class)
    public void testPermissionsAreEnforced() {
        createUser("jeff", "jeff", "user");
        Concourse concourse = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "jeff", "jeff");
        concourse.getServerEnvironment();
    }

    @Test
    public void testDriverIsUsableAfterPermissionException() {
        createUser("jeff", "jeff", "user");
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "jeff",
                "jeff");
        grant("jeff", "read", "");
        long record = client.add("foo", "foo");
        try {
            client2.set("foo", "bar", record);
            Assert.fail();
        }
        catch (PermissionException e) {
            Assert.assertEquals("foo", client2.get("foo", record));
        }
    }

    @Test
    public void testPermissionChangeTakesImmediateEffect() {
        createUser("jeff", "jeff", "user");
        grant("jeff", "write", "production");
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "jeff",
                "jeff", "production");
        long record = client2.add("name", "jeff");
        grant("jeff", "read", "production");
        try {
            client2.add("name", "jeff");
            Assert.fail();
        }
        catch (PermissionException e) {
            Assert.assertEquals(ImmutableSet.of(record), client2.inventory());
        }
    }

}

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
package com.cinchapi.concouse.server.upgrade;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.security.Role;
import com.cinchapi.concourse.security.UserService;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.test.UpgradeTest;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TSets;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit test for {@link Upgrade0_9_0_3}
 *
 * @author Jeff Nelson
 */
public class UpgradeTask0_9_0_3Test extends UpgradeTest {

    @Override
    protected String getInitialServerVersion() {
        return "0.8.2";
    }

    @Override
    protected void preUpgradeActions() {
        int count = TestData.getScaleCount();
        TSets.sequence(0, count - 1)
                .forEach(ignore -> server.executeCli("users", "create",
                        Random.getSimpleString(), "--set-password f00b@R",
                        "-u admin --password admin"));

    }

    @Test
    public void testEachUserIsAssignedAdminRole() {
        Path accessFile = Paths.get(server.getInstallDirectory(),
                GlobalState.ACCESS_FILE);
        UserService users = UserService
                .create(accessFile.toAbsolutePath().toString());
        users.forEachUser(username -> Assert.assertEquals(Role.ADMIN,
                users.getRole(username)));
    }

}

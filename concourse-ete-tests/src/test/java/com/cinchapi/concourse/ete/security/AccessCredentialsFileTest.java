/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.ete.security;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.ManagedConcourseServer;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.FileOps;

/**
 * Unit test for configurable access credentials file preference.
 *
 * @author jeff
 */
public class AccessCredentialsFileTest extends ClientServerTest {

    @Test
    public void testAccessCredentialsFileCorrectness() {
        server.executeCli("users",
                "create jeff --set-password admin --set-role ADMIN --username admin --password admin");
        server.connect("jeff", "admin");
        server.stop();
        String file = FileOps.tempFile();
        server.config().set("access_credentials_file", file);
        server.start();
        try {
            server.connect("jeff", " admin");
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        server.executeCli("users",
                "create ashleah --set-password admin --set-role ADMIN --username admin --password admin");
        server.connect("ashleah", "admin");
        server.stop();
        // New server installation should revert back to default creds which
        // doesn't have data for jeff or ashleah
        server = ManagedConcourseServer
                .manageExistingServer(server.getInstallDirectory());
        server.start();
        try {
            server.connect("jeff", " admin");
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            server.connect("ashleah", " admin");
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}

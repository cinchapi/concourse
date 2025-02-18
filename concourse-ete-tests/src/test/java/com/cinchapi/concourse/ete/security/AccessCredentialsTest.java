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
package com.cinchapi.concourse.ete.security;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;

/**
 * Unit test for configurable access credentials file preference.
 *
 * @author Jeff Nelson
 */
public class AccessCredentialsTest extends ClientServerTest {

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
        server = reinstallServer();
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

    @Test
    public void testChangeRootUsername() {
        String username = Random.getSimpleString();
        server = reinstallServer(config -> {
            config.set("init_root_username", username);
        });
        try {
            server.connect(username, "admin");
        }
        catch (Exception e) {
            Assert.fail(
                    "Authentication should succeed with configured credentials");
        }
        try {
            server.connect("admin", "admin");
            Assert.fail("Default admin credentials should not work");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testChangeRootPassword() {
        String password = Random.getSimpleString();
        server = reinstallServer(config -> {
            config.set("init_root_password", password);
        });
        try {
            server.connect("admin", password);
        }
        catch (Exception e) {
            Assert.fail(
                    "Authentication should succeed with configured credentials");
        }
        try {
            server.connect("admin", "admin");
            Assert.fail("Default admin credentials should not work");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testChangeRootUsernameAndPassword() {
        String username = Random.getSimpleString();
        String password = Random.getSimpleString();
        server = reinstallServer(config -> {
            config.set("init_root_password", password);
            config.set("init_root_username", username);
        });
        try {
            server.connect(username, password);
        }
        catch (Exception e) {
            Assert.fail(
                    "Authentication should succeed with configured credentials");
        }
        try {
            server.connect("admin", "admin");
            Assert.fail("Default admin credentials should not work");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testInitDotRootObjectConfigTakesPrecedence() {
        String initUsername = Random.getSimpleString();
        String initPassword = Random.getSimpleString();
        String rootUsername = Random.getSimpleString();
        String rootPassword = Random.getSimpleString();
        server = reinstallServer(config -> {
            config.set("init_root_password", initPassword);
            config.set("init_root_username", initUsername);
            config.set("init.root.password", rootPassword);
            config.set("init.root.username", rootUsername);

        });
        try {
            server.connect(rootUsername, rootPassword);
        }
        catch (Exception e) {
            Assert.fail(
                    "Authentication should succeed with init.root credentials");
        }
        try {
            server.connect(initUsername, initPassword);
            Assert.fail("init.root credentials should take precedence");
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            server.connect("admin", "admin");
            Assert.fail("Default admin credentials should not work");
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

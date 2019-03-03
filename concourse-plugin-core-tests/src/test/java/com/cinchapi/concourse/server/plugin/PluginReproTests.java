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
package com.cinchapi.concourse.server.plugin;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;
import com.google.common.collect.Sets;

/**
 * Unit tests that reproduce bugs found in the plugin-core framework
 *
 * @author Jeff Nelson
 */
public class PluginReproTests extends ClientServerTest implements PluginTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Test
    public void testCloud_1Repro() { // http://jira.cinchapi.com/browse/CLOUD-1
        // "Change" the password to invalidate the client's current token
        client.add("name", "jeff", 17);
        server.executeCli("users", "password", "--set-password", "admin",
                "--username", "admin", "--password", "admin", "admin");
        Set<Long> records = client.invokePlugin(TestPlugin.class.getName(),
                "inventory");
        Assert.assertEquals(records, Sets.newHashSet(17L)); // Verify that the
                                                            // access token is
                                                            // renewed and the
                                                            // correct value can
                                                            // be retrieved
    }

    @Test
    public void testReproCON_605() {
        String environment = "foo bar &!* baz";
        client = Concourse.connect("localhost", server.getClientPort(), "admin",
                "admin", environment);
        Assert.assertNotEquals(environment,
                client.invokePlugin(TestPlugin.class.getName(), "environment"));
    }

    @Test
    public void testReproCON_606() {
        String environment = client.invokePlugin(TestPlugin.class.getName(),
                "environment");
        Assert.assertEquals("default", environment);
    }

}

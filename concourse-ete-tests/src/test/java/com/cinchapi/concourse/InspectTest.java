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

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;

public class InspectTest extends ClientServerTest implements PluginTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Test
    public void testInspect() {
        final String TEST_PLUGIN_NAME = "com.cinchapi.concourse.CON569TestPlugin01";

        Map<String, Set<String>> descriptions = client.inspect();

        descriptions.forEach((plugin, methods) -> {
            System.out.println(
                    "Plugin: " + plugin + " Methods: " + methods.toString());
        });

        Assert.assertFalse("Descriptions contains no available plugins.",
                descriptions.isEmpty());
        Assert.assertTrue("Descriptions does not contain the test plugin.",
                descriptions.containsKey(TEST_PLUGIN_NAME));
        Assert.assertTrue(
                "CON569TestPlugin01 does not contain say(java.lang.String)",
                descriptions.get(TEST_PLUGIN_NAME)
                        .contains("say(java.lang.String )"));
        Assert.assertTrue("CON569TestPlugin01 does not contain bye()",
                descriptions.get(TEST_PLUGIN_NAME).contains("bye()"));
    }

}

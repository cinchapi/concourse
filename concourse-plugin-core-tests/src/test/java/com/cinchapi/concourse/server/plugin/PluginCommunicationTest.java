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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.data.ObjectResultDataset;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;

/**
 * Unit tests for communication between plugins and Concourse Server.
 *
 * @author Jeff Nelson
 */
public class PluginCommunicationTest extends ClientServerTest
        implements PluginTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Test
    public void testPluginReceivesDatasetFromConcourseRuntime() {
        String clazz = client.invokePlugin(TestPlugin.class.getName(),
                "getResultDatasetClassName");
        Assert.assertEquals(ObjectResultDataset.class.getName(), clazz);
    }

}

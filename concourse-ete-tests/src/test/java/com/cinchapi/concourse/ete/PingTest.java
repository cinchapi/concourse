/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.ete;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.google.common.collect.ImmutableSet;


/**
 * Unit tests for the ping functionality in Concourse
 *
 * @author Jeff Nelson
 */
public class PingTest extends ClientServerTest {

    @Test
    public void testPingWhenRunning() {
        Assert.assertTrue(client.ping());
    }

    @Test
    public void testPingWhenStopped() {
        Assert.assertTrue(client.ping());
        server.stop();
        Assert.assertFalse(client.ping());
    }
    
    @Test
    public void testPingWhenRestarted() {
        Assert.assertTrue(client.ping());
        server.restart();
        Assert.assertFalse(client.ping());
    }
    
    @Test
    public void testPingWhenRestartedAndReconnected() {
        Assert.assertTrue(client.ping());
        server.restart();
        Assert.assertFalse(client.ping());
        client = server.connect();
        Assert.assertTrue(client.ping());
    }
    
    @Test
    public void testPingExemptFromExceptions() {
        Assert.assertTrue(client.ping());
        Assert.assertEquals(ImmutableSet.of(), client.inventory());
        server.stop();
        try {
            client.inventory();
            Assert.fail();
        }
        catch(Exception e) {
            Assert.assertTrue(true);
        }
        Assert.assertFalse(client.ping());
    }

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}

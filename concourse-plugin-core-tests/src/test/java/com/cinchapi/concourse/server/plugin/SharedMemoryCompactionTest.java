/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.PluginTest;
import com.cinchapi.concourse.util.Random;

/**
 * ETE test for shared memory compaction in Concourse Server and JVM.
 * 
 * @author Jeff Nelson
 */
public class SharedMemoryCompactionTest extends ClientServerTest implements PluginTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }
    
    @Test
    public void testCompactionDoesntDropMessages(){
        long stop = System.currentTimeMillis() + (6000 * 2);
        while(System.currentTimeMillis() < stop){
            String expected = Random.getString();
            String actual = client.invokePlugin("com.cinchapi.concourse.server.plugin.TestPlugin", "echo", expected);
            System.out.println(actual);
            Assert.assertEquals(expected, actual);
        }
    }

}

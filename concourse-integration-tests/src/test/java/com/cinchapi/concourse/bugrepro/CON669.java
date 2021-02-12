/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.bugrepro;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.time.Time;

/**
 * Unit tests to reproduce the issues of CON-669.
 *
 * @author Jeff Nelson
 */
public class CON669 extends ConcourseIntegrationTest {

    @Test
    public void testConsistencyOfWideReadsWithConcurrentWrites()
            throws Exception {
        int threads = 10;
        ConnectionPool connections = ConnectionPool.newCachedConnectionPool(
                SERVER_HOST, SERVER_PORT, "admin", "admin");
        try {
            client.set("count", 1L, 1);
            AtomicBoolean done = new AtomicBoolean(false);
            AtomicBoolean passed = new AtomicBoolean(true);
            Thread reader = new Thread(() -> {
                while (!done.get()) {
                    Concourse con = connections.request();
                    try {
                        Assert.assertFalse(
                                con.select(1).get("count").isEmpty());
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        passed.set(false);
                    }
                    finally {
                        connections.release(con);
                    }
                }
            });
            reader.start();
            for (int i = 0; i < threads; ++i) {
                Thread t = new Thread(() -> {
                    while (!done.get()) {
                        Concourse con = connections.request();
                        try {
                            long expected = (long) con.select(1).get("count")
                                    .iterator().next();
                            con.verifyAndSwap("count", expected, 1, Time.now());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            passed.set(false);
                        }
                        finally {
                            connections.release(con);
                        }
                    }
                });
                t.start();
            }
            Threads.sleep(3000);
            done.set(true);
            Assert.assertTrue(passed.get());
        }
        finally {
            while (!connections.isClosed()) {
                try {
                    connections.close();
                }
                catch (IllegalStateException e) {
                    Threads.sleep(100);
                }
            }
        }
    }

    @Test
    public void testConcurrentAtomicWritesToDifferentKeysInRecord() {
        Concourse client2 = Concourse.copyExistingConnection(client);
        try {
            client.stage();
            client.add("a", "a", 1);
            client2.stage();
            client2.add("b", "b", 1);
            Assert.assertTrue(client.commit());
            Assert.assertTrue(client2.commit());
        }
        finally {
            client2.exit();
        }
    }

}

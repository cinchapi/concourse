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
package com.cinchapi.concourse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.FixedConnectionPool;
import com.cinchapi.concourse.util.StandardActions;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link FixedConnectionPool}.
 * 
 * @author Jeff Nelson
 */
public class FixedConnectionPoolTest extends ConnectionPoolTest {

    @Test
    public void testBlockUnitlConnectionAvailable() {
        List<Concourse> toReturn = Lists.newArrayList();
        for (int i = 0; i < POOL_SIZE; i++) {
            toReturn.add(connections.request());
        }
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                System.out.println("Waiting for next available connection...");
                Concourse connection = connections.request();
                System.out.println("Finally acquired connection");
                connections.release(connection);
            }

        });
        thread.start();
        StandardActions.wait(60, TimeUnit.MILLISECONDS);
        for (Concourse concourse : toReturn) {
            // must return all the connections so the pool can shutdown after
            // the test
            connections.release(concourse);
        }
    }

    @Test
    public void testNotHasAvailableConnectionWhenAllInUse() {
        List<Concourse> toReturn = Lists.newArrayList();
        for (int i = 0; i < POOL_SIZE; i++) {
            toReturn.add(connections.request());
        }
        Assert.assertFalse(connections.hasAvailableConnection());
        for (Concourse concourse : toReturn) {
            // must return all the connections so the pool can shutdown after
            // the test
            connections.release(concourse);
        }
    }

    @Override
    protected ConnectionPool getConnectionPool() {
        return ConnectionPool.newFixedConnectionPool(SERVER_HOST, SERVER_PORT,
                USERNAME, PASSWORD, POOL_SIZE);
    }

    @Override
    protected ConnectionPool getConnectionPool(String env) {
        return ConnectionPool.newFixedConnectionPool(SERVER_HOST, SERVER_PORT,
                USERNAME, PASSWORD, env, POOL_SIZE);
    }

}

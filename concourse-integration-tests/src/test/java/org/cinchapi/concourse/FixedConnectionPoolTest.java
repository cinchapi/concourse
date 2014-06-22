/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.util.StandardActions;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link FixedConnectionPool}.
 * 
 * @author jnelson
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

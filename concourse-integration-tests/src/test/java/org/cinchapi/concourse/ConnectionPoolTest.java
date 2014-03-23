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

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConnectionPool;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ConnectionPool}.
 * 
 * @author jnelson
 */
public abstract class ConnectionPoolTest extends ConcourseIntegrationTest {

    protected static final int POOL_SIZE = 3;
    protected static final String USERNAME = "admin";
    protected static final String PASSWORD = "admin";

    /**
     * The {@link ConnectionPool} which is instantiated and clean up before and
     * after each test.
     */
    protected ConnectionPool connections = null;

    @Override
    protected void afterEachTest() {
        super.afterEachTest();
        try {
            connections.close();
            connections = null;
        }
        catch (Exception e) {}
    }

    @Override
    protected void beforeEachTest() {
        super.beforeEachTest();
        connections = getConnectionPool();
    }

    @Test
    public void testHasAvailableConnection() {
        Assert.assertTrue(connections.hasAvailableConnection());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotReturnConnectionNotRequestedFromPool() {
        connections.release(Concourse.connect(SERVER_HOST, SERVER_PORT,
                USERNAME, PASSWORD));
    }

    /**
     * Return a {@link ConnectionPool} to use in a unit test.
     * 
     * @return the ConnectionPool
     */
    protected abstract ConnectionPool getConnectionPool();

}

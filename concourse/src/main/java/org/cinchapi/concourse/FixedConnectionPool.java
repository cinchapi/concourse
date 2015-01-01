/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import com.google.common.base.Throwables;

/**
 * A {@link ConnectionPool} with a fixed number of connections. If all the
 * connections from the pool are active, subsequent request attempts will block
 * until a connection is returned.
 * 
 * @author jnelson
 */
class FixedConnectionPool extends ConnectionPool {

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     */
    protected FixedConnectionPool(String host, int port, String username,
            String password, int poolSize) {
        super(host, port, username, password, poolSize);
    }

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @param poolSize
     */
    protected FixedConnectionPool(String host, int port, String username,
            String password, String environment, int poolSize) {
        super(host, port, username, password, environment, poolSize);
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return new ArrayBlockingQueue<Concourse>(size);
    }

    @Override
    protected Concourse getConnection() {
        try {
            return ((BlockingQueue<Concourse>) available).take();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

}

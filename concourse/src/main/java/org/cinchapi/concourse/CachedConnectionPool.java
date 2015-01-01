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
import java.util.concurrent.Callable;
import org.cinchapi.concourse.util.ConcurrentLoadingQueue;

/**
 * A {@link ConnectionPool} that has no limit on the number of connections it
 * can managed, but will try to use previously requested connections, where
 * possible.
 * 
 * @author jnelson
 */
class CachedConnectionPool extends ConnectionPool {

    // Connection Info
    private final String host;
    private final int port;
    private String username;
    private final String password;
    private final String environment;

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     */
    protected CachedConnectionPool(String host, int port, String username,
            String password, int poolSize) {
        this(host, port, username, password, "", poolSize);
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
    protected CachedConnectionPool(String host, int port, String username,
            String password, String environment, int poolSize) {
        super(host, port, username, password, environment, poolSize);
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.environment = environment;
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return ConcurrentLoadingQueue.create(new Callable<Concourse>() {

            @Override
            public Concourse call() throws Exception {
                return Concourse.connect(host, port, username, password,
                        environment);
            }

        });
    }

    @Override
    protected Concourse getConnection() {
        return available.poll();
    }

}

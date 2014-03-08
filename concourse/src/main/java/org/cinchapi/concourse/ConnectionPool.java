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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.config.ConcourseClientPreferences;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A {@link ConnectionPool} manages multiple concurrent connections to Concourse
 * for a single user. Generally speaking, a ConnectionPool is handy in long
 * running server API endpoints that want to reduce the overhead of creating a
 * new client connection for every asynchronous request. Using a ConnectionPool,
 * those applications can maintain a finite number of connections while ensuring
 * the resources are disconnected gracefully when necessary.
 * </p>
 * <h2>Usage</h2>
 * 
 * <pre>
 * Concourse concourse = pool.request();
 * try {
 *  ...
 * }
 * finally {
 *  pool.release();
 * }
 * ...
 * // All the threads with connections are done
 * pool.shutdown()
 * </pre>
 * 
 * @author jnelson
 */
@ThreadSafe
public final class ConnectionPool implements AutoCloseable {

    // NOTE: This class does not define #hashCode or #equals because the
    // defaults are the desired behaviour

    /**
     * Return a new {@link ConnectionPool} that provides connections to the
     * Concourse instance defined in the client {@code prefs} on behalf of the
     * user defined in the client {@code prefs}.
     * 
     * @param prefs
     * @return the ConnectionPool
     */
    public static ConnectionPool newConnectionPool(String prefs) {
        return newConnectionPool(prefs, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides {@code poolSize}
     * connections to the Concourse instance defined in the client {@code prefs}
     * on behalf of the user defined in the client {@code prefs}.
     * 
     * @param prefs
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newConnectionPool(String prefs, int poolSize) {
        ConcourseClientPreferences cp = ConcourseClientPreferences.load(prefs);
        return new ConnectionPool(cp.getHost(), cp.getPort(), cp.getUsername(),
                new String(cp.getPassword()), poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} that provides connections to the
     * Concourse instance at {@code host}:{@code port} on behalf of the user
     * identified by {@code username} and {@code password}.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @return the ConnectionPool
     */
    public static ConnectionPool newConnectionPool(String host, int port,
            String username, String password) {
        return new ConnectionPool(host, port, username, password,
                DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides {@code poolSize}
     * connections to the Concourse instance at {@code host}:{@code port} on
     * behalf of the user identified by {@code username} and {@code password}.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     * @return
     */
    public static ConnectionPool newConnectionPool(String host, int port,
            String username, String password, int poolSize) {
        return new ConnectionPool(host, port, username, password, poolSize);
    }

    /**
     * The default connection pool size.
     */
    private static final int DEFAULT_POOL_SIZE = 10;

    /**
     * The list of connections that are available for use.
     */
    private final List<Concourse> available = Lists.newArrayList();

    /**
     * The list of connections that are currently in use and are waiting to be
     * returned.
     */
    private final List<Concourse> taken = Lists.newArrayList();

    /**
     * A flag to indicate if the pool is currently open and operational.
     */
    private boolean open = true;

    /**
     * The lock that is used for concurrency control.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     */
    private ConnectionPool(String host, int port, String username,
            String password, int poolSize) {
        for (int i = 0; i < poolSize; i++) {
            available.add(Concourse.connect(host, port, username, password));
        }
        // Ensure that the client connections are forced closed when the JVM is
        // shutdown in case the user does not properly shutdown the pool
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                if(open) {
                    for (Concourse concourse : available) {
                        concourse.exit();
                    }
                    for (Concourse concourse : taken) {
                        concourse.exit();
                    }
                }
            }

        }));
    }

    /**
     * Return {@code true} if the pool has any available connections.
     * 
     * @return {@code true} if there are one or more available connections
     */
    public boolean hasAvailableConnection() {
        Preconditions.checkState(open, "Connection pool is closed");
        lock.readLock().lock();
        try {
            return !available.isEmpty();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Request a connection from the pool and block until one is available and
     * returned.
     * 
     * @return a connection
     */
    public Concourse request() {
        Preconditions.checkState(open, "Connection pool is closed");
        while (!hasAvailableConnection()) {
            continue;
        }
        lock.writeLock().lock();
        try {
            Concourse connection = available.remove(0);
            taken.add(connection);
            return connection;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return a previously requested connection back to the pool.
     * 
     * @param connection
     */
    public void release(Concourse connection) {
        Preconditions.checkState(open, "Connection pool is closed");
        lock.writeLock().lock();
        try {
            int index = taken.indexOf(connection);
            if(index != -1) {
                taken.remove(index);
                available.add(connection);
            }
            else {
                throw new IllegalArgumentException(
                        "Cannot return the connection because it was not "
                                + "previously requested from this pool");
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws Exception {
        lock.writeLock().lock();
        try {
            Preconditions.checkState(open, "Connection pool is closed");
            Preconditions.checkState(taken.isEmpty(),
                    "Cannot shutdown the connection pool "
                            + "until all the connections have been returned");
            open = false;
            for (Concourse connection : available) {
                connection.exit();
            }
        }
        finally {
            lock.writeLock().unlock();
        }

    }

}

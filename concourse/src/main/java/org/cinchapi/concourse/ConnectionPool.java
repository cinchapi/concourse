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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.config.ConcourseClientPreferences;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
 *  pool.release(concourse);
 * }
 * ...
 * // All the threads with connections are done
 * pool.shutdown()
 * </pre>
 * 
 * @author jnelson
 */
@ThreadSafe
public abstract class ConnectionPool implements AutoCloseable {

    // NOTE: This class does not define #hashCode or #equals because the
    // defaults are the desired behaviour

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance described in the
     * {@code concourse_client.prefs} file located in the working directory or
     * the default connection info if no such file exists, but will try to use
     * previously created connections before establishing new ones for any
     * request.
     * 
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool() {
        return newCachedConnectionPool(DEFAULT_PREFS_FILE);
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance at {@code host}:
     * {@code port} on behalf of the user identified by {@code username} and
     * {@code password}, but will try to use previously created connections
     * before establishing new ones for any request.
     * 
     * @param prefs
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(String prefs) {
        ConcourseClientPreferences cp = ConcourseClientPreferences.load(prefs);
        return new CachedConnectionPool(cp.getHost(), cp.getPort(),
                cp.getUsername(), new String(cp.getPassword()),
                cp.getEnvironment(), DEFAULT_POOL_SIZE);
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance defined in the client
     * {@code prefs} on behalf of the user defined in the client {@code prefs},
     * but will try to use previously created connections before establishing
     * new ones for any request.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(String host, int port,
            String username, String password) {
        return new CachedConnectionPool(host, port, username, password,
                DEFAULT_POOL_SIZE);
    }

    /**
     * Return a {@link ConnectionPool} that has no limit on the number of
     * connections it can manage to the Concourse instance defined in the client
     * {@code prefs} on behalf of the user defined in the client {@code prefs},
     * but will try to use previously created connections before establishing
     * new ones for any request.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @return the ConnectionPool
     */
    public static ConnectionPool newCachedConnectionPool(String host, int port,
            String username, String password, String environment) {
        return new CachedConnectionPool(host, port, username, password,
                environment, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides connections to the
     * Concourse instance defined in the client {@code prefs} on behalf of the
     * user defined in the client {@code prefs}.
     * 
     * @param prefs
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int)}.
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String prefs) {
        return newFixedConnectionPool(prefs, DEFAULT_POOL_SIZE);
    }

    /**
     * Return a new {@link ConnectionPool} that provides {@code poolSize}
     * connections to the Concourse instance defined in the client {@code prefs}
     * on behalf of the user defined in the client {@code prefs}.
     * 
     * @param prefs
     * @param poolSize
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int)}.
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String prefs, int poolSize) {
        return newFixedConnectionPool(prefs, poolSize);
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
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int, String, String, int)}
     *             .
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String host, int port,
            String username, String password) {
        return newFixedConnectionPool(host, port, username, password,
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
     * @return the ConnectionPool
     * @deprecated As of version 0.3.2, replaced by
     *             {@link #newFixedConnectionPool(String, int, String, String, int)}
     *             .
     */
    @Deprecated
    public static ConnectionPool newConnectionPool(String host, int port,
            String username, String password, int poolSize) {
        return newFixedConnectionPool(host, port, username, password, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of connections to
     * the Concourse instance defined in the {@code concourse_client.prefs} file
     * located in the working directory or using the default connection info if
     * no such file exists.
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(int poolSize) {
        return newFixedConnectionPool(DEFAULT_PREFS_FILE, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of
     * connections to the Concourse instance defined in the client {@code prefs}
     * on behalf of the user defined in the client {@code prefs}.
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param prefs
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(String prefs,
            int poolSize) {
        ConcourseClientPreferences cp = ConcourseClientPreferences.load(prefs);
        return new FixedConnectionPool(cp.getHost(), cp.getPort(),
                cp.getUsername(), new String(cp.getPassword()),
                cp.getEnvironment(), poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of connections to
     * the Concourse instance at {@code host}:{@code port} on behalf of the user
     * identified by {@code username} and {@code password}.
     * 
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(String host, int port,
            String username, String password, int poolSize) {
        return new FixedConnectionPool(host, port, username, password, poolSize);
    }

    /**
     * Return a new {@link ConnectionPool} with a fixed number of connections to
     * the Concourse instance at {@code host}:{@code port} on behalf of the user
     * identified by {@code username} and {@code password}.
     * 
     * <p>
     * If all the connections from the pool are active, subsequent request
     * attempts will block until a connection is returned.
     * </p>
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @param poolSize
     * @return the ConnectionPool
     */
    public static ConnectionPool newFixedConnectionPool(String host, int port,
            String username, String password, String environment, int poolSize) {
        return new FixedConnectionPool(host, port, username, password,
                environment, poolSize);
    }

    /**
     * The default connection pool size.
     */
    protected static final int DEFAULT_POOL_SIZE = 10;

    /**
     * The default preferences file to use if none is specified.
     */
    private static final String DEFAULT_PREFS_FILE = "concourse_client.prefs";

    /**
     * A FIFO queue of connections that are available to be leased.
     */
    protected final Queue<Concourse> available;

    /**
     * The connections that have been leased from this pool.
     */
    private final Set<Concourse> leased;

    /**
     * A flag to indicate if the pool is currently open and operational.
     */
    private AtomicBoolean open = new AtomicBoolean(true);

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param poolSize
     */
    protected ConnectionPool(String host, int port, String username,
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
    protected ConnectionPool(String host, int port, String username,
            String password, String environment, int poolSize) {
        this.available = buildQueue(poolSize);
        this.leased = Sets.newSetFromMap(Maps
                .<Concourse, Boolean> newConcurrentMap());
        for (int i = 0; i < poolSize; ++i) {
            available.offer(Concourse.connect(host, port, username, password,
                    environment));
        }
        // Ensure that the client connections are forced closed when the JVM is
        // shutdown in case the user does not properly close the pool
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                if(open.get()) {
                    exitAllConnections();
                }
            }

        }));
    }

    @Override
    public void close() throws Exception {
        Preconditions.checkState(isCloseable(),
                "Cannot shutdown the connection pool "
                        + "until all the connections have been returned");
        if(open.compareAndSet(true, false)) {
            exitAllConnections();
        }
        else {
            throw new IllegalStateException("Connection pool is closed");
        }
    }

    /**
     * Return {@code true} if the pool has any available connections.
     * 
     * @return {@code true} if there are one or more available connections
     */
    public boolean hasAvailableConnection() {
        verifyOpenState();
        return available.peek() != null;
    }

    /**
     * Return a previously requested connection back to the pool.
     * 
     * @param connection
     */
    public void release(Concourse connection) {
        verifyOpenState();
        verifyValidOrigin(connection);
        available.offer(connection);
        leased.remove(connection);
    }

    /**
     * Request a connection from the pool and block until one is available and
     * returned.
     * 
     * @return a connection
     */
    public Concourse request() {
        verifyOpenState();
        Concourse connection = getConnection();
        leased.add(connection);
        return connection;
    }

    /**
     * Return the {@link Queue} that will hold the connections.
     * 
     * @param size
     * 
     * @return the connections cache
     */
    protected abstract Queue<Concourse> buildQueue(int size);

    /**
     * Get a connection from the queue of {@code available} ones. The subclass
     * should use the correct method depending upon whether this method should
     * block or not.
     * 
     * @return the connection
     */
    protected abstract Concourse getConnection();

    /**
     * Exit all the connections managed by the pool.
     */
    private void exitAllConnections() {
        for (Concourse concourse : available) {
            concourse.exit();
        }
    }

    /**
     * Return {@code true} if none of the connections are currently active.
     * 
     * @return {@code true} if the pool can be closed
     */
    private boolean isCloseable() {
        return leased.isEmpty();
    }

    /**
     * Ensure that the connection pool is open. If it is not, throw an
     * IllegalStateException.
     */
    private void verifyOpenState() {
        Preconditions.checkState(open.get(), "Connection pool is closed");
    }

    /**
     * Verify that the {@code connection} was leased from this pool.
     * 
     * @param connection
     */
    private void verifyValidOrigin(Concourse connection) {
        if(!leased.contains(connection)) {
            throw new IllegalArgumentException(
                    "Cannot release the connection because it "
                            + "was not previously requested from this pool");
        }
    }

}

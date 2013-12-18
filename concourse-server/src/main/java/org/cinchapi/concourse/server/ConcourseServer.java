/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.cinchapi.concourse.security.AccessManager;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import org.cinchapi.concourse.server.jmx.ManagedOperation;
import org.cinchapi.concourse.server.storage.AtomicOperation;
import org.cinchapi.concourse.server.storage.AtomicStateException;
import org.cinchapi.concourse.server.storage.Compoundable;
import org.cinchapi.concourse.server.storage.Engine;
import org.cinchapi.concourse.server.storage.Transaction;
import org.cinchapi.concourse.shell.CommandLine;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.ConcourseService.Iface;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.cinchapi.concourse.server.GlobalState.*;

/**
 * Accepts requests from clients to read and write data in Concourse. The server
 * is configured with a {@code concourse.prefs} file.
 * 
 * @author jnelson
 */
public class ConcourseServer implements
        ConcourseService.Iface,
        ConcourseServerMXBean {

    /**
     * Run the server...
     * 
     * @param args
     * @throws TTransportException
     * @throws MalformedObjectNameException
     * @throws NotCompliantMBeanException
     * @throws MBeanRegistrationException
     * @throws InstanceAlreadyExistsException
     */
    public static void main(String... args) throws TTransportException,
            MalformedObjectNameException, InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException {
        final ConcourseServer server = new ConcourseServer();

        // Ensure the application is properly configured
        MemoryUsage heap = ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage();
        if(heap.getInit() < MIN_HEAP_SIZE) {
            System.err.println("Cannot initialize Concourse Server with "
                    + "a heap smaller than " + MIN_HEAP_SIZE + " bytes");
            System.exit(127);
        }

        // Register MXBean
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName(
                "org.cinchapi.concourse.server.jmx:type=ConcourseServerMXBean");
        mbs.registerMBean(server, name);

        // Start the server...
        Thread serverThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    CommandLine.displayWelcomeBanner();
                    server.start();
                }
                catch (TTransportException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }

        }, "main");
        serverThread.start();

        // Prepare for graceful shutdown...
        // NOTE: It may be necessary to run the Java VM with
        // -Djava.net.preferIPv4Stack=true
        final Thread shutdownThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerSocket socket = new ServerSocket(SHUTDOWN_PORT);
                    socket.accept(); // block until a shutdown request is made
                    Logger.info("Shutdown request received");
                    server.stop();
                    socket.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }, "Shutdown");
        shutdownThread.setDaemon(true);
        shutdownThread.start();

        // Add a shutdown hook that launches the official {@link ShutdownRunner}
        // in cases where the server process is directly killed (i.e. from the
        // tanuki scripts)
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                ShutdownRunner.main();
                try {
                    shutdownThread.join();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
    }

    protected static final int SERVER_PORT = 1717; // This may become
                                                   // configurable in a
                                                   // prefs file in a
                                                   // future release.

    private static final int NUM_WORKER_THREADS = 100; // This may become
                                                       // configurable in a
                                                       // prefs file in a
                                                       // future release.

    private static final int MIN_HEAP_SIZE = 268435456; // 256 MB

    /**
     * Contains the credentials used by the {@link #manager}. This file is
     * typically located in the root of the server installation.
     */
    private static final String ACCESS_FILE = ".access";

    /**
     * The Thrift server controls the RPC protocol. Use
     * https://github.com/m1ch1/mapkeeper/wiki/Thrift-Java-Servers-Compared for
     * a reference.
     */
    private final TServer server;

    /**
     * The Engine controls all the logic for data storage and retrieval.
     */
    private final Engine engine;

    /**
     * The AccessManager controls access to the server.
     */
    private final AccessManager manager;

    /**
     * The server maintains a collection of {@link Transaction} objects to
     * ensure that client requests are properly routed. When the client makes a
     * call to {@link #stage(AccessToken)}, a Transaction is started on the
     * server and a {@link TransactionToken} is used for the client to reference
     * that Transaction in future calls.
     */
    private final Map<TransactionToken, Transaction> transactions = Maps
            .newHashMap();

    /**
     * Construct a ConcourseServer that listens on {@link #SERVER_PORT} and
     * stores data in {@link Properties#DATA_HOME}.
     * 
     * @throws TTransportException
     */
    public ConcourseServer() throws TTransportException {
        this(SERVER_PORT, BUFFER_DIRECTORY, DATABASE_DIRECTORY);
    }

    /**
     * Construct a ConcourseServer that listens on {@code port} and store data
     * in {@code dbStore} and {@code bufferStore}.
     * 
     * @param port
     * @param bufferStore
     * @param dbStore
     * @throws TTransportException
     */
    public ConcourseServer(int port, String bufferStore, String dbStore)
            throws TTransportException {
        FileSystem.mkdirs(bufferStore);
        FileSystem.mkdirs(dbStore);
        TServerSocket socket = new TServerSocket(port);
        ConcourseService.Processor<Iface> processor = new ConcourseService.Processor<Iface>(
                this);
        Args args = new TThreadPoolServer.Args(socket);
        args.processor(processor);
        args.maxWorkerThreads(NUM_WORKER_THREADS);
        args.executorService(Executors
                .newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
                        "Server" + "-%d").build()));
        this.server = new TThreadPoolServer(args);
        this.engine = new Engine(bufferStore, dbStore);
        this.manager = AccessManager.create(ACCESS_FILE);
    }

    @Override
    public void abort(AccessToken creds, TransactionToken transaction)
            throws TException {
        authenticate(creds);
        Preconditions.checkArgument(transaction.getAccessToken().equals(creds)
                && transactions.containsKey(transaction));
        transactions.remove(transaction).abort();
    }

    @Override
    public boolean add(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction) throws TException {
        authenticate(creds);
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
        return transaction != null ? transactions.get(transaction).add(key,
                value, record) : engine.add(key, value, record);
    }

    @Override
    public Map<Long, String> audit(long record, String key, AccessToken creds,
            TransactionToken transaction) throws TException {
        authenticate(creds);
        if(transaction != null) {
            Preconditions.checkArgument(transaction.getAccessToken().equals(
                    creds)
                    && transactions.containsKey(transaction));
            Transaction t = transactions.get(transaction);
            return Strings.isNullOrEmpty(key) ? t.audit(record) : t.audit(key,
                    record);
        }
        return Strings.isNullOrEmpty(key) ? engine.audit(record) : engine
                .audit(key, record);
    }

    @Override
    public void clear(String key, long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doClear(key, record,
                    transaction != null ? transactions.get(transaction)
                            : engine);
        }
    }

    @Override
    public boolean commit(AccessToken creds, TransactionToken transaction)
            throws TException {
        authenticate(creds);
        Preconditions.checkArgument(transaction.getAccessToken().equals(creds)
                && transactions.containsKey(transaction));
        return transactions.remove(transaction).commit();
    }

    @Override
    public Set<String> describe(long record, long timestamp, AccessToken creds,
            TransactionToken transaction) throws TException {
        authenticate(creds);
        if(transaction != null) {
            Preconditions.checkArgument(transaction.getAccessToken().equals(
                    creds)
                    && transactions.containsKey(transaction));
            Transaction t = transactions.get(transaction);
            return timestamp == 0 ? t.describe(record) : t.describe(record,
                    timestamp);
        }
        return timestamp == 0 ? engine.describe(record) : engine.describe(
                record, timestamp);
    }

    @ManagedOperation
    @Override
    public String dump(String id) {
        return engine.dump(id);
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction) throws TException {
        authenticate(creds);
        if(transaction != null) {
            Preconditions.checkArgument(transaction.getAccessToken().equals(
                    creds)
                    && transactions.containsKey(transaction));
            Transaction t = transactions.get(transaction);
            return timestamp == 0 ? t.fetch(key, record) : t.fetch(key, record,
                    timestamp);
        }
        return timestamp == 0 ? engine.fetch(key, record) : engine.fetch(key,
                record, timestamp);
    }

    @Override
    public Set<Long> find(String key, Operator operator, List<TObject> values,
            long timestamp, AccessToken creds, TransactionToken transaction)
            throws TException {
        authenticate(creds);
        TObject[] tValues = values.toArray(new TObject[values.size()]);
        if(transaction != null) {
            Preconditions.checkArgument(transaction.getAccessToken().equals(
                    creds)
                    && transactions.containsKey(transaction));
            Transaction t = transactions.get(transaction);
            return timestamp == 0 ? t.find(key, operator, tValues) : t.find(
                    timestamp, key, operator, tValues);
        }
        return timestamp == 0 ? engine.find(key, operator, tValues) : engine
                .find(timestamp, key, operator, tValues);
    }

    @Override
    @ManagedOperation
    public boolean login(byte[] username, byte[] password) {
        // NOTE: Any existing sessions for the user will be invalidated.
        try {
            AccessToken token = login(ByteBuffer.wrap(username),
                    ByteBuffer.wrap(password));
            username = null;
            password = null;
            if(token != null) {
                logout(token);
                return true;
            }
            else {
                return false;
            }
        }
        catch (SecurityException e) {
            return false;
        }
        catch (TException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public AccessToken login(ByteBuffer username, ByteBuffer password)
            throws TException {
        validate(username, password);
        return manager.createAccessToken(username);
    }

    @Override
    public void logout(AccessToken creds) throws TException {
        authenticate(creds);
        manager.invalidateAccessToken(creds);
    }

    @Override
    public boolean ping(long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        authenticate(creds);
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
        return transaction != null ? !transactions.get(transaction)
                .describe(record).isEmpty() : !engine.describe(record)
                .isEmpty();
    }

    @Override
    public boolean remove(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction) throws TException {
        authenticate(creds);
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
        return transaction != null ? transactions.get(transaction).remove(key,
                value, record) : engine.remove(key, value, record);
    }

    @Override
    public void revert(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doRevert(key, record, timestamp,
                    transaction != null ? transactions.get(transaction)
                            : engine);
        }
    }

    @Override
    public Set<Long> search(String key, String query, AccessToken creds,
            TransactionToken transaction) throws TException {
        authenticate(creds);
        if(transaction != null) {
            Preconditions.checkArgument(transaction.getAccessToken().equals(
                    creds)
                    && transactions.containsKey(transaction));
            Transaction t = transactions.get(transaction);
            return t.search(key, query);
        }
        return engine.search(key, query);
    }

    @Override
    public void set0(String key, TObject value, long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doSet(key, value, record,
                    transaction != null ? transactions.get(transaction)
                            : engine);
        }
    }

    @Override
    public TransactionToken stage(AccessToken creds) throws TException {
        authenticate(creds);
        TransactionToken token = new TransactionToken(creds, Time.now());
        Transaction transaction = engine.startTransaction();
        transactions.put(token, transaction);
        Logger.info("Started Transaction {}", transaction.hashCode());
        return token;
    }

    /**
     * Start the server.
     * 
     * @throws TTransportException
     */
    public void start() throws TTransportException {
        engine.start();
        System.out.println("The Concourse server has started");
        server.serve();
    }

    /**
     * Stop the server.
     */
    public void stop() {
        if(server.isServing()) {
            server.stop();
            engine.stop();
            System.out.println("The Concourse server has stopped");
        }
    }

    @Override
    public boolean verify(String key, TObject value, long record,
            long timestamp, AccessToken creds, TransactionToken transaction)
            throws TException {
        authenticate(creds);
        if(transaction != null) {
            Preconditions.checkArgument(transaction.getAccessToken().equals(
                    creds)
                    && transactions.containsKey(transaction));
            Transaction t = transactions.get(transaction);
            return timestamp == 0 ? t.verify(key, value, record) : t.verify(
                    key, value, record, timestamp);
        }
        return timestamp == 0 ? engine.verify(key, value, record) : engine
                .verify(key, value, record, timestamp);
    }

    @Override
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds, TransactionToken transaction)
            throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = AtomicOperation
                .start(transaction != null ? transactions.get(transaction)
                        : engine);
        try {
            if(operation.verify(key, expected, record)) {
                operation.remove(key, expected, record);
                operation.add(key, replacement, record);
                return operation.commit();
            }
            return false;
        }
        catch (AtomicStateException e) {
            return false;
        }
    }

    /**
     * Verify that {@code token} is valid.
     * 
     * @param token
     * @throws SecurityException
     */
    private void authenticate(AccessToken token) throws SecurityException {
        if(!manager.approve(token)) {
            throw new SecurityException("Invalid access token");
        }
    }

    /**
     * Check to make sure that {@code creds} and {@code transaction} are valid
     * and are associated with one another.
     * 
     * @param creds
     * @param transaction
     * @throws SecurityException
     * @throws IllegalArgumentException
     */
    private void checkAccess(AccessToken creds, TransactionToken transaction)
            throws SecurityException, IllegalArgumentException {
        authenticate(creds);
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
    }

    /**
     * Start an {@link AtomicOperation} with {@code store} as the destination
     * and do the work to clear {@code key} in {@code record}.
     * 
     * @param key
     * @param record
     * @param store
     * @return the AtomicOperation
     */
    private AtomicOperation doClear(String key, long record, Compoundable store) {
        AtomicOperation operation = AtomicOperation.start(store);
        try {
            Set<TObject> values = operation.fetch(key, record);
            for (TObject value : values) {
                operation.remove(key, value, record);
            }
            return operation;
        }
        catch (AtomicStateException e) {
            return null;
        }
    }

    /**
     * Start an {@link AtomicOperation} with {@code store} as the destination
     * and do the work to revert {@code key} in {@code record} to
     * {@code timestamp}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param store
     * @return the AtomicOperation that must be committed
     */
    private AtomicOperation doRevert(String key, long record, long timestamp,
            Compoundable store) {
        AtomicOperation operation = AtomicOperation.start(store);
        try {
            Set<TObject> past = operation.fetch(key, record, timestamp);
            Set<TObject> present = operation.fetch(key, record);
            Set<TObject> xor = Sets.symmetricDifference(past, present);
            for (TObject value : xor) {
                if(present.contains(value)) {
                    operation.remove(key, value, record);
                }
                else {
                    operation.add(key, value, record);
                }
            }
            return operation;
        }
        catch (AtomicStateException e) {
            return null;
        }
    }

    /**
     * Start an {@link AtomicOperation} with {@code store} as the destination
     * and do the work to set {@code key} as {@code value} in {@code record}.
     * 
     * @param key
     * @param value
     * @param record
     * @param store
     * @return
     */
    private AtomicOperation doSet(String key, TObject value, long record,
            Compoundable store) {
        // NOTE: We cannot use the #clear() method because our removes must be
        // defined in terms of the AtomicOperation for true atomic safety.
        AtomicOperation operation = AtomicOperation.start(store);
        try {
            Set<TObject> values = operation.fetch(key, record);
            for (TObject oldValue : values) {
                operation.remove(key, oldValue, record);
            }
            operation.add(key, value, record);
            return operation;
        }
        catch (AtomicStateException e) {
            return null;
        }

    }

    /**
     * Validate that the {@code username} and {@code password} pair represent
     * correct credentials. If not, throw a SecurityException.
     * 
     * @param username
     * @param password
     * @throws SecurityException
     */
    private void validate(ByteBuffer username, ByteBuffer password)
            throws SecurityException {
        if(!manager.approve(username, password)) {
            throw new SecurityException(
                    "Invalid username/password combination.");
        }
    }
}

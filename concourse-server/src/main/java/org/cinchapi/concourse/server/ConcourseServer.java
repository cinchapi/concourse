/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
import org.cinchapi.concourse.config.Default;
import org.cinchapi.concourse.lang.ConjunctionSymbol;
import org.cinchapi.concourse.lang.Expression;
import org.cinchapi.concourse.lang.Parser;
import org.cinchapi.concourse.lang.PostfixNotationSymbol;
import org.cinchapi.concourse.lang.Symbol;
import org.cinchapi.concourse.lang.Translate;
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
import org.cinchapi.concourse.thrift.TCriteria;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.ConcourseService.Iface;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.concourse.thrift.TSymbol;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Logger;
import org.cinchapi.concourse.util.TLinkedHashMap;
import org.cinchapi.concourse.util.TSets;
import org.cinchapi.concourse.util.Version;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.thrift.Type;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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
     * A mapping from namespace to the corresponding Engine that controls all
     * the logic for data storage and retrieval.
     */
    private final Map<String, Engine> engines;

    /**
     * The base location where the indexed database records are stored.
     */
    private final String dbStore;

    /**
     * The base location where the indexed buffer pages are stored.
     */
    private final String bufferStore;

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
        this(CLIENT_PORT, BUFFER_DIRECTORY, DATABASE_DIRECTORY);
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
        Preconditions.checkState(!bufferStore.equalsIgnoreCase(dbStore),
                "Cannot store buffer and database files in the same directory. "
                        + "Please check concourse.prefs.");
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
        this.bufferStore = bufferStore;
        this.dbStore = dbStore;
        this.engines = Maps.newConcurrentMap();
        this.manager = AccessManager.create(ACCESS_FILE);
        engine(); // load the default engine
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
        if(value.getType() != Type.LINK
                || isValidLink((Link) Convert.thriftToJava(value), record)) {
            authenticate(creds);
            Preconditions
                    .checkArgument((transaction != null
                            && transaction.getAccessToken().equals(creds) && transactions
                                .containsKey(transaction))
                            || transaction == null);
            return transaction != null ? transactions.get(transaction).add(key,
                    value, record) : engine(Default.NAMESPACE).add(key, value,
                    record);
        }
        else {
            return false;
        }
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
        return Strings.isNullOrEmpty(key) ? engine(Default.NAMESPACE).audit(
                record) : engine(Default.NAMESPACE).audit(key, record);
    }

    @Override
    public Map<String, Set<TObject>> browse0(long record, long timestamp,
            AccessToken creds, TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        Compoundable store = transaction != null ? transactions
                .get(transaction) : engine(Default.NAMESPACE);
        return timestamp == 0 ? store.browse(record) : store.browse(record,
                timestamp);
    }

    @Override
    public Map<TObject, Set<Long>> browse1(String key, long timestamp,
            AccessToken creds, TransactionToken transaction)
            throws TSecurityException, TException {
        checkAccess(creds, transaction);
        Compoundable store = transaction != null ? transactions
                .get(transaction) : engine(Default.NAMESPACE);
        return timestamp == 0 ? store.browse(key) : store
                .browse(key, timestamp);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(long record, String key,
            AccessToken creds, TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        Compoundable store = transaction != null ? transactions
                .get(transaction) : engine(Default.NAMESPACE);
        Map<Long, Set<TObject>> result = TLinkedHashMap.newTLinkedHashMap();
        Map<Long, String> history = store.audit(key, record);
        for (Long timestamp : history.keySet()) {
            Set<TObject> values = store.fetch(key, record, timestamp);
            if(!values.isEmpty()) {
                result.put(timestamp, values);
            }
        }
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = updateChronologizeResultSet(key, record, result,
                    history, store);
        }
        return result;
    }

    @Override
    public void clear(String key, long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doClear(key, record,
                    transaction != null ? transactions.get(transaction)
                            : engine(Default.NAMESPACE));
        }
    }

    @Override
    public void clear1(long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doClear(record,
                    transaction != null ? transactions.get(transaction)
                            : engine(Default.NAMESPACE));
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
        return timestamp == 0 ? engine(Default.NAMESPACE).describe(record)
                : engine(Default.NAMESPACE).describe(record, timestamp);
    }

    @ManagedOperation
    @Override
    public String dump(String id) {
        return engine(Default.NAMESPACE).dump(id);
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
        return timestamp == 0 ? engine(Default.NAMESPACE).fetch(key, record)
                : engine(Default.NAMESPACE).fetch(key, record, timestamp);
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
        return timestamp == 0 ? engine(Default.NAMESPACE).find(key, operator,
                tValues) : engine(Default.NAMESPACE).find(timestamp, key,
                operator, tValues);
    }

    @Override
    public Set<Long> find1(TCriteria tcriteria, AccessToken creds,
            TransactionToken transaction) throws TSecurityException, TException {
        checkAccess(creds, transaction);
        List<Symbol> symbols = Lists.newArrayList();
        for (TSymbol tsymbol : tcriteria.getSymbols()) {
            symbols.add(Translate.fromThrift(tsymbol));
        }
        Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(symbols);
        Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doFind1(queue, stack,
                    transaction != null ? transactions.get(transaction)
                            : engine(Default.NAMESPACE));
        }
        return Sets.newTreeSet(stack.pop());
    }

    @Override
    public String getDumpList() {
        return engine(Default.NAMESPACE).getDumpList();
    }

    @Override
    @ManagedOperation
    public String getServerVersion() {
        return Version.getVersion(ConcourseServer.class).toString();
    }

    @Override
    @ManagedOperation
    public void grant(byte[] username, byte[] password) {
        manager.grant(ByteBuffer.wrap(username), ByteBuffer.wrap(password));
        username = null;
        password = null;
    }

    @Override
    @ManagedOperation
    public boolean hasUser(byte[] username) {
        return manager.isValidUsername(ByteBuffer.wrap(username));
    }

    @Override
    public boolean insert(String json, long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = AtomicOperation
                .start(transaction != null ? transactions.get(transaction)
                        : engine(Default.NAMESPACE));
        try {
            Multimap<String, Object> data = Convert.jsonToJava(json);
            for (String key : data.keySet()) {
                for (Object value : data.get(key)) {
                    if(!operation.add(key, Convert.javaToThrift(value), record)) {
                        return false;
                    }
                }
            }
            return operation.commit();
        }
        catch (AtomicStateException e) {
            return false;
        }

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
        catch (TSecurityException e) {
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
        return manager.authorize(username);
    }

    @Override
    public void logout(AccessToken creds) throws TException {
        authenticate(creds);
        manager.deauthorize(creds);
    }

    @Override
    public boolean ping(long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        authenticate(creds);
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
        return transaction != null ? !transactions.get(transaction)
                .describe(record).isEmpty() : !engine(Default.NAMESPACE)
                .describe(record).isEmpty();
    }

    @Override
    public boolean remove(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction) throws TException {
        if(value.getType() != Type.LINK
                || isValidLink((Link) Convert.thriftToJava(value), record)) {
            authenticate(creds);
            Preconditions
                    .checkArgument((transaction != null
                            && transaction.getAccessToken().equals(creds) && transactions
                                .containsKey(transaction))
                            || transaction == null);
            return transaction != null ? transactions.get(transaction).remove(
                    key, value, record) : engine(Default.NAMESPACE).remove(
                    key, value, record);
        }
        else {
            return false;
        }
    }

    @Override
    public void revert(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doRevert(key, record, timestamp,
                    transaction != null ? transactions.get(transaction)
                            : engine(Default.NAMESPACE));
        }
    }

    @Override
    @ManagedOperation
    public void revoke(byte[] username) {
        manager.revoke(ByteBuffer.wrap(username));
        username = null;
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
        return engine(Default.NAMESPACE).search(key, query);
    }

    @Override
    public void set0(String key, TObject value, long record, AccessToken creds,
            TransactionToken transaction) throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = null;
        while (operation == null || !operation.commit()) {
            operation = doSet(key, value, record,
                    transaction != null ? transactions.get(transaction)
                            : engine(Default.NAMESPACE));
        }
    }

    @Override
    public TransactionToken stage(AccessToken creds) throws TException {
        authenticate(creds);
        TransactionToken token = new TransactionToken(creds, Time.now());
        Transaction transaction = engine(Default.NAMESPACE).startTransaction();
        transactions.put(token, transaction);
        Logger.info("Started Transaction {}", transaction);
        return token;
    }

    /**
     * Start the server.
     * 
     * @throws TTransportException
     */
    public void start() throws TTransportException {
        for (Engine engine : engines.values()) {
            engine.start();
        }
        System.out.println("The Concourse server has started");
        server.serve();
    }

    /**
     * Stop the server.
     */
    public void stop() {
        if(server.isServing()) {
            server.stop();
            for (Engine engine : engines.values()) {
                engine.stop();
            }
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
        return timestamp == 0 ? engine(Default.NAMESPACE).verify(key, value,
                record) : engine(Default.NAMESPACE).verify(key, value, record,
                timestamp);
    }

    @Override
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds, TransactionToken transaction)
            throws TException {
        checkAccess(creds, transaction);
        AtomicOperation operation = AtomicOperation
                .start(transaction != null ? transactions.get(transaction)
                        : engine(Default.NAMESPACE));
        try {
            return (operation.verify(key, expected, record)
                    && operation.remove(key, expected, record) && operation
                        .add(key, replacement, record)) ? operation.commit()
                    : false;

        }
        catch (AtomicStateException e) {
            return false;
        }
    }

    /**
     * Verify that {@code token} is valid.
     * 
     * @param token
     * @throws TSecurityException
     */
    private void authenticate(AccessToken token) throws TSecurityException {
        if(!manager.validate(token)) {
            throw new TSecurityException("Invalid access token");
        }
    }

    /**
     * Check to make sure that {@code creds} and {@code transaction} are valid
     * and are associated with one another.
     * 
     * @param creds
     * @param transaction
     * @throws TSecurityException
     * @throws IllegalArgumentException
     */
    private void checkAccess(AccessToken creds, TransactionToken transaction)
            throws TSecurityException, IllegalArgumentException {
        authenticate(creds);
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
    }

    /**
     * Start an {@link AtomicOperation} with {@code store} as the destination
     * and do the work to clear {@code record}.
     * 
     * @param record
     * @param store
     * @return the AtomicOperation
     */
    private AtomicOperation doClear(long record, Compoundable store) {
        AtomicOperation operation = AtomicOperation.start(store);
        try {
            Map<String, Set<TObject>> values = operation.browse(record);
            for (Map.Entry<String, Set<TObject>> entry : values.entrySet()) {
                String key = entry.getKey();
                Set<TObject> valueSet = entry.getValue();
                for (TObject value : valueSet) {
                    operation.remove(key, value, record);
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
     * Do the work necessary to complete the
     * {@link #find1(TCriteria, long, AccessToken, TransactionToken)} method as
     * an AtomicOperation.
     * 
     * @param queue
     * @param stack
     * @param store
     * @return the AtomicOperation
     */
    private AtomicOperation doFind1(Queue<PostfixNotationSymbol> queue,
            Deque<Set<Long>> stack, Compoundable store) {
        // TODO there is room to do some query planning/optimization by going
        // through the pfn and plotting an Abstract Syntax Tree and looking for
        // the optimal routes to start with
        Preconditions.checkArgument(stack.isEmpty());
        AtomicOperation operation = store.startAtomicOperation();
        for (PostfixNotationSymbol symbol : queue) {
            if(symbol == ConjunctionSymbol.AND) {
                stack.push(TSets.intersection(stack.pop(), stack.pop()));
            }
            else if(symbol == ConjunctionSymbol.OR) {
                stack.push(TSets.union(stack.pop(), stack.pop()));
            }
            else if(symbol instanceof Expression) {
                Expression exp = (Expression) symbol;
                stack.push(exp.getTimestampRaw() == 0 ? operation.find(
                        exp.getKeyRaw(), exp.getOperatorRaw(),
                        exp.getValuesRaw()) : operation.find(
                        exp.getTimestampRaw(), exp.getKeyRaw(),
                        exp.getOperatorRaw(), exp.getValuesRaw()));
            }
            else {
                // If we reach here, then the conversion to postfix notation
                // failed :-/
                throw new IllegalStateException();
            }
        }
        return operation;
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
     * Return the {@link Engine} that is associated with the
     * {@link Engine#Defaults.NAMESPACE}.
     * 
     * @return the Engine
     */
    private Engine engine() {
        return engine(Default.NAMESPACE);
    }

    /**
     * Return the {@link Engine} that is associated with {@code namespace}. If
     * such an Engine does not exist, create a new one and add it to the
     * collection.
     * 
     * @param namespace
     * @return the Engine
     */
    private Engine engine(String namespace) {
        Engine engine = engines.get(namespace);
        if(engine == null) {
            engine = new Engine(bufferStore + File.separator + namespace,
                    dbStore + File.separator + namespace, namespace);
            engines.put(namespace, engine);
        }
        return engine;
    }

    /**
     * Return {@code true} if adding {@code link} to {@code record} is valid.
     * 
     * @param link
     * @param record
     * @return {@code true} if the link is valid
     */
    private boolean isValidLink(Link link, long record) {
        return link.longValue() != record;
    }

    /**
     * Start an {@link AtomicOperation} with {@code store} as the destination
     * and do the work to update chronologized values in {@code key} in
     * {@code record} with respect to {@code history} audit.
     * 
     * @param key
     * @param record
     * @param result
     * @param history
     * @param store
     * @return the AtomicOperation that must be committed
     */
    private AtomicOperation updateChronologizeResultSet(String key,
            long record, Map<Long, Set<TObject>> result,
            Map<Long, String> history, Compoundable store) {
        AtomicOperation operation = AtomicOperation.start(store);
        try {
            Map<Long, String> newResult = operation.audit(key, record);
            if(newResult.size() > history.size()) {
                for (int i = history.size(); i < newResult.size(); i++) {
                    Long timestamp = Iterables.get(
                            (Iterable<Long>) newResult.keySet(), i);
                    Set<TObject> values = operation.fetch(key, record);
                    if(!values.isEmpty()) {
                        result.put(timestamp, operation.fetch(key, record));
                    }
                }
            }
            return operation;
        }
        catch (AtomicStateException e) {
            return null;
        }
    }

    /**
     * Validate that the {@code username} and {@code password} pair represent
     * correct credentials. If not, throw a TSecurityException.
     * 
     * @param username
     * @param password
     * @throws TSecurityException
     */
    private void validate(ByteBuffer username, ByteBuffer password)
            throws TSecurityException {
        if(!manager.validate(username, password)) {
            throw new TSecurityException(
                    "Invalid username/password combination.");
        }
    }

}

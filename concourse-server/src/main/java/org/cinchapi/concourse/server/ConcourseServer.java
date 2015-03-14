/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;
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
import org.cinchapi.concourse.annotate.Alias;
import org.cinchapi.concourse.annotate.Atomic;
import org.cinchapi.concourse.annotate.AutoRetry;
import org.cinchapi.concourse.annotate.Batch;
import org.cinchapi.concourse.annotate.HistoricalRead;
import org.cinchapi.concourse.annotate.VersionControl;
import org.cinchapi.concourse.lang.ConjunctionSymbol;
import org.cinchapi.concourse.lang.Expression;
import org.cinchapi.concourse.lang.Parser;
import org.cinchapi.concourse.lang.PostfixNotationSymbol;
import org.cinchapi.concourse.lang.Symbol;
import org.cinchapi.concourse.lang.Language;
import org.cinchapi.concourse.security.AccessManager;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import org.cinchapi.concourse.server.jmx.ManagedOperation;
import org.cinchapi.concourse.server.storage.AtomicOperation;
import org.cinchapi.concourse.server.storage.AtomicStateException;
import org.cinchapi.concourse.server.storage.BufferedStore;
import org.cinchapi.concourse.server.storage.Compoundable;
import org.cinchapi.concourse.server.storage.Engine;
import org.cinchapi.concourse.server.storage.Store;
import org.cinchapi.concourse.server.storage.Transaction;
import org.cinchapi.concourse.server.storage.TransactionStateException;
import org.cinchapi.concourse.shell.CommandLine;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.TCriteria;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.ConcourseService.Iface;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.concourse.thrift.TSymbol;
import org.cinchapi.concourse.thrift.TTransactionException;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Convert.ResolvableLink;
import org.cinchapi.concourse.util.Environments;
import org.cinchapi.concourse.util.Logger;
import org.cinchapi.concourse.util.TCollections;
import org.cinchapi.concourse.util.TSets;
import org.cinchapi.concourse.util.Timestamps;
import org.cinchapi.concourse.util.Version;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.thrift.Type;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

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
 * @author Jeff Nelson
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

    /**
     * Add {@code key} as {@code value} in {@code record} using the atomic
     * {@code operation} if the record is empty. Otherwise, throw an
     * {@link AtomicStateException}.
     * <p>
     * If another operation adds data to the record after the initial check,
     * then an {@link AtomicStateException} will be thrown when an attempt is
     * made to commit {@code operation}.
     * </p>
     * 
     * @param key
     * @param value
     * @param record
     * @param operation
     * @throws AtomicStateException
     */
    private static void addIfEmpty(String key, TObject value, long record,
            AtomicOperation operation) throws AtomicStateException {
        if(operation.describe(record).isEmpty()) {
            operation.add(key, value, record);
        }
        else {
            throw new AtomicStateException();
        }
    }

    /**
     * Do the work to chronologize (generate a chronology of values) for
     * {@code key} in {@code record}. If {@code history} and {@code result} are
     * not {@code null}, then this method will only update the chronology with
     * the latest changes since the history/result were calculated.
     * 
     * @param key
     * @param record
     * @param result
     * @param history
     * @param atomic
     */
    private static void chronologize0(String key, long record,
            Map<Long, Set<TObject>> result, Map<Long, String> history,
            AtomicOperation atomic) {
        Map<Long, String> latest = atomic.audit(key, record);
        if(latest.size() > history.size()) {
            for (int i = history.size(); i < latest.size(); ++i) {
                long timestamp = Iterables.get(
                        (Iterable<Long>) latest.keySet(), i);
                Set<TObject> values = atomic.fetch(key, record, timestamp);
                if(!values.isEmpty()) {
                    result.put(timestamp, values);
                }
            }
        }
    }

    /**
     * Do the work to remove all the data from {@code record} using the
     * specified {@code atomic} operation.
     * 
     * @param record
     * @param atomic
     */
    private static void clear0(long record, AtomicOperation atomic) {
        Map<String, Set<TObject>> values = atomic.browse(record);
        for (Map.Entry<String, Set<TObject>> entry : values.entrySet()) {
            String key = entry.getKey();
            Set<TObject> valueSet = entry.getValue();
            for (TObject value : valueSet) {
                atomic.remove(key, value, record);
            }
        }
    }

    /**
     * Remove all the values mapped from the {@code key} in {@code record} using
     * the specified {@code atomic} operation.
     * 
     * @param key
     * @param record
     * @param atomic
     */
    private static void clear0(String key, long record, AtomicOperation atomic) {
        Set<TObject> values = atomic.fetch(key, record);
        for (TObject value : values) {
            atomic.remove(key, value, record);
        }
    }

    /**
     * Do the work necessary to complete a complex find operation based on the
     * {@code queue} of symbols.
     * 
     * @param queue
     * @param stack
     * @param atomic
     */
    private static void find0(Queue<PostfixNotationSymbol> queue,
            Deque<Set<Long>> stack, AtomicOperation atomic) {
        // NOTE: there is room to do some query planning/optimization by going
        // through the pfn and plotting an Abstract Syntax Tree and looking for
        // the optimal routes to start with
        Preconditions.checkArgument(stack.isEmpty());
        for (PostfixNotationSymbol symbol : queue) {
            if(symbol == ConjunctionSymbol.AND) {
                stack.push(TSets.intersection(stack.pop(), stack.pop()));
            }
            else if(symbol == ConjunctionSymbol.OR) {
                stack.push(TSets.union(stack.pop(), stack.pop()));
            }
            else if(symbol instanceof Expression) {
                Expression exp = (Expression) symbol;
                stack.push(exp.getTimestampRaw() == 0 ? atomic.find(
                        exp.getKeyRaw(), exp.getOperatorRaw(),
                        exp.getValuesRaw()) : atomic.find(
                        exp.getTimestampRaw(), exp.getKeyRaw(),
                        exp.getOperatorRaw(), exp.getValuesRaw()));
            }
            else {
                // If we reach here, then the conversion to postfix notation
                // failed :-/
                throw new IllegalStateException();
            }
        }
    }

    /**
     * Return {@code true} if adding {@code link} to {@code record} is valid.
     * This method is used to enforce referential integrity (i.e. record cannot
     * link to itself) before the data makes it way to the Engine.
     * 
     * @param link
     * @param record
     * @return {@code true} if the link is valid
     */
    private static boolean isValidLink(Link link, long record) {
        return link.longValue() != record;
    }

    /**
     * Perform a ping of the {@code record} (e.g check to see if the record
     * currently has any data) from the perspective of the specified
     * {@code store}.
     * 
     * @param record
     * @param store
     * @return {@code true} if the record currently has any data
     */
    private static boolean ping0(long record, Store store) {
        return !store.describe(record).isEmpty();
    }

    /**
     * Revert {@code key} in {@code record} to its state {@code timestamp} using
     * the provided atomic {@code operation}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param operation
     * @throws AtomicStateException
     */
    private static void revert0(String key, long record, long timestamp,
            AtomicOperation operation) throws AtomicStateException {
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
    }

    /**
     * Contains the credentials used by the {@link #manager}. This file is
     * typically located in the root of the server installation.
     */
    private static final String ACCESS_FILE = ".access";

    private static final int MIN_HEAP_SIZE = 268435456; // 256 MB

    private static final int NUM_WORKER_THREADS = 100; // This may become
                                                       // configurable in a
                                                       // prefs file in a
                                                       // future release.

    /**
     * The base location where the indexed buffer pages are stored.
     */
    private final String bufferStore;

    /**
     * The base location where the indexed database records are stored.
     */
    private final String dbStore;

    /**
     * A mapping from env to the corresponding Engine that controls all the
     * logic for data storage and retrieval.
     */
    private final Map<String, Engine> engines;

    /**
     * The AccessManager controls access to the server.
     */
    private final AccessManager manager;

    /**
     * The Thrift server controls the RPC protocol. Use
     * https://github.com/m1ch1/mapkeeper/wiki/Thrift-Java-Servers-Compared for
     * a reference.
     */
    private final TServer server;

    /**
     * The server maintains a collection of {@link Transaction} objects to
     * ensure that client requests are properly routed. When the client makes a
     * call to {@link #stage(AccessToken)}, a Transaction is started on the
     * server and a {@link TransactionToken} is used for the client to reference
     * that Transaction in future calls.
     */
    private final Map<TransactionToken, Transaction> transactions = new NonBlockingHashMap<TransactionToken, Transaction>();

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
        Preconditions
                .checkState(!Strings.isNullOrEmpty(Environments
                        .sanitize(DEFAULT_ENVIRONMENT)), "Cannot initialize "
                        + "Concourse Server with a default environment of "
                        + "'%s'. Please use a default environment name that "
                        + "contains only alphanumeric characters.",
                        DEFAULT_ENVIRONMENT);
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
                        "Client Worker" + " %d").build()));
        this.server = new TThreadPoolServer(args);
        this.bufferStore = bufferStore;
        this.dbStore = dbStore;
        this.engines = Maps.newConcurrentMap();
        this.manager = AccessManager.create(ACCESS_FILE);
        getEngine(); // load the default engine
    }

    @Override
    public void abort(AccessToken creds, TransactionToken transaction,
            String env) throws TException {
        checkAccess(creds, transaction);
        transactions.remove(transaction).abort();
    }

    @Override
    @Atomic
    @AutoRetry
    public long addKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        long record = 0;
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    record = Time.now();
                    addIfEmpty(key, value, record, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return record;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public boolean addKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            if(value.getType() != Type.LINK
                    || isValidLink((Link) Convert.thriftToJava(value), record)) {
                return ((BufferedStore) getStore(transaction, environment))
                        .add(key, value, record);
            }
            else {
                return false;
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Boolean> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, atomic.add(key, value, record));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @VersionControl
    public Map<Long, String> auditKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            return getStore(transaction, environment).audit(key, record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Alias
    @VersionControl
    public Map<Long, String> auditKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        return auditKeyRecordStartEnd(key, record, start, Time.now(), creds,
                transaction, environment);
    }

    @Override
    @VersionControl
    public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, String> result = Maps.newLinkedHashMap();
            Map<Long, String> base = store.audit(key, record);
            int index = Timestamps.findNearestSuccessorForTimestamp(
                    base.keySet(), start);
            Entry<Long, String> entry = null;
            if(index > 0) {
                entry = Iterables.get(base.entrySet(), index - 1);
                result.put(entry.getKey(), entry.getValue());
            }
            for (int i = index; i < base.size(); ++i) {
                entry = Iterables.get(base.entrySet(), i);
                if(entry.getKey() >= end) {
                    break;
                }
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @VersionControl
    public Map<Long, String> auditRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        try {
            return getStore(transaction, environment).audit(record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Alias
    @VersionControl
    public Map<Long, String> auditRecordStart(long record, long start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStartEnd(record, start, Time.now(), creds,
                transaction, environment);
    }

    @Override
    @VersionControl
    public Map<Long, String> auditRecordStartEnd(long record, long start,
            long end, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, String> result = Maps.newLinkedHashMap();
            Map<Long, String> base = store.audit(record);
            int index = Timestamps.findNearestSuccessorForTimestamp(
                    base.keySet(), start);
            Entry<Long, String> entry = null;
            if(index > 0) {
                entry = Iterables.get(base.entrySet(), index - 1);
                result.put(entry.getKey(), entry.getValue());
            }
            for (int i = index; i < base.size(); ++i) {
                entry = Iterables.get(base.entrySet(), i);
                if(entry.getKey() >= end) {
                    break;
                }
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public Set<Long> browse(AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            return getEngine(environment).browse();
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browseKey(String key, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).browse(key);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<String, Map<TObject, Set<Long>>> browseKeys(List<String> keys,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<String, Map<TObject, Set<Long>>> result = Maps
                    .newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (String key : keys) {
                        result.put(key, atomic.browse(key));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<String, Map<TObject, Set<Long>>> browseKeysTime(
            List<String> keys, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<String, Map<TObject, Set<Long>>> result = Maps
                    .newLinkedHashMap();
            for (String key : keys) {
                result.put(key, store.browse(key));
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public Map<TObject, Set<Long>> browseKeyTime(String key, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).browse(key, timestamp);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public Map<String, Set<TObject>> browseRecord(long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).browse(record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Map<String, Set<TObject>>> browseRecords(
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = Maps
                    .newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, atomic.browse(record));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<Long, Map<String, Set<TObject>>> browseRecordsTime(
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = Maps
                    .newLinkedHashMap();
            for (long record : records) {
                result.put(record, store.browse(record));
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public Map<String, Set<TObject>> browseRecordTime(long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).browse(record, timestamp);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @AutoRetry
    public Map<Long, Set<TObject>> chronologizeKeyRecord(String key,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
            Map<Long, String> history = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    chronologize0(key, record, result, history, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Alias
    @AutoRetry
    public Map<Long, Set<TObject>> chronologizeKeyRecordStart(String key,
            long record, long start, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return chronologizeKeyRecordStartEnd(key, record, start, Time.now(),
                creds, transaction, environment);
    }

    @Override
    @AutoRetry
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartEnd(String key,
            long record, long start, long end, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        // TODO review this implementation
        try {
            Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
            Map<Long, Set<TObject>> base = chronologizeKeyRecord(key, record,
                    creds, transaction, environment);
            int index = Timestamps.findNearestSuccessorForTimestamp(
                    base.keySet(), start);
            Entry<Long, Set<TObject>> entry = null;
            if(index > 0) {
                entry = Iterables.get(base.entrySet(), index - 1);
                result.put(entry.getKey(), entry.getValue());
            }
            for (int i = index; i < base.size(); ++i) {
                entry = Iterables.get(base.entrySet(), i);
                if(entry.getKey() >= end) {
                    break;
                }
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @AutoRetry
    public void clearKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    clear0(key, record, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    public void clearKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        clear0(key, record, atomic);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    public void clearKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (String key : keys) {
                        clear0(key, record, atomic);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    public void clearKeysRecords(List<String> keys, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        for (String key : keys) {
                            clear0(key, record, atomic);
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @AutoRetry
    public void clearRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    clear0(record, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    public void clearRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        clear0(record, atomic);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public boolean commit(AccessToken creds, TransactionToken transaction,
            String env) throws TException {
        checkAccess(creds, transaction);
        try {
            return transactions.remove(transaction).commit();
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public Set<String> describeRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).describe(record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Set<String>> describeRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Set<String>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, atomic.describe(record));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Set<String>> result = Maps.newLinkedHashMap();
            for (long record : records) {
                result.put(record, store.describe(record, timestamp));
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public Set<String> describeRecordTime(long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        try {
            return getStore(transaction, environment).describe(record,
                    timestamp);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @ManagedOperation
    @Override
    @Deprecated
    public String dump(String id) {
        return dump(DEFAULT_ENVIRONMENT);
    }

    @Override
    public String dump(String id, String env) {
        return getEngine(env).dump(id);
    }

    @Override
    public Set<TObject> fetchKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).fetch(key, record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Set<TObject>> fetchKeyRecords(String key,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, atomic.fetch(key, record));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    @HistoricalRead
    public Map<Long, Set<TObject>> fetchKeyRecordsTime(String key,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
            for (long record : records) {
                result.put(record, store.fetch(key, record, timestamp));
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public Set<TObject> fetchKeyRecordTime(String key, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).fetch(key, record,
                    timestamp);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<String, Set<TObject>> fetchKeysRecord(List<String> keys,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<String, Set<TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (String key : keys) {
                        result.put(key, atomic.fetch(key, record));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Map<String, Set<TObject>>> fetchKeysRecords(
            List<String> keys, List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = Maps
                    .newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        Map<String, Set<TObject>> entry = Maps.newHashMap();
                        for (String key : keys) {
                            entry.put(key, atomic.fetch(key, record));
                        }
                        if(!entry.isEmpty()) {
                            result.put(record, entry);
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<Long, Map<String, Set<TObject>>> fetchKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = Maps
                    .newLinkedHashMap();
            for (long record : records) {
                Map<String, Set<TObject>> entry = Maps.newHashMap();
                for (String key : keys) {
                    entry.put(key, store.fetch(key, record, timestamp));
                }
                if(!entry.isEmpty()) {
                    result.put(record, entry);
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<String, Set<TObject>> fetchKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<String, Set<TObject>> result = Maps.newLinkedHashMap();
            for (String key : keys) {
                result.put(key, store.fetch(key, record, timestamp));
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Set<Long> findCriteria(TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            List<Symbol> symbols = Lists.newArrayList();
            for (TSymbol tsymbol : criteria.getSymbols()) {
                symbols.add(Language.translateFromThrift(tsymbol));
            }
            Queue<PostfixNotationSymbol> queue = Parser
                    .toPostfixNotation(symbols);
            Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    find0(queue, stack, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return Sets.newTreeSet(stack.pop());
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            TObject[] tValues = values.toArray(new TObject[values.size()]);
            return getStore(transaction, environment).find(key, operator,
                    tValues);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            TObject[] tValues = values.toArray(new TObject[values.size()]);
            return getStore(transaction, environment).find(timestamp, key,
                    operator, tValues);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Alias
    public Set<Long> findKeyStringOperatorValues(String key, String operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return findKeyOperatorValues(key, Convert.stringToOperator(operator),
                values, creds, transaction, environment);
    }

    @Override
    @Alias
    public Set<Long> findKeyStringOperatorValuesTime(String key,
            String operator, List<TObject> values, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTime(key,
                Convert.stringToOperator(operator), values, timestamp, creds,
                transaction, environment);
    }

    @Override
    @ManagedOperation
    @Deprecated
    public String getDumpList() {
        return getDumpList(DEFAULT_ENVIRONMENT);
    }

    @Override
    public String getDumpList(String env) {
        return getEngine(env).getDumpList();
    }

    @Override
    public TObject getKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            return Iterables.getFirst(
                    getStore(transaction, environment).fetch(key, record),
                    TObject.NULL);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, TObject> getKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, TObject> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        try {
                            result.put(record, atomic.fetch(key, record)
                                    .iterator().next());
                        }
                        catch (NoSuchElementException e) {
                            continue;
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<Long, TObject> getKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Map<Long, TObject> result = Maps.newLinkedHashMap();
            Compoundable store = getStore(transaction, environment);
            for (long record : records) {
                try {
                    result.put(record, store.fetch(key, record, timestamp)
                            .iterator().next());
                }
                catch (NoSuchElementException e) {
                    continue;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public TObject getKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            return Iterables.getFirst(
                    getStore(transaction, environment).fetch(key, record,
                            timestamp), TObject.NULL);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<String, TObject> getKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<String, TObject> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (String key : keys) {
                        try {
                            result.put(key, atomic.fetch(key, record)
                                    .iterator().next());
                        }
                        catch (NoSuchElementException e) {
                            continue;
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Map<String, TObject>> getKeysRecords(List<String> keys,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        Map<String, TObject> entry = Maps.newHashMap();
                        for (String key : keys) {
                            try {
                                entry.put(key, atomic.fetch(key, record)
                                        .iterator().next());
                            }
                            catch (NoSuchElementException e) {
                                continue;
                            }
                        }
                        if(!entry.isEmpty()) {
                            result.put(record, entry);
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<Long, Map<String, TObject>> getKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
            Compoundable store = getStore(transaction, environment);
            for (long record : records) {
                Map<String, TObject> entry = Maps.newLinkedHashMap();
                for (String key : keys) {
                    try {
                        entry.put(key, store.fetch(key, record, timestamp)
                                .iterator().next());
                    }
                    catch (NoSuchElementException e) {
                        continue;
                    }
                }
                if(!entry.isEmpty()) {
                    result.put(record, entry);
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Batch
    @HistoricalRead
    public Map<String, TObject> getKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Map<String, TObject> result = Maps.newLinkedHashMap();
            Compoundable store = getStore(transaction, environment);
            for (String key : keys) {
                try {
                    result.put(key, store.fetch(key, record, timestamp)
                            .iterator().next());
                }
                catch (NoSuchElementException e) {
                    continue;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public String getServerEnvironment(AccessToken creds,
            TransactionToken transaction, String env)
            throws TSecurityException, TException {
        checkAccess(creds, transaction);
        return Environments.sanitize(env);
    }

    @Override
    @ManagedOperation
    public String getServerVersion() {
        return Version.getVersion(ConcourseServer.class).toString();
    }

    @Override
    @ManagedOperation
    public void grant(byte[] username, byte[] password) {
        manager.createUser(ByteBuffer.wrap(username), ByteBuffer.wrap(password));
        username = null;
        password = null;
    }

    @Override
    @ManagedOperation
    public boolean hasUser(byte[] username) {
        return manager.isExistingUsername(ByteBuffer.wrap(username));
    }

    @Override
    @Atomic
    @Batch
    public Set<Long> insertJson(String json, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            List<Multimap<String, Object>> objects = Convert
                    .anyJsonToJava(json);
            Compoundable store = getStore(transaction, environment);
            Set<Long> records = Sets.newLinkedHashSet();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (Multimap<String, Object> object : objects) {
                        long record = Time.now();
                        if(atomic.describe(record).isEmpty()
                                && insert0(object, record, atomic)) {
                            records.add(record);
                        }
                        else {
                            throw new AtomicStateException();
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                    records.clear();
                }
            }
            return records;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    public boolean insertJsonRecord(String json, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Compoundable store = getStore(transaction, environment);
        try {
            Multimap<String, Object> data = Convert.jsonToJava(json);
            AtomicOperation atomic = store.startAtomicOperation();
            return insert0(data, record, atomic) && atomic.commit();
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
        catch (AtomicStateException e) {
            return false;
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Boolean> insertJsonRecords(String json,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        Compoundable store = getStore(transaction, environment);
        try {
            Multimap<String, Object> data = Convert.jsonToJava(json);
            Map<Long, Boolean> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, insert0(data, record, atomic));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public String listAllEnvironments() {
        return TCollections.toOrderedListString(TSets.intersection(
                FileSystem.getSubDirs(BUFFER_DIRECTORY),
                FileSystem.getSubDirs(DATABASE_DIRECTORY)));
    }

    @Override
    public String listAllUserSessions() {
        return TCollections.toOrderedListString(manager
                .describeAllAccessTokens());
    }

    @Override
    @ManagedOperation
    public boolean login(byte[] username, byte[] password) {
        try {
            AccessToken token = login(ByteBuffer.wrap(username),
                    ByteBuffer.wrap(password));
            username = null;
            password = null;
            if(token != null) {
                logout(token, null); // NOTE: managed operations don't actually
                                     // need an access token, so we expire it
                                     // immediately
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
    public AccessToken login(ByteBuffer username, ByteBuffer password,
            String env) throws TException {
        validate(username, password);
        getEngine(env);
        return manager.getNewAccessToken(username);
    }

    @Override
    public void logout(AccessToken creds, String env) throws TException {
        checkAccess(creds, null);
        manager.expireAccessToken(creds);
    }

    @Override
    public boolean pingRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            return ping0(record, getStore(transaction, environment));
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Boolean> pingRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Boolean> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, ping0(record, atomic));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public boolean removeKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            if(value.getType() != Type.LINK
                    || isValidLink((Link) Convert.thriftToJava(value), record)) {
                return ((BufferedStore) getStore(transaction, environment))
                        .remove(key, value, record);
            }
            else {
                return false;
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            Map<Long, Boolean> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        result.put(record, atomic.remove(key, value, record));
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return result;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    @VersionControl
    @AutoRetry
    public void revertKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        revert0(key, record, timestamp, atomic);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }

    }

    @Override
    @Atomic
    @VersionControl
    @AutoRetry
    public void revertKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    revert0(key, record, timestamp, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    @VersionControl
    @AutoRetry
    public void revertKeysRecordsTime(List<String> keys, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        for (String key : keys) {
                            revert0(key, record, timestamp, atomic);
                        }
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Atomic
    @Batch
    @VersionControl
    @AutoRetry
    public void revertKeysRecordTime(List<String> keys, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (String key : keys) {
                        revert0(key, record, timestamp, atomic);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @ManagedOperation
    public void revoke(byte[] username) {
        manager.deleteUser(ByteBuffer.wrap(username));
        username = null;
    }

    @Override
    public Set<Long> search(String key, String query, AccessToken creds,
            TransactionToken transaction, String env) throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, env).search(key, query);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @Alias
    public long setKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return addKeyValue(key, value, creds, transaction, environment);
    }

    @Override
    public void setKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            ((BufferedStore) getStore(transaction, environment)).set(key,
                    value, record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }

    }

    @Override
    @Atomic
    @Batch
    public void setKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    for (long record : records) {
                        atomic.set(key, value, record);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    public TransactionToken stage(AccessToken creds, String env)
            throws TException {
        checkAccess(creds, null);
        TransactionToken token = new TransactionToken(creds, Time.now());
        Transaction transaction = getEngine(env).startTransaction();
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

    @Atomic
    @Override
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            AtomicOperation atomic = getStore(transaction, environment)
                    .startAtomicOperation();
            return (atomic.verify(key, expected, record)
                    && atomic.remove(key, expected, record) && atomic.add(key,
                    replacement, record)) ? atomic.commit() : false;
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
        catch (AtomicStateException e) {
            return false;
        }
    }

    @Override
    public boolean verifyKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment)
                    .verify(key, value, record);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Override
    @HistoricalRead
    public boolean verifyKeyValueRecordTime(String key, TObject value,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            return getStore(transaction, environment).verify(key, value,
                    record, timestamp);
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
        }
    }

    @Atomic
    @Override
    @AutoRetry
    public void verifyOrSet(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String env)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Compoundable store = getStore(transaction, env);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Set<TObject> values = atomic.fetch(key, record);
                    for (TObject val : values) {
                        if(!val.equals(value)) {
                            atomic.remove(key, val, record);
                        }
                    }
                    if(!atomic.verify(key, value, record)) {
                        atomic.add(key, value, record);
                    }
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
        }
        catch (TransactionStateException e) {
            throw new TTransactionException();
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
    private void checkAccess(AccessToken creds,
            @Nullable TransactionToken transaction) throws TSecurityException,
            IllegalArgumentException {
        if(!manager.isValidAccessToken(creds)) {
            throw new TSecurityException("Invalid access token");
        }
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds) && transactions
                    .containsKey(transaction)) || transaction == null);
    }

    /**
     * Return the {@link Engine} that is associated with the
     * {@link Default#ENVIRONMENT}.
     * 
     * @return the Engine
     */
    private Engine getEngine() {
        return getEngine(DEFAULT_ENVIRONMENT);
    }

    /**
     * Return the {@link Engine} that is associated with {@code env}. If such an
     * Engine does not exist, create a new one and add it to the collection.
     * 
     * @param env
     * @return the Engine
     */
    private Engine getEngine(String env) {
        Engine engine = engines.get(env);
        if(engine == null) {
            env = Environments.sanitize(env);
            return getEngineUnsafe(env);
        }
        return engine;
    }

    /**
     * Return the {@link Engine} that is associated with {@code env} without
     * performing any sanitzation on the name. If such an Engine does not exist,
     * create a new one and add it to the collection.
     */
    private Engine getEngineUnsafe(String env) {
        Engine engine = engines.get(env);
        if(engine == null) {
            engine = new Engine(bufferStore + File.separator + env, dbStore
                    + File.separator + env, env);
            engine.start();
            engines.put(env, engine);
        }
        return engine;
    }

    /**
     * Return the correct store to use for a read/write operation depending upon
     * the {@code env} whether the client has submitted a {@code transaction}
     * token.
     * 
     * @param transaction
     * @param env
     * @return the store to use
     */
    private Compoundable getStore(TransactionToken transaction, String env) {
        return transaction != null ? transactions.get(transaction)
                : getEngine(env);
    }

    /**
     * Do the work to atomically insert all of the {@code data} into
     * {@code record} and return {@code true} if the operation is successful.
     * 
     * @param data
     * @param record
     * @param atomic
     * @return {@code true} if all the data is atomically inserted
     */
    private boolean insert0(Multimap<String, Object> data, long record,
            AtomicOperation atomic) {
        for (String key : data.keySet()) {
            for (Object value : data.get(key)) {
                if(value instanceof ResolvableLink) {
                    ResolvableLink rl = (ResolvableLink) value;
                    Set<Long> links = atomic.find(rl.getKey(), Operator.EQUALS,
                            Convert.javaToThrift(rl.getValue()));
                    for (long link : links) {
                        TObject t = Convert.javaToThrift(Link.to(link));
                        if(!atomic.add(key, t, record)) {
                            return false;
                        }
                    }
                }
                else if(!atomic.add(key, Convert.javaToThrift(value), record)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * A version of the login routine that handles the case when no environment
     * has been specified. The is most common when authenticating a user for
     * managed operations.
     * 
     * @param username
     * @param password
     * @return the access token
     * @throws TException
     */
    private AccessToken login(ByteBuffer username, ByteBuffer password)
            throws TException {
        return login(username, password, DEFAULT_ENVIRONMENT);
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
        if(!manager.isExistingUsernamePasswordCombo(username, password)) {
            throw new TSecurityException(
                    "Invalid username/password combination.");
        }
    }

}
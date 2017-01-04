/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
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
import javax.annotation.concurrent.Immutable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.annotate.Alias;
import com.cinchapi.concourse.annotate.Atomic;
import com.cinchapi.concourse.annotate.AutoRetry;
import com.cinchapi.concourse.annotate.Batch;
import com.cinchapi.concourse.annotate.HistoricalRead;
import com.cinchapi.concourse.annotate.VersionControl;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.lang.NaturalLanguage;
import com.cinchapi.concourse.lang.Parser;
import com.cinchapi.concourse.lang.PostfixNotationSymbol;
import com.cinchapi.concourse.security.AccessManager;
import com.cinchapi.concourse.server.http.HttpServer;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.management.ConcourseManagementService;
import com.cinchapi.concourse.server.plugin.PluginException;
import com.cinchapi.concourse.server.plugin.PluginManager;
import com.cinchapi.concourse.server.plugin.data.TObjectResultDataset;
import com.cinchapi.concourse.server.plugin.PluginRestricted;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.BufferedStore;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Transaction;
import com.cinchapi.concourse.server.storage.TransactionStateException;
import com.cinchapi.concourse.server.upgrade.UpgradeTasks;
import com.cinchapi.concourse.shell.CommandLine;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.ConcourseService;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.DuplicateEntryException;
import com.cinchapi.concourse.thrift.InvalidArgumentException;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.ParseException;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionException;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.thrift.ConcourseService.Iface;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Environments;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.TMaps;
import com.cinchapi.concourse.util.Timestamps;
import com.cinchapi.concourse.util.Version;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonParseException;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;

import static com.cinchapi.concourse.server.GlobalState.*;

/**
 * Accepts requests from clients to read and write data in Concourse. The server
 * is configured with a {@code concourse.prefs} file.
 *
 * @author Jeff Nelson
 */
public class ConcourseServer extends BaseConcourseServer
        implements ConcourseService.Iface {

    /**
     * Create a new {@link ConcourseServer} instance that uses the default port
     * and storage locations or those defined in the accessible
     * {@code concourse.prefs} file.
     *
     * Creates a new {@link ConcourseServer} for management running
     * on {@link JMX_PORT} using {@code Thrift}
     *
     * @return {@link ConcourseServer}
     * @throws TTransportException
     */
    public static ConcourseServer create() throws TTransportException {
        return create(CLIENT_PORT, BUFFER_DIRECTORY, DATABASE_DIRECTORY);
    }

    /**
     * Create a new {@link ConcourseServer} instance that uses the specified
     * port and storage locations.
     * <p>
     * In general, this factory should on be used by unit tests. Runtime
     * construction of the server should be done using the
     * {@link ConcourseServer#create()} method so that the preferences file is
     * used.
     * </p>
     *
     * @param port - the port on which to listen for client connections
     * @param bufferStore - the location to store {@link Buffer} files
     * @param dbStore - the location to store {@link Database} files
     * @return {@link ConcourseServer}
     * @throws TTransportException
     */
    public static ConcourseServer create(int port, String bufferStore,
            String dbStore) throws TTransportException {
        Injector injector = Guice.createInjector(new ThriftModule());
        ConcourseServer server = injector.getInstance(ConcourseServer.class);
        server.init(port, bufferStore, dbStore);
        return server;
    }

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

        // Run all the pending upgrade tasks
        UpgradeTasks.runLatest();

        // Ensure the application is properly configured
        MemoryUsage heap = ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage();
        if(heap.getInit() < MIN_HEAP_SIZE) {
            System.err.println("Cannot initialize Concourse Server with "
                    + "a heap smaller than " + MIN_HEAP_SIZE + " bytes");
            System.exit(127);
        }

        // Create an instance of the server and all of its dependencies
        final ConcourseServer server = ConcourseServer.create();

        // Check if concourse is in inconsistent state.
        if(GlobalState.SYSTEM_ID == null) {
            throw new IllegalStateException(
                    "Concourse is in inconsistent state because "
                            + "the System ID in the buffer and database directories are different");
        }

        // Start the server...
        Thread serverThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    CommandLine.displayWelcomeBanner();
                    System.out.println("System ID: " + GlobalState.SYSTEM_ID);
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

        // "Warm up" the ANTLR parsing engine in the background
        new Thread(new Runnable() {

            @Override
            public void run() {
                NaturalLanguage.parseMicros("now");
            }

        }).start();

        // Add a shutdown hook that launches the official {@link ShutdownRunner}
        // in cases where the server process is directly killed (i.e. from the
        // control script)
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
     * Return the appropriate collection for a result dataset, depending upon
     * the execution thread.
     * 
     * @return the result dataset collection
     */
    private static Map<Long, Map<String, Set<TObject>>> emptyResultDataset() {
        return (INVOCATION_THREAD_CLASS == Thread.currentThread().getClass())
                ? new TObjectResultDataset() : Maps.newLinkedHashMap();
    }

    /**
     * Return the appropriate collection for a result dataset, depending upon
     * the execution thread.
     * 
     * @param capacity the initial capacity for the dataset collection
     * @return the result dataset collection
     */
    private static Map<Long, Map<String, Set<TObject>>> emptyResultDatasetWithCapacity(
            int capacity) {
        return (INVOCATION_THREAD_CLASS == Thread.currentThread().getClass())
                ? new TObjectResultDataset()
                : TMaps.newLinkedHashMapWithCapacity(capacity);
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
     * Contains the credentials used by the {@link #accessManager}. This file is
     * typically located in the root of the server installation.
     */
    private static final String ACCESS_FILE = ".access";

    /**
     * The minimum heap size required to run Concourse Server.
     */
    private static final int MIN_HEAP_SIZE = 268435456; // 256 MB

    /**
     * The number of worker threads that Concourse Server uses.
     */
    private static final int NUM_WORKER_THREADS = 100; // This may become
                                                       // configurable in a
                                                       // prefs file in a
                                                       // future release.

    /**
     * The AccessManager controls access to the server.
     */
    private AccessManager accessManager;

    /**
     * The base location where the indexed buffer pages are stored.
     */
    private String bufferStore;

    /**
     * The base location where the indexed database records are stored.
     */
    private String dbStore;

    /**
     * A mapping from env to the corresponding Engine that controls all the
     * logic for data storage and retrieval.
     */
    private Map<String, Engine> engines;

    /**
     * A server for handling HTTP requests, if the {@code http_port} preference
     * is configured.
     */
    @Nullable
    private HttpServer httpServer;

    /**
     * The Thrift server that handles all managed operations.
     */
    private TServer mgmtServer;

    /**
     * The PluginManager seamlessly handles plugins that are running in separate
     * JVMs.
     */
    private PluginManager pluginManager;

    /**
     * The Thrift server controls the RPC protocol. Use
     * https://github.com/m1ch1/mapkeeper/wiki/Thrift-Java-Servers-Compared for
     * a reference.
     */
    private TServer server;

    /**
     * The server maintains a collection of {@link Transaction} objects to
     * ensure that client requests are properly routed. When the client makes a
     * call to {@link #stage(AccessToken)}, a Transaction is started on the
     * server and a {@link TransactionToken} is used for the client to reference
     * that Transaction in future calls.
     */
    private final Map<TransactionToken, Transaction> transactions = new NonBlockingHashMap<TransactionToken, Transaction>();

    @Override
    @ThrowsThriftExceptions
    @PluginRestricted
    public void abort(AccessToken creds, TransactionToken transaction,
            String env) throws TException {
        checkAccess(creds, transaction);
        transactions.remove(transaction).abort();
    }

    @Override
    @Atomic
    @AutoRetry
    @ThrowsThriftExceptions
    public long addKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        long record = 0;
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                record = Time.now();
                Operations.addIfEmptyAtomic(key, value, record, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return record;
    }

    @Override
    @ThrowsThriftExceptions
    public boolean addKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        if(value.getType() != Type.LINK
                || isValidLink((Link) Convert.thriftToJava(value), record)) {
            return ((BufferedStore) getStore(transaction, environment)).add(key,
                    value, record);
        }
        else {
            return false;
        }
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
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

    @Override
    @VersionControl
    @ThrowsThriftExceptions
    public Map<Long, String> auditKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).audit(key, record);
    }

    @Override
    @Alias
    @VersionControl
    @ThrowsThriftExceptions
    public Map<Long, String> auditKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return auditKeyRecordStartEnd(key, record, start, Time.NONE, creds,
                transaction, environment);
    }

    @Override
    @VersionControl
    @ThrowsThriftExceptions
    public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, String> base = store.audit(key, record);
        Map<Long, String> result = TMaps
                .newLinkedHashMapWithCapacity(base.size());
        int index = Timestamps.findNearestSuccessorForTimestamp(base.keySet(),
                start);
        Entry<Long, String> entry = null;
        for (int i = index; i < base.size(); ++i) {
            entry = Iterables.get(base.entrySet(), i);
            if(entry.getKey() >= end) {
                break;
            }
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, String> auditKeyRecordStartstr(String key, long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return auditKeyRecordStart(key, record,
                NaturalLanguage.parseMicros(start), creds, transaction,
                environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, String> auditKeyRecordStartstrEndstr(String key,
            long record, String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return auditKeyRecordStartEnd(key, record,
                NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @VersionControl
    @ThrowsThriftExceptions
    public Map<Long, String> auditRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).audit(record);
    }

    @Override
    @Alias
    @VersionControl
    @ThrowsThriftExceptions
    public Map<Long, String> auditRecordStart(long record, long start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStartEnd(record, start, Time.NONE, creds, transaction,
                environment);
    }

    @Override
    @VersionControl
    @ThrowsThriftExceptions
    public Map<Long, String> auditRecordStartEnd(long record, long start,
            long end, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, String> base = store.audit(record);
        Map<Long, String> result = TMaps
                .newLinkedHashMapWithCapacity(base.size());
        int index = Timestamps.findNearestSuccessorForTimestamp(base.keySet(),
                start);
        Entry<Long, String> entry = null;
        for (int i = index; i < base.size(); ++i) {
            entry = Iterables.get(base.entrySet(), i);
            if(entry.getKey() >= end) {
                break;
            }
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, String> auditRecordStartstr(long record, String start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStart(record, NaturalLanguage.parseMicros(start),
                creds, transaction, environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, String> auditRecordStartstrEndstr(long record,
            String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStartEnd(record, NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                average = Operations.avgKeyAtomic(key, Time.NONE, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                average = 0;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            Number average = 0;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    average = Operations.avgKeyRecordsAtomic(key, records,
                            Time.NONE, atomic);
                }
                catch (AtomicStateException e) {
                    average = 0;
                    atomic = null;
                }
            }
            return Convert.javaToThrift(average);
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            Number average = 0;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    average = Operations.avgKeyRecordsAtomic(key, records,
                            timestamp, atomic);
                }
                catch (AtomicStateException e) {
                    average = 0;
                    atomic = null;
                }
            }
            return Convert.javaToThrift(average);
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                average = Operations.avgKeyRecordsAtomic(key, records,
                        Time.NONE, atomic);
            }
            catch (AtomicStateException e) {
                average = 0;
                atomic = null;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                average = Operations.avgKeyRecordsAtomic(key, records,
                        timestamp, atomic);
            }
            catch (AtomicStateException e) {
                average = 0;
                atomic = null;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                average = Operations.avgKeyRecordAtomic(key, record, Time.NONE,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                average = 0;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                average = Operations.avgKeyRecordsAtomic(key, records,
                        Time.NONE, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                average = 0;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                average = Operations.avgKeyRecordsAtomic(key, records,
                        timestamp, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                average = 0;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                average = Operations.avgKeyRecordAtomic(key, record, timestamp,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                average = 0;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject averageKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws SecurityException, TransactionException, TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number average = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                average = Operations.avgKeyAtomic(key, timestamp, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                average = 0;
            }
        }
        return Convert.javaToThrift(average);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<TObject, Set<Long>> browseKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).browse(key);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<String, Map<TObject, Set<Long>>> browseKeys(List<String> keys,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Map<TObject, Set<Long>>> result = Maps.newLinkedHashMap();
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

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<String, Map<TObject, Set<Long>>> browseKeysTime(
            List<String> keys, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Map<TObject, Set<Long>>> result = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        for (String key : keys) {
            result.put(key, store.browse(key));
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<String, Map<TObject, Set<Long>>> browseKeysTimestr(
            List<String> keys, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return browseKeysTime(keys, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<TObject, Set<Long>> browseKeyTime(String key, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).browse(key, timestamp);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<TObject, Set<Long>> browseKeyTimestr(String key,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return browseKeyTime(key, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @Atomic
    @AutoRetry
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> chronologizeKeyRecord(String key,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record, 0, Time.now());
    }

    @Override
    @Alias
    @AutoRetry
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> chronologizeKeyRecordStart(String key,
            long record, long start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record, start, Time.NONE);
    }

    @Override
    @AutoRetry
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartEnd(String key,
            long record, long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record, start, end);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record,
                NaturalLanguage.parseMicros(start), Time.now());
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstrEndstr(
            String key, long record, String start, String end,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record,
                NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end));
    }

    @Override
    @Atomic
    @AutoRetry
    @ThrowsThriftExceptions
    public void clearKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Operations.clearKeyRecordAtomic(key, record, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public void clearKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    Operations.clearKeyRecordAtomic(key, record, atomic);
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public void clearKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (String key : keys) {
                    Operations.clearKeyRecordAtomic(key, record, atomic);
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public void clearKeysRecords(List<String> keys, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    for (String key : keys) {
                        Operations.clearKeyRecordAtomic(key, record, atomic);
                    }
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @Atomic
    @AutoRetry
    @ThrowsThriftExceptions
    public void clearRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Operations.clearRecordAtomic(record, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @AutoRetry
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public void clearRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    Operations.clearRecordAtomic(record, atomic);
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @ThrowsThriftExceptions
    @PluginRestricted
    public boolean commit(AccessToken creds, TransactionToken transaction,
            String env) throws TException {
        checkAccess(creds, transaction);
        return transactions.remove(transaction).commit();
    }

    @Override
    @ThrowsThriftExceptions
    public Set<String> describeRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).describe(record);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Set<String>> describeRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
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

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<String>> result = TMaps
                .newLinkedHashMapWithCapacity(records.size());
        for (long record : records) {
            result.put(record, store.describe(record, timestamp));
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Set<String>> describeRecordsTimestr(List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return describeRecordsTime(records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public Set<String> describeRecordTime(long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).describe(record, timestamp);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Set<String> describeRecordTimestr(long record, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return describeRecordTime(record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Diff, Set<TObject>> diffKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyRecordStartEnd(key, record, start,
                Timestamp.now().getMicros(), creds, transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Diff, Set<TObject>> diffKeyRecordStartEnd(String key,
            long record, long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Set<TObject> startValues = null;
        Set<TObject> endValues = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                startValues = store.select(key, record, start);
                endValues = store.select(key, record, end);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        Map<Diff, Set<TObject>> result = Maps.newHashMapWithExpectedSize(2);
        Set<TObject> xor = Sets.symmetricDifference(startValues, endValues);
        int expectedSize = xor.size() / 2;
        Set<TObject> added = Sets.newHashSetWithExpectedSize(expectedSize);
        Set<TObject> removed = Sets.newHashSetWithExpectedSize(expectedSize);
        for (TObject current : xor) {
            if(!startValues.contains(current))
                added.add(current);
            else {
                removed.add(current);
            }
        }
        if(!added.isEmpty()) {
            result.put(Diff.ADDED, added);
        }
        if(!removed.isEmpty()) {
            result.put(Diff.REMOVED, removed);
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Diff, Set<TObject>> diffKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffKeyRecordStart(key, record,
                NaturalLanguage.parseMicros(start), creds, transaction,
                environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Diff, Set<TObject>> diffKeyRecordStartstrEndstr(String key,
            long record, String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffKeyRecordStartEnd(key, record,
                NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStart(String key,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyStartEnd(key, start, Timestamp.now().getMicros(), creds,
                transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartEnd(String key,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Map<TObject, Set<Long>> startData = null;
        Map<TObject, Set<Long>> endData = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                startData = store.browse(key, start);
                endData = store.browse(key, end);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        Set<TObject> startValues = startData.keySet();
        Set<TObject> endValues = endData.keySet();
        Set<TObject> xor = Sets.symmetricDifference(startValues, endValues);
        Set<TObject> intersection = startValues.size() < endValues.size()
                ? Sets.intersection(startValues, endValues)
                : Sets.intersection(endValues, startValues);
        Map<TObject, Map<Diff, Set<Long>>> result = TMaps
                .newLinkedHashMapWithCapacity(xor.size() + intersection.size());
        for (TObject value : xor) {
            Map<Diff, Set<Long>> entry = Maps.newHashMapWithExpectedSize(1);
            if(!startValues.contains(value)) {
                entry.put(Diff.ADDED, endData.get(value));
            }
            else {
                entry.put(Diff.REMOVED, endData.get(value));
            }
            result.put(value, entry);
        }
        for (TObject value : intersection) {
            Set<Long> startRecords = startData.get(value);
            Set<Long> endRecords = endData.get(value);
            Set<Long> xorRecords = Sets.symmetricDifference(startRecords,
                    endRecords);
            if(!xorRecords.isEmpty()) {
                Set<Long> added = Sets
                        .newHashSetWithExpectedSize(xorRecords.size());
                Set<Long> removed = Sets
                        .newHashSetWithExpectedSize(xorRecords.size());
                for (Long record : xorRecords) {
                    if(!startRecords.contains(record)) {
                        added.add(record);
                    }
                    else {
                        removed.add(record);
                    }
                }
                Map<Diff, Set<Long>> entry = Maps.newHashMapWithExpectedSize(2);
                if(!added.isEmpty()) {
                    entry.put(Diff.ADDED, added);
                }
                if(!removed.isEmpty()) {
                    entry.put(Diff.REMOVED, removed);
                }
                result.put(value, entry);
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstr(String key,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyStart(key, NaturalLanguage.parseMicros(start), creds,
                transaction, environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstrEndstr(String key,
            String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffKeyStartEnd(key, NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStart(long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffRecordStartEnd(record, start, Timestamp.now().getMicros(),
                creds, transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartEnd(long record,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Map<String, Set<TObject>> startData = null;
        Map<String, Set<TObject>> endData = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                startData = store.select(record, start);
                endData = store.select(record, end);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        Set<String> startKeys = startData.keySet();
        Set<String> endKeys = endData.keySet();
        Set<String> xor = Sets.symmetricDifference(startKeys, endKeys);
        Set<String> intersection = Sets.intersection(startKeys, endKeys);
        Map<String, Map<Diff, Set<TObject>>> result = TMaps
                .newLinkedHashMapWithCapacity(xor.size() + intersection.size());
        for (String key : xor) {
            Map<Diff, Set<TObject>> entry = Maps.newHashMapWithExpectedSize(1);
            if(!startKeys.contains(key)) {
                entry.put(Diff.ADDED, endData.get(key));
            }
            else {
                entry.put(Diff.REMOVED, endData.get(key));
            }
            result.put(key, entry);
        }
        for (String key : intersection) {
            Set<TObject> startValues = startData.get(key);
            Set<TObject> endValues = endData.get(key);
            Set<TObject> xorValues = Sets.symmetricDifference(startValues,
                    endValues);
            if(!xorValues.isEmpty()) {
                Set<TObject> added = Sets
                        .newHashSetWithExpectedSize(xorValues.size());
                Set<TObject> removed = Sets
                        .newHashSetWithExpectedSize(xorValues.size());
                for (TObject value : xorValues) {
                    if(!startValues.contains(value)) {
                        added.add(value);
                    }
                    else {
                        removed.add(value);
                    }
                }
                Map<Diff, Set<TObject>> entry = Maps
                        .newHashMapWithExpectedSize(2);
                if(!added.isEmpty()) {
                    entry.put(Diff.ADDED, added);
                }
                if(!removed.isEmpty()) {
                    entry.put(Diff.REMOVED, removed);
                }
                result.put(key, entry);
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstr(long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffRecordStart(record, NaturalLanguage.parseMicros(start),
                creds, transaction, environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstrEndstr(
            long record, String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffRecordStartEnd(record, NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Set<Long> findCcl(String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
            AtomicSupport store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Operations.findAtomic(queue, stack, atomic);
                }
                catch (AtomicStateException e) {
                    atomic = null;
                }
            }
            return Sets.newTreeSet(stack.pop());
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Set<Long> findCriteria(TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Operations.findAtomic(queue, stack, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return Sets.newTreeSet(stack.pop());
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Set<Long> findKeyOperatorstrValues(String key, String operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValues(key, Convert.stringToOperator(operator),
                values, creds, transaction, environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTime(key,
                Convert.stringToOperator(operator), values, timestamp, creds,
                transaction, environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Set<Long> findKeyOperatorstrValuesTimestr(String key,
            String operator, List<TObject> values, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorstrValuesTime(key, operator, values,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        TObject[] tValues = values.toArray(new TObject[values.size()]);
        return getStore(transaction, environment).find(key, operator, tValues);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        TObject[] tValues = values.toArray(new TObject[values.size()]);
        return getStore(transaction, environment).find(timestamp, key, operator,
                tValues);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Set<Long> findKeyOperatorValuesTimestr(String key, Operator operator,
            List<TObject> values, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTime(key, operator, values,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @AutoRetry
    @Atomic
    @ThrowsThriftExceptions
    public long findOrAddKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Set<Long> records = Sets.newLinkedHashSetWithExpectedSize(1);
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                records.addAll(atomic.find(key, Operator.EQUALS, value));
                if(records.isEmpty()) {
                    long record = Time.now();
                    Operations.addIfEmptyAtomic(key, value, record, atomic);
                    records.add(record);
                }
            }
            catch (AtomicStateException e) {
                records.clear();
                atomic = null;
            }
        }
        if(records.size() == 1) {
            return Iterables.getOnlyElement(records);
        }
        else {
            throw new DuplicateEntryException(
                    com.cinchapi.concourse.util.Strings.joinWithSpace("Found",
                            records.size(), "records that match", key, "=",
                            value));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    @Atomic
    @ThrowsThriftExceptions
    public long findOrInsertCclJson(String ccl, String json, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        List<Multimap<String, Object>> objects = Lists
                .newArrayList(Convert.jsonToJava(json));
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSet();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Queue<PostfixNotationSymbol> queue = Parser
                        .toPostfixNotation(ccl);
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findOrInsertAtomic(records, objects, queue, stack,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                records.clear();
            }
        }
        if(records.size() == 1) {
            return Iterables.getOnlyElement(records);
        }
        else {
            throw new DuplicateEntryException(
                    com.cinchapi.concourse.util.Strings.joinWithSpace("Found",
                            records.size(), "records that match", ccl));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    @Atomic
    @ThrowsThriftExceptions
    public long findOrInsertCriteriaJson(TCriteria criteria, String json,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        List<Multimap<String, Object>> objects = Lists
                .newArrayList(Convert.jsonToJava(json));
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSet();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Queue<PostfixNotationSymbol> queue = Operations
                        .convertCriteriaToQueue(criteria);
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findOrInsertAtomic(records, objects, queue, stack,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                records.clear();
            }
        }
        if(records.size() == 1) {
            return Iterables.getOnlyElement(records);
        }
        else {
            throw new DuplicateEntryException(
                    com.cinchapi.concourse.util.Strings.joinWithSpace("Found",
                            records.size(), "records that match", Language
                                    .translateFromThriftCriteria(criteria)));
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getCcl(String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, TObject> entry = TMaps
                                .newLinkedHashMapWithCapacity(
                                        atomic.describe(record).size());
                        for (String key : atomic.describe(record)) {
                            try {
                                entry.put(key, Iterables
                                        .getLast(atomic.select(key, record)));
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
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, TObject> entry = TMaps
                                .newLinkedHashMapWithCapacity(atomic
                                        .describe(record, timestamp).size());
                        for (String key : atomic.describe(record, timestamp)) {
                            try {
                                entry.put(key, Iterables.getLast(
                                        atomic.select(key, record, timestamp)));
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
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getCclTimestr(String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getCclTime(ccl, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getCriteria(TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, TObject> entry = TMaps
                            .newLinkedHashMapWithCapacity(
                                    atomic.describe(record).size());
                    for (String key : atomic.describe(record)) {
                        try {
                            entry.put(key, Iterables
                                    .getLast(atomic.select(key, record)));
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
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTime(TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, TObject> entry = TMaps
                            .newLinkedHashMapWithCapacity(
                                    atomic.describe(record, timestamp).size());
                    for (String key : atomic.describe(record, timestamp)) {
                        try {
                            entry.put(key, Iterables
                                    .getLast(atomic.select(key, record)));
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
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCriteriaTime(criteria, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, TObject> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        try {
                            result.put(record, Iterables
                                    .getLast(atomic.select(key, record)));
                        }
                        catch (NoSuchElementException e) {
                            continue;
                        }
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, TObject> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        try {
                            result.put(record, Iterables.getLast(
                                    atomic.select(key, record, timestamp)));
                        }
                        catch (NoSuchElementException e) {
                            continue;
                        }
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyCclTime(key, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, TObject> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    try {
                        result.put(record,
                                Iterables.getLast(atomic.select(key, record)));
                    }
                    catch (NoSuchElementException e) {
                        continue;
                    }
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, TObject> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    try {
                        result.put(record, Iterables.getLast(
                                atomic.select(key, record, timestamp)));
                    }
                    catch (NoSuchElementException e) {
                        continue;
                    }
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject getKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return Iterables.getLast(
                getStore(transaction, environment).select(key, record),
                TObject.NULL);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, TObject> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    try {
                        result.put(record,
                                Iterables.getLast(atomic.select(key, record)));
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

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Map<Long, TObject> result = TMaps
                .newLinkedHashMapWithCapacity(records.size());
        AtomicSupport store = getStore(transaction, environment);
        for (long record : records) {
            try {
                result.put(record, Iterables
                        .getLast(store.select(key, record, timestamp)));
            }
            catch (NoSuchElementException e) {
                continue;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, TObject> getKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public TObject getKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return Iterables.getLast(getStore(transaction, environment).select(key,
                record, timestamp), TObject.NULL);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public TObject getKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysCcl(List<String> keys,
            String ccl, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, TObject> entry = TMaps
                                .newLinkedHashMapWithCapacity(keys.size());
                        for (String key : keys) {
                            try {
                                entry.put(key, Iterables
                                        .getLast(atomic.select(key, record)));
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
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTime(List<String> keys,
            String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, TObject> entry = TMaps
                                .newLinkedHashMapWithCapacity(keys.size());
                        for (String key : keys) {
                            try {
                                entry.put(key, Iterables.getLast(
                                        atomic.select(key, record, timestamp)));
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
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTimestr(List<String> keys,
            String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysCclTime(keys, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteria(List<String> keys,
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, TObject> entry = TMaps
                            .newLinkedHashMapWithCapacity(keys.size());
                    for (String key : keys) {
                        try {
                            entry.put(key, Iterables
                                    .getLast(atomic.select(key, record)));
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
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, TObject> entry = TMaps
                            .newLinkedHashMapWithCapacity(keys.size());
                    for (String key : keys) {
                        try {
                            entry.put(key, Iterables.getLast(
                                    atomic.select(key, record, timestamp)));
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
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysCriteriaTime(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<String, TObject> getKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<String, TObject> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (String key : keys) {
                    try {
                        result.put(key,
                                Iterables.getLast(atomic.select(key, record)));
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

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysRecords(List<String> keys,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    Map<String, TObject> entry = TMaps
                            .newLinkedHashMapWithCapacity(keys.size());
                    for (String key : keys) {
                        try {
                            entry.put(key, Iterables
                                    .getLast(atomic.select(key, record)));
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

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTime(List<String> keys,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Map<Long, Map<String, TObject>> result = TMaps
                .newLinkedHashMapWithCapacity(records.size());
        AtomicSupport store = getStore(transaction, environment);
        for (long record : records) {
            Map<String, TObject> entry = TMaps
                    .newLinkedHashMapWithCapacity(keys.size());
            for (String key : keys) {
                try {
                    entry.put(key, Iterables
                            .getLast(store.select(key, record, timestamp)));
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

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<String, TObject> getKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Map<String, TObject> result = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        AtomicSupport store = getStore(transaction, environment);
        for (String key : keys) {
            try {
                result.put(key, Iterables
                        .getLast(store.select(key, record, timestamp)));
            }
            catch (NoSuchElementException e) {
                continue;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<String, TObject> getKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public String getServerEnvironment(AccessToken creds,
            TransactionToken transaction, String env)
            throws SecurityException, TException {
        checkAccess(creds, transaction);
        return Environments.sanitize(env);
    }

    @Override
    @ManagedOperation
    public String getServerVersion() {
        return Version.getVersion(ConcourseServer.class).toString();
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Set<Long> insertJson(String json, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        List<Multimap<String, Object>> objects = Convert.anyJsonToJava(json);
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSet();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                List<DeferredWrite> deferred = Lists.newArrayList();
                for (Multimap<String, Object> object : objects) {
                    long record = Time.now();
                    atomic.touch(record);
                    if(Operations.insertAtomic(object, record, atomic,
                            deferred)) {
                        records.add(record);
                    }
                    else {
                        throw AtomicStateException.RETRY;
                    }
                }
                Operations.insertDeferredAtomic(deferred, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                records.clear();
            }
        }
        return records;
    }

    @Override
    @Atomic
    @ThrowsThriftExceptions
    public boolean insertJsonRecord(String json, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        try {
            Multimap<String, Object> data = Convert.jsonToJava(json);
            AtomicOperation atomic = store.startAtomicOperation();
            List<DeferredWrite> deferred = Lists.newArrayList();
            return Operations.insertAtomic(data, record, atomic, deferred)
                    && Operations.insertDeferredAtomic(deferred, atomic)
                    && atomic.commit();
        }
        catch (TransactionStateException e) {
            throw new TransactionException();
        }
        catch (AtomicStateException e) {
            return false;
        }
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Boolean> insertJsonRecords(String json, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Map<Long, Boolean> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                List<DeferredWrite> deferred = Lists.newArrayList();
                for (long record : records) {
                    result.put(record, Operations.insertAtomic(data, record,
                            atomic, deferred));
                }
                Operations.insertDeferredAtomic(deferred, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Set<Long> inventory(AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).getAllRecords();
    }

    @Override
    @ThrowsThriftExceptions
    public ComplexTObject invokePlugin(String id, String method,
            List<ComplexTObject> params, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return pluginManager.invoke(id, method, params, creds, transaction,
                environment);
    }

    @Override
    @Atomic
    @AutoRetry
    @ThrowsThriftExceptions
    public String jsonifyRecords(List<Long> records, boolean identifier,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        String json = "";
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                json = Operations.jsonify(records, 0L, identifier, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return json;
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public String jsonifyRecordsTime(List<Long> records, long timestamp,
            boolean identifier, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        return Operations.jsonify(records, timestamp, identifier,
                getStore(transaction, environment));
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public String jsonifyRecordsTimestr(List<Long> records, String timestamp,
            boolean identifier, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return jsonifyRecordsTime(records,
                NaturalLanguage.parseMicros(timestamp), identifier, creds,
                transaction, environment);
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
    @Override
    @PluginRestricted
    public AccessToken login(ByteBuffer username, ByteBuffer password)
            throws TException {
        return login(username, password, DEFAULT_ENVIRONMENT);
    }

    @Override
    @PluginRestricted
    public AccessToken login(ByteBuffer username, ByteBuffer password,
            String environment) throws TException {
        validate(username, password);
        getEngine(environment);
        return accessManager.getNewAccessToken(username);
    }

    @Override
    @PluginRestricted
    public void logout(AccessToken creds) throws TException {
        logout(creds, null);
    }

    @Override
    @PluginRestricted
    public void logout(AccessToken creds, String environment)
            throws TException {
        checkAccess(creds, null);
        accessManager.expireAccessToken(creds);
    }

    /**
     * Return a service {@link AccessToken token} that can be used to
     * authenticate and authorize non-user requests.
     * 
     * @return the {@link AccessToken service token}
     */
    @PluginRestricted
    public AccessToken newServiceToken() {
        return accessManager.getNewServiceToken();
    }

    @Override
    @ThrowsThriftExceptions
    public boolean pingRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return Operations.ping(record, getStore(transaction, environment));
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Boolean> pingRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Boolean> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    result.put(record, Operations.ping(record, atomic));
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Atomic
    @ThrowsThriftExceptions
    public void reconcileKeyRecordValues(String key, long record,
            Set<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Set<TObject> existingValues = store.select(key, record);
                for (TObject existingValue : existingValues) {
                    if(!values.remove(existingValue)) {
                        atomic.remove(key, existingValue, record);
                    }
                }
                for (TObject value : values) {
                    atomic.add(key, value, record);
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @ThrowsThriftExceptions
    public boolean removeKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        if(value.getType() != Type.LINK
                || isValidLink((Link) Convert.thriftToJava(value), record)) {
            return ((BufferedStore) getStore(transaction, environment))
                    .remove(key, value, record);
        }
        else {
            return false;
        }
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
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

    @Override
    @Atomic
    @Batch
    @VersionControl
    @AutoRetry
    @ThrowsThriftExceptions
    public void revertKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    Operations.revertAtomic(key, record, timestamp, atomic);
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public void revertKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @Atomic
    @VersionControl
    @AutoRetry
    @ThrowsThriftExceptions
    public void revertKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Operations.revertAtomic(key, record, timestamp, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public void revertKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeyRecordTime(key, record, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @Atomic
    @Batch
    @VersionControl
    @AutoRetry
    @ThrowsThriftExceptions
    public void revertKeysRecordsTime(List<String> keys, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    for (String key : keys) {
                        Operations.revertAtomic(key, record, timestamp, atomic);
                    }
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public void revertKeysRecordsTimestr(List<String> keys, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @Atomic
    @Batch
    @VersionControl
    @AutoRetry
    @ThrowsThriftExceptions
    public void revertKeysRecordTime(List<String> keys, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (String key : keys) {
                    Operations.revertAtomic(key, record, timestamp, atomic);
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public void revertKeysRecordTimestr(List<String> keys, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Set<Long> search(String key, String query, AccessToken creds,
            TransactionToken transaction, String env) throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, env).search(key, query);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCcl(String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, Set<TObject>> entry = TMaps
                                .newLinkedHashMapWithCapacity(
                                        atomic.describe(record).size());
                        for (String key : atomic.describe(record)) {
                            entry.put(key, atomic.select(key, record));
                        }
                        result.put(record, entry);
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, Set<TObject>> entry = TMaps
                                .newLinkedHashMapWithCapacity(atomic
                                        .describe(record, timestamp).size());
                        for (String key : atomic.describe(record, timestamp)) {
                            entry.put(key,
                                    atomic.select(key, record, timestamp));
                        }
                        result.put(record, entry);
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTimestr(String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectCclTime(ccl, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteria(
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, Set<TObject>> entry = TMaps
                            .newLinkedHashMapWithCapacity(
                                    atomic.describe(record).size());
                    for (String key : atomic.describe(record)) {
                        entry.put(key, atomic.select(key, record));
                    }
                    result.put(record, entry);
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTime(
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, Set<TObject>> entry = TMaps
                            .newLinkedHashMapWithCapacity(
                                    atomic.describe(record, timestamp).size());
                    for (String key : atomic.describe(record, timestamp)) {
                        entry.put(key, atomic.select(key, record, timestamp));
                    }
                    result.put(record, entry);
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectCriteriaTime(criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        result.put(record, atomic.select(key, record));
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        result.put(record,
                                atomic.select(key, record, timestamp));
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyCclTime(key, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyCriteria(String key,
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    result.put(record, atomic.select(key, record));
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    result.put(record, atomic.select(key, record, timestamp));
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Set<TObject> selectKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).select(key, record);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyRecords(String key,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    result.put(record, atomic.select(key, record));
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Atomic
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTime(String key,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<TObject>> result = TMaps
                .newLinkedHashMapWithCapacity(records.size());
        for (long record : records) {
            result.put(record, store.select(key, record, timestamp));
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public Set<TObject> selectKeyRecordTime(String key, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).select(key, record,
                timestamp);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Set<TObject> selectKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCcl(List<String> keys,
            String ccl, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, Set<TObject>> entry = TMaps
                                .newLinkedHashMapWithCapacity(keys.size());
                        for (String key : keys) {
                            entry.put(key, atomic.select(key, record));
                        }
                        result.put(record, entry);
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTime(
            List<String> keys, String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
            AtomicOperation atomic = null;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    for (long record : records) {
                        Map<String, Set<TObject>> entry = TMaps
                                .newLinkedHashMapWithCapacity(keys.size());
                        for (String key : keys) {
                            entry.put(key,
                                    atomic.select(key, record, timestamp));
                        }
                        result.put(record, entry);
                    }
                }
                catch (AtomicStateException e) {
                    result.clear();
                    atomic = null;
                }
            }
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestr(
            List<String> keys, String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCclTime(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteria(
            List<String> keys, TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, Set<TObject>> entry = TMaps
                            .newLinkedHashMapWithCapacity(keys.size());
                    for (String key : keys) {
                        entry.put(key, atomic.select(key, record));
                    }
                    result.put(record, entry);
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                for (long record : records) {
                    Map<String, Set<TObject>> entry = TMaps
                            .newLinkedHashMapWithCapacity(keys.size());
                    for (String key : keys) {
                        entry.put(key, atomic.select(key, record, timestamp));
                    }
                    result.put(record, entry);
                }
            }
            catch (AtomicStateException e) {
                result.clear();
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCriteriaTime(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<String, Set<TObject>> selectKeysRecord(List<String> keys,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Set<TObject>> result = Maps.newLinkedHashMap();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (String key : keys) {
                    result.put(key, atomic.select(key, record));
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecords(
            List<String> keys, List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    Map<String, Set<TObject>> entry = TMaps
                            .newLinkedHashMapWithCapacity(keys.size());
                    for (String key : keys) {
                        entry.put(key, atomic.select(key, record));
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

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDatasetWithCapacity(
                records.size());
        for (long record : records) {
            Map<String, Set<TObject>> entry = TMaps
                    .newLinkedHashMapWithCapacity(keys.size());
            for (String key : keys) {
                entry.put(key, store.select(key, record, timestamp));
            }
            if(!entry.isEmpty()) {
                result.put(record, entry);
            }
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<String, Set<TObject>> selectKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Set<TObject>> result = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        for (String key : keys) {
            result.put(key, store.select(key, record, timestamp));
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<String, Set<TObject>> selectKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @ThrowsThriftExceptions
    public Map<String, Set<TObject>> selectRecord(long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).select(record);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecords(
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDataset();
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                for (long record : records) {
                    result.put(record, atomic.select(record));
                }
            }
            catch (AtomicStateException e) {
                atomic = null;
            }
        }
        return result;
    }

    @Override
    @Batch
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTime(
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Map<String, Set<TObject>>> result = emptyResultDatasetWithCapacity(
                records.size());
        for (long record : records) {
            result.put(record, store.select(record, timestamp));
        }
        return result;
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestr(
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectRecordsTime(records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public Map<String, Set<TObject>> selectRecordTime(long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).select(record, timestamp);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public Map<String, Set<TObject>> selectRecordTimestr(long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectRecordTime(record, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public long setKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return addKeyValue(key, value, creds, transaction, environment);
    }

    @Override
    @ThrowsThriftExceptions
    public void setKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        ((BufferedStore) getStore(transaction, environment)).set(key, value,
                record);
    }

    @Override
    @Atomic
    @Batch
    @ThrowsThriftExceptions
    public void setKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
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

    @Override
    @ThrowsThriftExceptions
    @PluginRestricted
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
    @PluginRestricted
    public void start() throws TTransportException {
        for (Engine engine : engines.values()) {
            engine.start();
        }
        httpServer.start();
        pluginManager.start();
        Thread mgmtThread = new Thread(() -> {
            mgmtServer.serve();
        }, "management-server");
        mgmtThread.setDaemon(true);
        mgmtThread.start();
        System.out.println("The Concourse server has started");
        server.serve();
    }

    /**
     * Stop the server.
     */
    @PluginRestricted
    public void stop() {
        if(server.isServing()) {
            mgmtServer.stop();
            server.stop();
            pluginManager.stop();
            httpServer.stop();
            for (Engine engine : engines.values()) {
                engine.stop();
            }
            System.out.println("The Concourse server has stopped");
        }
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                sum = Operations.sumKeyAtomic(key, Time.NONE, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                sum = 0;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            Number sum = 0;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    sum = Operations.sumKeyRecordsAtomic(key, records,
                            Time.NONE, atomic);
                }
                catch (AtomicStateException e) {
                    sum = 0;
                    atomic = null;
                }
            }
            return Convert.javaToThrift(sum);
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(ccl);
            AtomicSupport store = getStore(transaction, environment);
            AtomicOperation atomic = null;
            Number sum = 0;
            while (atomic == null || !atomic.commit()) {
                atomic = store.startAtomicOperation();
                try {
                    Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                    Operations.findAtomic(queue, stack, atomic);
                    Set<Long> records = stack.pop();
                    sum = Operations.sumKeyRecordsAtomic(key, records,
                            timestamp, atomic);
                }
                catch (AtomicStateException e) {
                    sum = 0;
                    atomic = null;
                }
            }
            return Convert.javaToThrift(sum);
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                sum = Operations.sumKeyRecordsAtomic(key, records, Time.NONE,
                        atomic);
            }
            catch (AtomicStateException e) {
                sum = 0;
                atomic = null;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        Queue<PostfixNotationSymbol> queue = Operations
                .convertCriteriaToQueue(criteria);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> records = stack.pop();
                sum = Operations.sumKeyRecordsAtomic(key, records, timestamp,
                        atomic);
            }
            catch (AtomicStateException e) {
                sum = 0;
                atomic = null;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                sum = Operations.sumKeyRecordAtomic(key, record, Time.NONE,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                sum = 0;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                sum = Operations.sumKeyRecordsAtomic(key, records, Time.NONE,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                sum = 0;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                sum = Operations.sumKeyRecordsAtomic(key, records, timestamp,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                sum = 0;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                sum = Operations.sumKeyRecordAtomic(key, record, timestamp,
                        atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                sum = 0;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public TObject sumKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws SecurityException, TransactionException, TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperation atomic = null;
        Number sum = 0;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                sum = Operations.sumKeyAtomic(key, timestamp, atomic);
            }
            catch (AtomicStateException e) {
                atomic = null;
                sum = 0;
            }
        }
        return Convert.javaToThrift(sum);
    }

    @Override
    @ThrowsThriftExceptions
    public long time(AccessToken creds, TransactionToken token,
            String environment) throws TException {
        return Time.now();
    }

    @Override
    @ThrowsThriftExceptions
    public long timePhrase(String phrase, AccessToken creds,
            TransactionToken token, String environment) throws TException {
        try {
            return NaturalLanguage.parseMicros(phrase);
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Atomic
    @Override
    @ThrowsThriftExceptions
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        try {
            AtomicOperation atomic = getStore(transaction, environment)
                    .startAtomicOperation();
            return atomic.remove(key, expected, record)
                    && atomic.add(key, replacement, record) ? atomic.commit()
                            : false;
        }
        catch (TransactionStateException e) {
            throw new TransactionException();
        }
        catch (AtomicStateException e) {
            return false;
        }
    }

    @Override
    @ThrowsThriftExceptions
    public boolean verifyKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).verify(key, value, record);
    }

    @Override
    @HistoricalRead
    @ThrowsThriftExceptions
    public boolean verifyKeyValueRecordTime(String key, TObject value,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        checkAccess(creds, transaction);
        return getStore(transaction, environment).verify(key, value, record,
                timestamp);
    }

    @Override
    @Alias
    @ThrowsThriftExceptions
    public boolean verifyKeyValueRecordTimestr(String key, TObject value,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return verifyKeyValueRecordTime(key, value, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Atomic
    @Override
    @AutoRetry
    @ThrowsThriftExceptions
    public void verifyOrSet(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String env)
            throws TException {
        checkAccess(creds, transaction);
        AtomicSupport store = getStore(transaction, env);
        AtomicOperation atomic = null;
        while (atomic == null || !atomic.commit()) {
            atomic = store.startAtomicOperation();
            try {
                Set<TObject> values = atomic.select(key, record);
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

    @Override
    protected void checkAccess(AccessToken creds) throws TException {
        checkAccess(creds, null);
    }

    @Override
    protected AccessManager getAccessManager() {
        return accessManager;
    }

    @Override
    protected String getBufferStore() {
        return bufferStore;
    }

    @Override
    protected String getDbStore() {
        return dbStore;
    }

    /**
     * Return the {@link Engine} that is associated with {@code env}. If such an
     * Engine does not exist, create a new one and add it to the collection.
     *
     * @param env
     * @return the Engine
     */
    protected Engine getEngine(String env) {
        Engine engine = engines.get(env);
        if(engine == null) {
            env = Environments.sanitize(env);
            return getEngineUnsafe(env);
        }
        return engine;
    }

    @Override
    protected PluginManager getPluginManager() {
        return pluginManager;
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
    private void checkAccess(AccessToken creds,
            @Nullable TransactionToken transaction)
            throws SecurityException, IllegalArgumentException {
        if(!accessManager.isValidAccessToken(creds)) {
            throw new SecurityException("Invalid access token");
        }
        Preconditions.checkArgument((transaction != null
                && transaction.getAccessToken().equals(creds)
                && transactions.containsKey(transaction))
                || transaction == null);
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
     * Return the {@link Engine} that is associated with {@code env} without
     * performing any sanitization on the name. If such an Engine does not
     * exist, create a new one and add it to the collection.
     */
    private Engine getEngineUnsafe(String env) {
        Engine engine = engines.get(env);
        if(engine == null) {
            engine = new Engine(bufferStore + File.separator + env,
                    dbStore + File.separator + env, env);
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
    private AtomicSupport getStore(TransactionToken transaction, String env) {
        return transaction != null ? transactions.get(transaction)
                : getEngine(env);
    }

    /**
     * Initialize this instance. This method MUST always be called after
     * constructing the instance.
     *
     * @param port - the port on which to listen for client connections
     * @param bufferStore - the location to store {@link Buffer} files
     * @param dbStore - the location to store {@link Database} files
     * @throws TTransportException
     */
    private void init(int port, String bufferStore, String dbStore)
            throws TTransportException {
        Preconditions.checkState(!bufferStore.equalsIgnoreCase(dbStore),
                "Cannot store buffer and database files in the same directory. "
                        + "Please check concourse.prefs.");
        Preconditions.checkState(
                !Strings.isNullOrEmpty(
                        Environments.sanitize(DEFAULT_ENVIRONMENT)),
                "Cannot initialize "
                        + "Concourse Server with a default environment of "
                        + "'%s'. Please use a default environment name that "
                        + "contains only alphanumeric characters.",
                DEFAULT_ENVIRONMENT);
        FileSystem.mkdirs(bufferStore);
        FileSystem.mkdirs(dbStore);
        FileSystem.lock(bufferStore);
        FileSystem.lock(dbStore);
        TServerSocket socket = new TServerSocket(port);
        ConcourseService.Processor<Iface> processor = new ConcourseService.Processor<Iface>(
                this);
        Args args = new TThreadPoolServer.Args(socket);
        args.processor(processor);
        args.maxWorkerThreads(NUM_WORKER_THREADS);
        args.executorService(Executors
                .newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("Client Worker" + " %d").build()));
        // CON-530: Set a lower timeout on the ExecutorService's termination to
        // prevent the server from hanging because of active threads that have
        // not yet been given a task but won't allow shutdown to proceed (i.e.
        // clients from a ConnectionPool).
        args.stopTimeoutVal(2);
        this.server = new TThreadPoolServer(args);
        this.bufferStore = bufferStore;
        this.dbStore = dbStore;
        this.engines = Maps.newConcurrentMap();
        this.accessManager = AccessManager.create(ACCESS_FILE);
        this.httpServer = GlobalState.HTTP_PORT > 0
                ? HttpServer.create(this, GlobalState.HTTP_PORT)
                : HttpServer.disabled();
        getEngine(); // load the default engine
        this.pluginManager = new PluginManager(this,
                GlobalState.CONCOURSE_HOME + File.separator + "plugins");

        // Setup the management server
        TServerSocket mgmtSocket = new TServerSocket(
                GlobalState.MANAGEMENT_PORT);
        TSimpleServer.Args mgmtArgs = new TSimpleServer.Args(mgmtSocket);
        mgmtArgs.processor(new ConcourseManagementService.Processor<>(this));
        this.mgmtServer = new TSimpleServer(mgmtArgs);
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
        if(!accessManager.isExistingUsernamePasswordCombo(username, password)) {
            throw new SecurityException(
                    "Invalid username/password combination.");
        }
    }

    /**
     * A {@link DeferredWrite} is a wrapper around a key, value, and record.
     * This is typically used by Concourse Server to gather certain writes
     * during a batch operation that shouldn't be tried until the end.
     *
     * @author Jeff Nelson
     */
    @Immutable
    public static final class DeferredWrite {

        // NOTE: This class does not define hashCode() or equals() because the
        // defaults are the desired behaviour.

        private final String key;
        private final long record;
        private final Object value;

        /**
         * Construct a new instance.
         *
         * @param key
         * @param value
         * @param record
         */
        public DeferredWrite(String key, Object value, long record) {
            this.key = key;
            this.value = value;
            this.record = record;
        }

        /**
         * Return the key.
         *
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Return the record.
         *
         * @return the record
         */
        public long getRecord() {
            return record;
        }

        /**
         * Return the value.
         *
         * @return the value
         */
        public Object getValue() {
            return value;
        }

    }

    /**
     * A {@link MethodInterceptor} that delegates to the underlying annotated
     * method, but catches specific exceptions and translates them to the
     * appropriate Thrift counterparts.
     */
    static class ThriftExceptionHandler implements MethodInterceptor {

        @Override
        public Object invoke(MethodInvocation invocation) throws Throwable {
            try {
                return invocation.proceed();
            }
            catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(e.getMessage());
            }
            catch (AtomicStateException e) {
                // If an AtomicStateException makes it here, then it must really
                // be a TransactionStateException.
                assert e.getClass() == TransactionStateException.class;
                throw new TransactionException();
            }
            catch (java.lang.SecurityException e) {
                throw new SecurityException(e.getMessage());
            }
            catch (IllegalStateException | JsonParseException e) {
                // java.text.ParseException is checked, so internal server
                // classes don't use it to indicate parse errors. Since most
                // parsing using some sort of state machine, we've adopted the
                // convention to throw IllegalStateExceptions whenever a parse
                // error has occurred.
                throw new ParseException(e.getMessage());
            }
            catch (PluginException e) {
                throw new TException(e);
            }
            catch (TException e) {
                // This clause may seem unnecessary, but some of the server
                // methods manually throw TExceptions, so we need to catch them
                // here and re-throw so that they don't get propagated as
                // TTransportExceptions.
                throw e;
            }
            catch (Throwable t) {
                Logger.warn(
                        "The following exception occurred "
                                + "but was not propagated to the client: {}",
                        t);
                throw Throwables.propagate(t);
            }
        }

    }

    /**
     * A {@link com.google.inject.Module Module} that configures AOP
     * interceptors and injectors that handle Thrift specific needs.
     */
    static class ThriftModule extends AbstractModule {

        @Override
        protected void configure() {
            bindInterceptor(Matchers.subclassesOf(ConcourseServer.class),
                    Matchers.annotatedWith(ThrowsThriftExceptions.class),
                    new ThriftExceptionHandler());

        }

    }

    /**
     * Indicates that a {@link ConcourseServer server} method propagates certain
     * Java exceptions to the client using analogous ones in the
     * {@code com.cinchapi.concourse.thrift} package.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface ThrowsThriftExceptions {}

}
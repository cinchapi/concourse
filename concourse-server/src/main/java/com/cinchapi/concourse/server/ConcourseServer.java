/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import static com.cinchapi.concourse.server.GlobalState.*;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.ccl.util.NaturalLanguage;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.data.sort.SortableColumn;
import com.cinchapi.concourse.data.sort.SortableSet;
import com.cinchapi.concourse.data.sort.SortableTable;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.security.Permission;
import com.cinchapi.concourse.security.Role;
import com.cinchapi.concourse.security.UserService;
import com.cinchapi.concourse.server.aop.ConcourseServerAdvisor;
import com.cinchapi.concourse.server.aop.Internal;
import com.cinchapi.concourse.server.aop.TranslateClientExceptions;
import com.cinchapi.concourse.server.aop.VerifyAccessToken;
import com.cinchapi.concourse.server.aop.VerifyReadPermission;
import com.cinchapi.concourse.server.aop.VerifyWritePermission;
import com.cinchapi.concourse.server.http.HttpServer;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.management.ClientInvokable;
import com.cinchapi.concourse.server.management.ConcourseManagementService;
import com.cinchapi.concourse.server.ops.AtomicOperations;
import com.cinchapi.concourse.server.ops.InsufficientAtomicityException;
import com.cinchapi.concourse.server.ops.Operations;
import com.cinchapi.concourse.server.ops.Stores;
import com.cinchapi.concourse.server.plugin.PluginManager;
import com.cinchapi.concourse.server.plugin.PluginRestricted;
import com.cinchapi.concourse.server.plugin.data.LazyTrackingTObjectResultDataset;
import com.cinchapi.concourse.server.query.Finder;
import com.cinchapi.concourse.server.query.paginate.Pages;
import com.cinchapi.concourse.server.query.paginate.Paging;
import com.cinchapi.concourse.server.query.sort.Orders;
import com.cinchapi.concourse.server.query.sort.Sorting;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.BufferedStore;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.Transaction;
import com.cinchapi.concourse.server.storage.TransactionStateException;
import com.cinchapi.concourse.server.upgrade.UpgradeTasks;
import com.cinchapi.concourse.shell.CommandLine;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.ConcourseCalculateService;
import com.cinchapi.concourse.thrift.ConcourseNavigateService;
import com.cinchapi.concourse.thrift.ConcourseService;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.DuplicateEntryException;
import com.cinchapi.concourse.thrift.ManagementException;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.ParseException;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TOrder;
import com.cinchapi.concourse.thrift.TPage;
import com.cinchapi.concourse.thrift.TransactionException;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Environments;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.TMaps;
import com.cinchapi.concourse.util.Timestamps;
import com.cinchapi.concourse.util.Version;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Accepts requests from clients to read and write data in Concourse. The server
 * is configured with a {@code concourse.prefs} file.
 *
 * @author Jeff Nelson
 */
public class ConcourseServer extends BaseConcourseServer implements
        ConcourseService.Iface,
        ConcourseCalculateService.Iface,
        ConcourseNavigateService.Iface {

    /*
     * IMPORTANT NOTICE
     * ----------------
     * DO NOT declare as FINAL any methods that are intercepted by Guice because
     * doing so will cause the interception to silently fail. See
     * https://github.com/google/guice/wiki/AOP#limitations for more details.
     */

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
     * In general, this factory should only be used by unit tests. Runtime
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
        Injector injector = Guice.createInjector(new ConcourseServerAdvisor());
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
                    "Concourse is in an inconsistent state because "
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
     * Return the appropriate collection for a sortable result dataset,
     * depending upon
     * the execution thread.
     * <p>
     * Please use {@link TMaps#putResultDatasetOptimized} to add data to the
     * returned {@link Map} in the most efficient manner
     * </p>
     * 
     * @return the result dataset collection
     */
    private static SortableTable<Set<TObject>> emptySortableResultDataset() {
        return (REMOTE_INVOCATION_THREAD_CLASS == Thread.currentThread()
                .getClass()) ? new LazyTrackingTObjectResultDataset()
                        : SortableTable.multiValued(Maps.newLinkedHashMap());
    }

    /**
     * Return the appropriate collection for a sortable result dataset,
     * depending upon
     * the execution thread.
     * <p>
     * Please use {@link TMaps#putResultDatasetOptimized} to add data to the
     * returned
     * {@link Map} in the most efficient manner
     * </p>
     * 
     * @param capacity the initial capacity for the dataset collection
     * @return the result dataset collection
     */
    private static SortableTable<Set<TObject>> emptySortableResultDatasetWithCapacity(
            int capacity) {
        return (REMOTE_INVOCATION_THREAD_CLASS == Thread.currentThread()
                .getClass()) ? new LazyTrackingTObjectResultDataset()
                        : SortableTable.multiValued(
                                TMaps.newLinkedHashMapWithCapacity(capacity));
    }

    /**
     * Contains the credentials used by the {@link #users}. This file is
     * typically located in the root of the server installation.
     */
    private static final String ACCESS_FILE = ".access";

    /**
     * The minimum heap size required to run Concourse Server.
     */
    private static final int MIN_HEAP_SIZE = 268435456; // 256 MB

    /**
     * A placeholder to signfiy that no {@link Order} should be imposed on a
     * result set.
     */
    private static final TOrder NO_ORDER = null;

    /**
     * The number of worker threads that Concourse Server uses.
     */
    private static final int NUM_WORKER_THREADS = 100; // This may become
                                                       // configurable in a
                                                       // prefs file in a
                                                       // future release.

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
     * A {@link Inspector} facade that calls through to the {@link #users
     * user service} to inspect access tokens.
     */
    private Inspector inspector;

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

    /**
     * The UserService controls access to the server.
     */
    private UserService users;

    /**
     * Reference to the {@link ConcourseCompiler}.
     */
    private final ConcourseCompiler compiler = ConcourseCompiler.get();

    @Override
    @TranslateClientExceptions
    @PluginRestricted
    @VerifyAccessToken
    public void abort(AccessToken creds, TransactionToken transaction,
            String env) throws TException {
        transactions.remove(transaction).abort();
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public long addKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            long record = Time.now();
            Operations.addIfEmptyAtomic(key, value, record, atomic);
            return record;
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public boolean addKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return ((BufferedStore) getStore(transaction, environment)).add(key,
                value, record);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Boolean> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                result.put(record, atomic.add(key, value, record));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, String> auditKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).audit(key, record);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, String> auditKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return auditKeyRecordStartEnd(key, record, start, Time.NONE, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
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
    @TranslateClientExceptions
    public Map<Long, String> auditKeyRecordStartstr(String key, long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return auditKeyRecordStart(key, record,
                NaturalLanguage.parseMicros(start), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
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
    @TranslateClientExceptions
    public Map<Long, String> auditRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).audit(record);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, String> auditRecordStart(long record, long start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStartEnd(record, start, Time.NONE, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, String> auditRecordStartEnd(long record, long start,
            long end, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
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
    @TranslateClientExceptions
    public Map<Long, String> auditRecordStartstr(long record, String start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStart(record, NaturalLanguage.parseMicros(start),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, String> auditRecordStartstrEndstr(long record,
            String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStartEnd(record, NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number average = Operations.avgKeyAtomic(key, Time.NONE, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number average = Operations.avgKeyRecordsAtomic(key, records,
                        Time.NONE, atomic);
                return Convert.javaToThrift(average);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number average = Operations.avgKeyRecordsAtomic(key, records,
                        timestamp, atomic);
                return Convert.javaToThrift(average);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public TObject averageKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return averageKeyCclTime(key, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number average = Operations.avgKeyRecordsAtomic(key, records,
                    Time.NONE, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number average = Operations.avgKeyRecordsAtomic(key, records,
                    timestamp, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject averageKeyCriteriaTimestr(String key, TCriteria criteria,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return averageKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number average = Operations.avgKeyRecordAtomic(key, record,
                    Time.NONE, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number average = Operations.avgKeyRecordsAtomic(key, records,
                    Time.NONE, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number average = Operations.avgKeyRecordsAtomic(key, records,
                    timestamp, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject averageKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return averageKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number average = Operations.avgKeyRecordAtomic(key, record,
                    timestamp, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject averageKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return averageKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject averageKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws SecurityException, TransactionException, TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number average = Operations.avgKeyAtomic(key, timestamp, atomic);
            return Convert.javaToThrift(average);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject averageKeyTimestr(String key, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return averageKeyTime(key, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<TObject, Set<Long>> browseKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Stores.browse(getStore(transaction, environment), key);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Map<TObject, Set<Long>>> browseKeys(List<String> keys,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Map<TObject, Set<Long>>> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (String key : keys) {
                result.put(key, Stores.browse(atomic, key));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Map<TObject, Set<Long>>> browseKeysTime(
            List<String> keys, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Map<TObject, Set<Long>>> result = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        for (String key : keys) {
            result.put(key, Stores.browse(store, key, timestamp));
        }
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<String, Map<TObject, Set<Long>>> browseKeysTimestr(
            List<String> keys, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return browseKeysTime(keys, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<TObject, Set<Long>> browseKeyTime(String key, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Stores.browse(getStore(transaction, environment), key,
                timestamp);
    }

    @Override
    @TranslateClientExceptions
    public Map<TObject, Set<Long>> browseKeyTimestr(String key,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return browseKeyTime(key, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> chronologizeKeyRecord(String key,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record, 0, Time.now());
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> chronologizeKeyRecordStart(String key,
            long record, long start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record, start, Time.NONE);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartEnd(String key,
            long record, long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record, start, end);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record,
                NaturalLanguage.parseMicros(start), Time.now());
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstrEndstr(
            String key, long record, String start, String end,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return store.chronologize(key, record,
                NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void clearKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Operations.clearKeyRecordAtomic(key, record, atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void clearKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                Operations.clearKeyRecordAtomic(key, record, atomic);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void clearKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (String key : keys) {
                Operations.clearKeyRecordAtomic(key, record, atomic);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void clearKeysRecords(List<String> keys, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                for (String key : keys) {
                    Operations.clearKeyRecordAtomic(key, record, atomic);
                }
            }
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void clearRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Operations.clearRecordAtomic(record, atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void clearRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                Operations.clearRecordAtomic(record, atomic);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    @PluginRestricted
    @VerifyAccessToken
    public boolean commit(AccessToken creds, TransactionToken transaction,
            String env) throws TException {
        return transactions.remove(transaction).commit();
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public boolean consolidateRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        Set<Long> $records = Sets.newLinkedHashSet(records);
        if($records.size() >= 2) {
            AtomicSupport store = getStore(transaction, environment);
            AtomicOperation atomic = store.startAtomicOperation();
            try {
                Iterator<Long> it = $records.iterator();
                long destination = it.next();
                while (it.hasNext()) {
                    // 1. Copy all data from the #source to the #destination
                    long source = it.next();
                    Map<String, Set<TObject>> data = store.select(source);
                    for (Entry<String, Set<TObject>> entry : data.entrySet()) {
                        String key = entry.getKey();
                        for (TObject value : entry.getValue()) {
                            if(!atomic.verify(key, value, destination)
                                    && !atomic.add(key, value, destination)) {
                                return false;
                            }
                        }
                    }
                    // 2. Replace all incoming links to #source with links to
                    // #destination
                    Map<String, Set<Long>> incoming = Operations
                            .traceRecordAtomic(source, Time.NONE, atomic);
                    for (Entry<String, Set<Long>> entry : incoming.entrySet()) {
                        String key = entry.getKey();
                        for (long record : entry.getValue()) {
                            if(!atomic.remove(key,
                                    Convert.javaToThrift(Link.to(source)),
                                    record)) {
                                return false;
                            }
                            if(!atomic.add(key,
                                    Convert.javaToThrift(Link.to(destination)),
                                    record)) {
                                return false;
                            }
                        }
                    }
                    // 3. Clear the #source
                    Operations.clearRecordAtomic(source, atomic);
                }
                return atomic.commit();
            }
            catch (TransactionStateException e) {
                throw new TransactionException();
            }
            catch (AtomicStateException e) {
                return false;
            }
        }
        else {
            // Consolidating fewer than 2 records has no logical effect, so
            // don't return a truthy value.
            return false;
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store,
                atomic -> Operations.countKeyAtomic(key, Time.NONE, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                return Operations.countKeyRecordsAtomic(key, records, Time.NONE,
                        atomic);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                return Operations.countKeyRecordsAtomic(key, records, timestamp,
                        atomic);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public long countKeyCclTimestr(String key, String ccl, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return countKeyCclTime(key, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            return Operations.countKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            return Operations.countKeyRecordsAtomic(key, records, timestamp,
                    atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    public long countKeyCriteriaTimestr(String key, TCriteria criteria,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return countKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .countKeyRecordAtomic(key, record, Time.NONE, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .countKeyRecordsAtomic(key, records, Time.NONE, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .countKeyRecordsAtomic(key, records, timestamp, atomic));
    }

    @Override
    @TranslateClientExceptions
    public long countKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return countKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .countKeyRecordAtomic(key, record, timestamp, atomic));
    }

    @Override
    @TranslateClientExceptions
    public long countKeyRecordTimestr(String key, long record, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return countKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long countKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store,
                atomic -> Operations.countKeyAtomic(key, timestamp, atomic));
    }

    @Override
    @TranslateClientExceptions
    public long countKeyTimestr(String key, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return countKeyTime(key, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<String> describe(AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Set<String> result = Sets.newLinkedHashSet();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Set<Long> records = store.getAllRecords();
            for (long record : records) {
                result.addAll(store.describe(record));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<String> describeRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).describe(record);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<String>> describeRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<String>> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                result.put(record, atomic.describe(record));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Set<String>> result = TMaps
                .newLinkedHashMapWithCapacity(records.size());
        for (long record : records) {
            result.put(record, store.describe(record, timestamp));
        }
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<String>> describeRecordsTimestr(List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return describeRecordsTime(records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<String> describeRecordTime(long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).describe(record, timestamp);
    }

    @Override
    @TranslateClientExceptions
    public Set<String> describeRecordTimestr(long record, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return describeRecordTime(record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<String> describeTime(long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Set<String> result = Sets.newLinkedHashSet();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Set<Long> records = store.getAllRecords();
            for (long record : records) {
                result.addAll(store.describe(record, timestamp));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<String> describeTimestr(String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return describeTime(NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Diff, Set<TObject>> diffKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyRecordStartEnd(key, record, start,
                Timestamp.now().getMicros(), creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Diff, Set<TObject>> diffKeyRecordStartEnd(String key,
            long record, long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicReference<Set<TObject>> startValues = new AtomicReference<>(null);
        AtomicReference<Set<TObject>> endValues = new AtomicReference<>(null);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            startValues.set(store.select(key, record, start));
            endValues.set(store.select(key, record, end));
        });
        Map<Diff, Set<TObject>> result = Maps.newHashMapWithExpectedSize(2);
        Set<TObject> xor = Sets.symmetricDifference(startValues.get(),
                endValues.get());
        int expectedSize = xor.size() / 2;
        Set<TObject> added = Sets.newHashSetWithExpectedSize(expectedSize);
        Set<TObject> removed = Sets.newHashSetWithExpectedSize(expectedSize);
        for (TObject current : xor) {
            if(!startValues.get().contains(current))
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
    @TranslateClientExceptions
    public Map<Diff, Set<TObject>> diffKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffKeyRecordStart(key, record,
                NaturalLanguage.parseMicros(start), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
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
    @TranslateClientExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStart(String key,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyStartEnd(key, start, Timestamp.now().getMicros(), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartEnd(String key,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicReference<Map<TObject, Set<Long>>> startData = new AtomicReference<>(
                null);
        AtomicReference<Map<TObject, Set<Long>>> endData = new AtomicReference<>(
                null);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            startData.set(store.browse(key, start));
            endData.set(store.browse(key, end));
        });
        Set<TObject> startValues = startData.get().keySet();
        Set<TObject> endValues = endData.get().keySet();
        Set<TObject> xor = Sets.symmetricDifference(startValues, endValues);
        Set<TObject> intersection = startValues.size() < endValues.size()
                ? Sets.intersection(startValues, endValues)
                : Sets.intersection(endValues, startValues);
        Map<TObject, Map<Diff, Set<Long>>> result = TMaps
                .newLinkedHashMapWithCapacity(xor.size() + intersection.size());
        for (TObject value : xor) {
            Map<Diff, Set<Long>> entry = Maps.newHashMapWithExpectedSize(1);
            if(!startValues.contains(value)) {
                entry.put(Diff.ADDED, endData.get().get(value));
            }
            else {
                entry.put(Diff.REMOVED, endData.get().get(value));
            }
            result.put(value, entry);
        }
        for (TObject value : intersection) {
            Set<Long> startRecords = startData.get().get(value);
            Set<Long> endRecords = endData.get().get(value);
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
    @TranslateClientExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstr(String key,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyStart(key, NaturalLanguage.parseMicros(start), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstrEndstr(String key,
            String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffKeyStartEnd(key, NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStart(long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffRecordStartEnd(record, start, Timestamp.now().getMicros(),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartEnd(long record,
            long start, long end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicReference<Map<String, Set<TObject>>> startData = new AtomicReference<>(
                null);
        AtomicReference<Map<String, Set<TObject>>> endData = new AtomicReference<>(
                null);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            startData.set(store.select(record, start));
            endData.set(store.select(record, end));
        });
        Set<String> startKeys = startData.get().keySet();
        Set<String> endKeys = endData.get().keySet();
        Set<String> xor = Sets.symmetricDifference(startKeys, endKeys);
        Set<String> intersection = Sets.intersection(startKeys, endKeys);
        Map<String, Map<Diff, Set<TObject>>> result = TMaps
                .newLinkedHashMapWithCapacity(xor.size() + intersection.size());
        for (String key : xor) {
            Map<Diff, Set<TObject>> entry = Maps.newHashMapWithExpectedSize(1);
            if(!startKeys.contains(key)) {
                entry.put(Diff.ADDED, endData.get().get(key));
            }
            else {
                entry.put(Diff.REMOVED, endData.get().get(key));
            }
            result.put(key, entry);
        }
        for (String key : intersection) {
            Set<TObject> startValues = startData.get().get(key);
            Set<TObject> endValues = endData.get().get(key);
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
    @TranslateClientExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstr(long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffRecordStart(record, NaturalLanguage.parseMicros(start),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstrEndstr(
            long record, String start, String end, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return diffRecordStartEnd(record, NaturalLanguage.parseMicros(start),
                NaturalLanguage.parseMicros(end), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findCcl(String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findCclOrder(ccl, NO_ORDER, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<Long> findCclOrder(String ccl, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                SortableSet<Set<TObject>> records = SortableSet
                        .of(ast.accept(Finder.instance(), atomic));
                records.sort(Sorting.byValues(Orders.from(order), store));
                return records;
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findCclOrderPage(String ccl, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                findCclOrder(ccl, order, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findCclPage(String ccl, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(findCcl(ccl, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findCriteria(TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findCriteriaOrder(criteria, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<Long> findCriteriaOrder(TCriteria criteria, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            SortableSet<Set<TObject>> records = SortableSet
                    .of(ast.accept(Finder.instance(), atomic));
            records.sort(Sorting.byValues(Orders.from(order), store));
            return records;
        });
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findCriteriaOrderPage(TCriteria criteria, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(findCriteriaOrder(criteria, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findCriteriaPage(TCriteria criteria, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                findCriteria(criteria, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValues(String key, String operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValues(key, Convert.stringToOperator(operator),
                values, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesOrder(String key, String operator,
            List<TObject> values, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesOrder(key,
                Convert.stringToOperator(operator), values, order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesOrderPage(String key,
            String operator, List<TObject> values, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging
                .paginate(
                        findKeyOperatorstrValuesOrder(key, operator, values,
                                order, creds, transaction, environment),
                        Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesPage(String key, String operator,
            List<TObject> values, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(findKeyOperatorstrValues(key, operator, values,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTime(key,
                Convert.stringToOperator(operator), values, timestamp, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimeOrder(String key,
            String operator, List<TObject> values, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTimeOrder(key,
                Convert.stringToOperator(operator), values, timestamp, order,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimeOrderPage(String key,
            String operator, List<TObject> values, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(
                findKeyOperatorstrValuesTimeOrder(key, operator, values,
                        timestamp, order, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimePage(String key,
            String operator, List<TObject> values, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging
                .paginate(
                        findKeyOperatorstrValuesTime(key, operator, values,
                                timestamp, creds, transaction, environment),
                        Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimestr(String key,
            String operator, List<TObject> values, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorstrValuesTime(key, operator, values,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimestrOrder(String key,
            String operator, List<TObject> values, String timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return findKeyOperatorstrValuesTimeOrder(key, operator, values,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimestrOrderPage(String key,
            String operator, List<TObject> values, String timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                findKeyOperatorstrValuesTimestrOrder(key, operator, values,
                        timestamp, order, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorstrValuesTimestrPage(String key,
            String operator, List<TObject> values, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                findKeyOperatorstrValuesTimestr(key, operator, values,
                        timestamp, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesOrder(key, operator, values, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<Long> findKeyOperatorValuesOrder(String key, Operator operator,
            List<TObject> values, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        TObject[] tValues = values.toArray(Array.containing());
        AtomicSupport store = getStore(transaction, environment);
        SortableSet<Set<TObject>> records = null;
        boolean requiresAtomicity = false;
        while (records == null) {
            try {
                Function<Store, SortableSet<Set<TObject>>> function = $store -> {
                    SortableSet<Set<TObject>> $records = SortableSet
                            .of(Stores.find($store, key, operator, tValues));
                    $records.sort(Sorting.byValues(Orders.from(order), store));
                    return $records;
                };
                if(requiresAtomicity) {
                    records = AtomicOperations
                            .<SortableSet<Set<TObject>>> supplyWithRetry(store,
                                    atomic -> function.apply(atomic));
                }
                else {
                    records = function.apply(store);
                }
            }
            catch (InsufficientAtomicityException e) {
                requiresAtomicity = true;
            }
        }
        return records;
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesOrderPage(String key,
            Operator operator, List<TObject> values, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(findKeyOperatorValuesOrder(key, operator, values,
                order, creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesPage(String key, Operator operator,
            List<TObject> values, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(findKeyOperatorValues(key, operator, values,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTimeOrder(key, operator, values, timestamp,
                NO_ORDER, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<Long> findKeyOperatorValuesTimeOrder(String key,
            Operator operator, List<TObject> values, long timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        TObject[] tValues = values.toArray(Array.containing());
        AtomicSupport store = getStore(transaction, environment);
        SortableSet<Set<TObject>> records = SortableSet
                .of(Stores.find(store, timestamp, key, operator, tValues));
        // NOTE: The #timestamp is not considered when sorting because it is a
        // component of criteria evaluation and no data is being selected.
        records.sort(Sorting.byValues(Orders.from(order), store));
        return records;
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTimeOrderPage(String key,
            Operator operator, List<TObject> values, long timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                findKeyOperatorValuesTimeOrder(key, operator, values, timestamp,
                        order, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTimePage(String key,
            Operator operator, List<TObject> values, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(findKeyOperatorValuesTime(key, operator, values,
                timestamp, creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTimestr(String key, Operator operator,
            List<TObject> values, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTime(key, operator, values,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTimestrOrder(String key,
            Operator operator, List<TObject> values, String timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return findKeyOperatorValuesTimeOrder(key, operator, values,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTimestrOrderPage(String key,
            Operator operator, List<TObject> values, String timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                findKeyOperatorValuesTimestrOrder(key, operator, values,
                        timestamp, order, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    public Set<Long> findKeyOperatorValuesTimestrPage(String key,
            Operator operator, List<TObject> values, String timestamp,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging
                .paginate(
                        findKeyOperatorValuesTimestr(key, operator, values,
                                timestamp, creds, transaction, environment),
                        Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long findOrAddKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSetWithExpectedSize(1);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            records.clear();
            records.addAll(atomic.find(key, Operator.EQUALS, value));
            if(records.isEmpty()) {
                long record = Time.now();
                Operations.addIfEmptyAtomic(key, value, record, atomic);
                records.add(record);
            }
        });
        if(records.size() == 1) {
            return Iterables.getOnlyElement(records);
        }
        else {
            throw new DuplicateEntryException(AnyStrings.joinWithSpace("Found",
                    records.size(), "records that match", key, "=", value));
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public long findOrInsertCclJson(String ccl, String json, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        List<Multimap<String, Object>> objects = Lists
                .newArrayList(Convert.jsonToJava(json));
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSet();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            records.clear();
            AbstractSyntaxTree ast;
            if(objects.size() == 1) {
                // CON-321: Support local resolution when the data blob is a
                // single object
                ast = compiler.parse(ccl, objects.get(0));
            }
            else {
                ast = compiler.parse(ccl);
            }
            Operations.findOrInsertAtomic(records, objects, ast, atomic);
        });
        if(records.size() == 1) {
            return Iterables.getOnlyElement(records);
        }
        else {
            throw new DuplicateEntryException(AnyStrings.joinWithSpace("Found",
                    records.size(), "records that match", ccl));
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public long findOrInsertCriteriaJson(TCriteria criteria, String json,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        List<Multimap<String, Object>> objects = Lists
                .newArrayList(Convert.jsonToJava(json));
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSet();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            records.clear();
            Operations.findOrInsertAtomic(records, objects, ast, atomic);
        });
        if(records.size() == 1) {
            return Iterables.getOnlyElement(records);
        }
        else {
            throw new DuplicateEntryException(AnyStrings.joinWithSpace("Found",
                    records.size(), "records that match",
                    Language.translateFromThriftCriteria(criteria).ccl()));
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCcl(String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCclOrder(ccl, NO_ORDER, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCclOrder(String ccl, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getAstAtomic(ast, Time.NONE, result,
                            null,
                            $result -> $result.sort(Sorting
                                    .byValue(Orders.from(order), atomic)),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclOrderPage(String ccl,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                getCclOrder(ccl, order, creds, transaction, environment),
                Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCclPage(String ccl, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getAstAtomic(
                            ast, Time.NONE, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getCclTimeOrder(ccl, timestamp, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCclTimeOrder(String ccl,
            long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getAstAtomic(ast, timestamp, result,
                            null,
                            $result -> $result.sort(
                                    Sorting.byValue(Orders.from(order), atomic),
                                    timestamp),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclTimeOrderPage(String ccl,
            long timestamp, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getCclTimeOrder(ccl, timestamp, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCclTimePage(String ccl,
            long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getAstAtomic(
                            ast, timestamp, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclTimestr(String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getCclTime(ccl, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclTimestrOrder(String ccl,
            String timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCclTimeOrder(ccl, NaturalLanguage.parseMicros(timestamp),
                order, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclTimestrOrderPage(String ccl,
            String timestamp, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCclTimeOrderPage(ccl, NaturalLanguage.parseMicros(timestamp),
                order, page, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCclTimestrPage(String ccl,
            String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCclTimePage(ccl, NaturalLanguage.parseMicros(timestamp), page,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteria(TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getCriteriaOrder(criteria, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCriteriaOrder(TCriteria criteria,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getAstAtomic(ast, Time.NONE, result, null,
                        $result -> $result.sort(
                                Sorting.byValue(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaOrderPage(
            TCriteria criteria, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getCriteriaOrder(criteria, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCriteriaPage(TCriteria criteria,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getAstAtomic(ast, Time.NONE, result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTime(TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getCriteriaTimeOrder(criteria, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCriteriaTimeOrder(
            TCriteria criteria, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations
                .executeWithRetry(store,
                        atomic -> Operations.getAstAtomic(ast, timestamp,
                                result, null,
                                $result -> $result.sort(Sorting.byValue(
                                        Orders.from(order), atomic), timestamp),
                                atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTimeOrderPage(
            TCriteria criteria, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getCriteriaTimeOrder(criteria, timestamp, order,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getCriteriaTimePage(
            TCriteria criteria, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getAstAtomic(ast, timestamp, result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCriteriaTime(criteria, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTimestrOrder(
            TCriteria criteria, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getCriteriaTimeOrder(criteria,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTimestrOrderPage(
            TCriteria criteria, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getCriteriaTimeOrderPage(criteria,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getCriteriaTimestrPage(
            TCriteria criteria, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getCriteriaTimePage(criteria,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyCclOrder(key, ccl, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCclOrder(String key, String ccl,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<TObject> result = SortableColumn.singleValued(key,
                    Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeyAstAtomic(key, ast, Time.NONE,
                            result, null,
                            $result -> $result.sort(Sorting
                                    .byValue(Orders.from(order), atomic)),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclOrderPage(String key, String ccl,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeyCclOrder(key, ccl, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCclPage(String key, String ccl, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<TObject> result = SortableColumn.singleValued(key,
                    Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeyAstAtomic(
                            key, ast, Time.NONE, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyCclTimeOrder(key, ccl, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCclTimeOrder(String key, String ccl,
            long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<TObject> result = SortableColumn.singleValued(key,
                    Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeyAstAtomic(key, ast, timestamp,
                            result, null,
                            $result -> $result.sort(
                                    Sorting.byValue(Orders.from(order), atomic),
                                    timestamp),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclTimeOrderPage(String key, String ccl,
            long timestamp, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeyCclTimeOrder(key, ccl, timestamp, order,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCclTimePage(String key, String ccl,
            long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<TObject> result = SortableColumn.singleValued(key,
                    Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeyAstAtomic(
                            key, ast, timestamp, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyCclTime(key, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclTimestrOrder(String key, String ccl,
            String timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyCclTimeOrder(key, ccl,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclTimestrOrderPage(String key, String ccl,
            String timestamp, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyCclTimeOrderPage(key, ccl,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCclTimestrPage(String key, String ccl,
            String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyCclTimePage(key, ccl,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyCriteriaOrder(key, criteria, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCriteriaOrder(String key,
            TCriteria criteria, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeyAstAtomic(key, ast, Time.NONE,
                        result, null,
                        $result -> $result.sort(
                                Sorting.byValue(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaOrderPage(String key,
            TCriteria criteria, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeyCriteriaOrder(key, criteria, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCriteriaPage(String key, TCriteria criteria,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeyAstAtomic(key, ast, Time.NONE,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyCriteriaTimeOrder(key, criteria, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCriteriaTimeOrder(String key,
            TCriteria criteria, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations
                .executeWithRetry(store,
                        atomic -> Operations.getKeyAstAtomic(key, ast,
                                timestamp, result, null,
                                $result -> $result.sort(Sorting.byValue(
                                        Orders.from(order), atomic), timestamp),
                                atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaTimeOrderPage(String key,
            TCriteria criteria, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeyCriteriaTimeOrder(key, criteria, timestamp,
                order, creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyCriteriaTimePage(String key,
            TCriteria criteria, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeyAstAtomic(key, ast, timestamp,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaTimestrOrder(String key,
            TCriteria criteria, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyCriteriaTimeOrder(key, criteria,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaTimestrOrderPage(String key,
            TCriteria criteria, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyCriteriaTimeOrderPage(key, criteria,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyCriteriaTimestrPage(String key,
            TCriteria criteria, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyCriteriaTimePage(key, criteria,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject getKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Function<Store, TObject> function = $store -> Iterables
                .getLast(Stores.select($store, key, record), TObject.NULL);
        try {
            return function.apply(store);
        }
        catch (InsufficientAtomicityException e) {
            return AtomicOperations.supplyWithRetry(store,
                    atomic -> function.apply(atomic));
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsOrder(key, records, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyRecordsOrder(String key, List<Long> records,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeyRecordsAtomic(key, records, result,
                        null,
                        $result -> $result.sort(
                                Sorting.byValue(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsOrderPage(String key,
            List<Long> records, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeyRecordsOrder(key, records, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyRecordsPage(String key, List<Long> records,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeyRecordsAtomic(key, records, result,
                        $records -> Paging.paginate($records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyRecordsTimeOrder(key, records, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyRecordsTimeOrder(String key,
            List<Long> records, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        Operations.getKeyRecordsOptionalAtomic(key, records, timestamp, result,
                null,
                $result -> $result.sort(
                        Sorting.byValue(Orders.from(order), store), timestamp),
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsTimeOrderPage(String key,
            List<Long> records, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeyRecordsTimeOrder(key, records, timestamp,
                order, creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, TObject> getKeyRecordsTimePage(String key,
            List<Long> records, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<TObject> result = SortableColumn.singleValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        Operations.getKeyRecordsOptionalAtomic(key, records, timestamp, result,
                $records -> Paging.paginate($records, Pages.from(page)), null,
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsTimestrOrder(key, records, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsTimestrOrder(String key,
            List<Long> records, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsTimeOrder(key, records,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsTimestrOrderPage(String key,
            List<Long> records, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsTimeOrderPage(key, records,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, TObject> getKeyRecordsTimestrPage(String key,
            List<Long> records, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsTimePage(key, records,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject getKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Iterables
                .getLast(Stores.select(getStore(transaction, environment), key,
                        record, timestamp), TObject.NULL);
    }

    @Override
    @TranslateClientExceptions
    public TObject getKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCcl(List<String> keys,
            String ccl, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysCclOrder(keys, ccl, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCclOrder(List<String> keys,
            String ccl, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeysAstAtomic(keys, ast, Time.NONE,
                            result, null,
                            $result -> $result.sort(Sorting
                                    .byValue(Orders.from(order), atomic)),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclOrderPage(
            List<String> keys, String ccl, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeysCclOrder(keys, ccl, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCclPage(List<String> keys,
            String ccl, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeysAstAtomic(
                            keys, ast, Time.NONE, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTime(List<String> keys,
            String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysCclTimeOrder(keys, ccl, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCclTimeOrder(
            List<String> keys, String ccl, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeysAstAtomic(keys, ast, timestamp,
                            result, null,
                            $result -> $result.sort(
                                    Sorting.byValue(Orders.from(order), atomic),
                                    timestamp),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTimeOrderPage(
            List<String> keys, String ccl, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(getKeysCclTimeOrder(keys, ccl, timestamp, order,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCclTimePage(List<String> keys,
            String ccl, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<TObject> result = SortableTable
                    .singleValued(Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.getKeysAstAtomic(
                            keys, ast, timestamp, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTimestr(List<String> keys,
            String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysCclTime(keys, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTimestrOrder(
            List<String> keys, String ccl, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysCclTimeOrder(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTimestrOrderPage(
            List<String> keys, String ccl, String timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysCclTimeOrderPage(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCclTimestrPage(
            List<String> keys, String ccl, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysCclTimePage(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteria(List<String> keys,
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysCriteriaOrder(keys, criteria, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCriteriaOrder(
            List<String> keys, TCriteria criteria, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeysAstAtomic(keys, ast, Time.NONE,
                        result, null,
                        $result -> $result.sort(
                                Sorting.byValue(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaOrderPage(
            List<String> keys, TCriteria criteria, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeysCriteriaOrder(keys, criteria, order,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCriteriaPage(
            List<String> keys, TCriteria criteria, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeysAstAtomic(keys, ast, Time.NONE,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysCriteriaTimeOrder(keys, criteria, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimeOrder(
            List<String> keys, TCriteria criteria, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations
                .executeWithRetry(store,
                        atomic -> Operations.getKeysAstAtomic(keys, ast,
                                timestamp, result, null,
                                $result -> $result.sort(Sorting.byValue(
                                        Orders.from(order), atomic), timestamp),
                                atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimeOrderPage(
            List<String> keys, TCriteria criteria, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging
                .paginate(
                        getKeysCriteriaTimeOrder(keys, criteria, timestamp,
                                order, creds, transaction, environment),
                        Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimePage(
            List<String> keys, TCriteria criteria, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeysAstAtomic(keys, ast, timestamp,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysCriteriaTime(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestrOrder(
            List<String> keys, TCriteria criteria, String timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysCriteriaTimeOrder(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestrOrderPage(
            List<String> keys, TCriteria criteria, String timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysCriteriaTimeOrderPage(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestrPage(
            List<String> keys, TCriteria criteria, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysCriteriaTimePage(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, TObject> getKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<String, TObject> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (String key : keys) {
                try {
                    result.put(key, Iterables
                            .getLast(Stores.select(atomic, key, record)));
                }
                catch (NoSuchElementException e) {
                    continue;
                }
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecords(List<String> keys,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysRecordsOrder(keys, records, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysRecordsOrder(
            List<String> keys, List<Long> records, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeysRecordsAtomic(keys, records, result,
                        null,
                        $result -> $result.sort(
                                Sorting.byValue(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsOrderPage(
            List<String> keys, List<Long> records, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(getKeysRecordsOrder(keys, records, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysRecordsPage(List<String> keys,
            List<Long> records, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable
                .singleValued(Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.getKeysRecordsAtomic(keys, records, result,
                        $records -> Paging.paginate($records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTime(List<String> keys,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordsTimeOrder(keys, records, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysRecordsTimeOrder(
            List<String> keys, List<Long> records, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable.singleValued(
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        Operations.getKeysRecordsOptionalAtomic(keys, records, timestamp,
                result, null,
                $result -> $result.sort(
                        Sorting.byValue(Orders.from(order), store), timestamp),
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTimeOrderPage(
            List<String> keys, List<Long> records, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(getKeysRecordsTimeOrder(keys, records, timestamp,
                order, creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, TObject>> getKeysRecordsTimePage(
            List<String> keys, List<Long> records, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<TObject> result = SortableTable.singleValued(
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        Operations.getKeysRecordsOptionalAtomic(keys, records, timestamp,
                result, $records -> Paging.paginate($records, Pages.from(page)),
                null, store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestrOrder(
            List<String> keys, List<Long> records, String timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysRecordsTimeOrder(keys, records,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestrOrderPage(
            List<String> keys, List<Long> records, String timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordsTimeOrderPage(keys, records,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestrPage(
            List<String> keys, List<Long> records, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordsTimePage(keys, records,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, TObject> getKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<String, TObject> result = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        for (String key : keys) {
            try {
                result.put(key, Iterables
                        .getLast(Stores.select(store, key, record, timestamp)));
            }
            catch (NoSuchElementException e) {
                continue;
            }
        }
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<String, TObject> getKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public String getServerEnvironment(AccessToken creds,
            TransactionToken transaction, String env)
            throws SecurityException, TException {
        return Environments.sanitize(env);
    }

    @Override
    @ManagedOperation
    public String getServerVersion() {
        return Version.getVersion(ConcourseServer.class).toString();
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public Set<Long> insertJson(String json, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        List<Multimap<String, Object>> objects = Convert.anyJsonToJava(json);
        AtomicSupport store = getStore(transaction, environment);
        Set<Long> records = Sets.newLinkedHashSet();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            records.clear();
            List<DeferredWrite> deferred = Lists.newArrayList();
            for (Multimap<String, Object> object : objects) {
                long record;
                if(object
                        .containsKey(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
                    // If the $id$ is specified in the JSON blob, insert the
                    // data into that record since no record(s) are provided
                    // as method parameters.

                    // WARNING: This means that doing the equivalent of
                    // `insert(jsonify(records))` will cause an infinite
                    // loop because this method will attempt to insert the
                    // data into the same records from which it was
                    // exported. Therefore, advise users to not export data
                    // with the $id and import that same data in the same
                    // environment.
                    record = ((Number) Iterables.getOnlyElement(object
                            .get(Constants.JSON_RESERVED_IDENTIFIER_NAME)))
                                    .longValue();
                }
                else {
                    record = Time.now();
                }
                atomic.touch(record);
                if(Operations.insertAtomic(object, record, atomic, deferred)) {
                    records.add(record);
                }
                else {
                    throw AtomicStateException.RETRY;
                }
            }
            Operations.insertDeferredAtomic(deferred, atomic);
        });
        return records;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public boolean insertJsonRecord(String json, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
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
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public Map<Long, Boolean> insertJsonRecords(String json, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Map<Long, Boolean> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            List<DeferredWrite> deferred = Lists.newArrayList();
            for (long record : records) {
                result.put(record, Operations.insertAtomic(data, record, atomic,
                        deferred));
            }
            Operations.insertDeferredAtomic(deferred, atomic);
        });
        return result;
    }

    /**
     * Return an {@link Inspector} for this server.
     * 
     * @return an {@link Inspector}
     */
    public Inspector inspector() {
        return inspector;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<Long> inventory(AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getStore(transaction, environment).getAllRecords();
    }

    @Override
    @PluginRestricted
    public ComplexTObject invokeManagement(String method,
            List<ComplexTObject> params, AccessToken creds) throws TException {
        Object[] args = new Object[params.size() + 1];
        for (int i = 0; i < params.size(); ++i) {
            ComplexTObject arg = params.get(i);
            args[i] = arg.getJavaObject();
        }
        args[args.length - 1] = creds;
        try {
            Object result = Reflection.callIf(invoked -> Reflection
                    .isDeclaredAnnotationPresentInHierarchy(invoked,
                            ClientInvokable.class),
                    this, method, args);
            return ComplexTObject.fromJavaObject(result);
        }
        catch (IllegalStateException e) {
            throw new ManagementException(
                    "The requested method invocation is either invalid or not "
                            + "eligble for client-side invocation");
        }
        catch (Exception e) {
            throw new ManagementException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public ComplexTObject invokePlugin(String id, String method,
            List<ComplexTObject> params, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        environment = Environments.sanitize(environment); // CON-605, CON-606
        return pluginManager.invoke(id, method, params, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public String jsonifyRecords(List<Long> records, boolean identifier,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store,
                atomic -> Operations.jsonify(records, 0L, identifier, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public String jsonifyRecordsTime(List<Long> records, long timestamp,
            boolean identifier, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Operations.jsonify(records, timestamp, identifier,
                getStore(transaction, environment));
    }

    @Override
    @TranslateClientExceptions
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
        return users.tokens.issue(username);
    }

    @Override
    @TranslateClientExceptions
    @PluginRestricted
    public void logout(AccessToken creds) throws TException {
        logout(creds, null);
    }

    @Override
    @TranslateClientExceptions
    @PluginRestricted
    @VerifyAccessToken
    public void logout(AccessToken creds, String environment)
            throws TException {
        users.tokens.expire(creds);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Map<TObject, Set<Long>> data = atomic.browse(key);
            return Iterables.getLast(data.keySet(), null);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number max = Operations.maxKeyRecordsAtomic(key, records,
                        Time.NONE, atomic);
                return Convert.javaToThrift(max);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number max = Operations.maxKeyRecordsAtomic(key, records,
                        timestamp, atomic);
                return Convert.javaToThrift(max);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public TObject maxKeyCclTimestr(String key, String ccl, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return maxKeyCclTime(key, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number max = Operations.maxKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
            return Convert.javaToThrift(max);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number max = Operations.maxKeyRecordsAtomic(key, records, timestamp,
                    atomic);
            return Convert.javaToThrift(max);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject maxKeyCriteriaTimestr(String key, TCriteria criteria,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return maxKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number max = Operations.maxKeyRecordAtomic(key, record, Time.NONE,
                    atomic);
            return Convert.javaToThrift(max);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number max = Operations.maxKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
            return Convert.javaToThrift(max);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number max = Operations.maxKeyRecordsAtomic(key, records, timestamp,
                    atomic);
            return Convert.javaToThrift(max);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject maxKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return maxKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number max = Operations.maxKeyRecordAtomic(key, record, timestamp,
                    atomic);
            return Convert.javaToThrift(max);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject maxKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return maxKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject maxKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws SecurityException, TransactionException, TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Map<TObject, Set<Long>> data = atomic.browse(key, timestamp);
            return Iterables.getLast(data.keySet(), null);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject maxKeyTimestr(String key, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return maxKeyTime(key, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Map<TObject, Set<Long>> data = atomic.browse(key);
            return Iterables.getFirst(data.keySet(), null);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number min = Operations.minKeyRecordsAtomic(key, records,
                        Time.NONE, atomic);
                return Convert.javaToThrift(min);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number min = Operations.minKeyRecordsAtomic(key, records,
                        timestamp, atomic);
                return Convert.javaToThrift(min);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public TObject minKeyCclTimestr(String key, String ccl, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return minKeyCclTime(key, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number min = Operations.minKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
            return Convert.javaToThrift(min);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number min = Operations.minKeyRecordsAtomic(key, records, timestamp,
                    atomic);
            return Convert.javaToThrift(min);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject minKeyCriteriaTimestr(String key, TCriteria criteria,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return minKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number min = Operations.minKeyRecordAtomic(key, record, Time.NONE,
                    atomic);
            return Convert.javaToThrift(min);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number min = Operations.minKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
            return Convert.javaToThrift(min);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number min = Operations.minKeyRecordsAtomic(key, records, timestamp,
                    atomic);
            return Convert.javaToThrift(min);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject minKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return minKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number min = Operations.minKeyRecordAtomic(key, record, timestamp,
                    atomic);
            return Convert.javaToThrift(min);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject minKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return minKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);

    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject minKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Map<TObject, Set<Long>> data = atomic.browse(key, timestamp);
            return Iterables.getFirst(data.keySet(), null);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject minKeyTimestr(String key, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return minKeyTime(key, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return navigateKeyCclTime(key, ccl, Time.NONE, creds, transaction,
                environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                return Operations.navigateKeyRecordsAtomic(key, records,
                        timestamp, atomic);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return navigateKeyCclTime(key, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyCriteria(String key,
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return navigateKeyCriteriaTime(key, criteria, Time.NONE, creds,
                transaction, environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            return Operations.navigateKeyRecordsAtomic(key, records, timestamp,
                    atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return navigateKeyRecordTime(key, record, Time.NONE, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyRecords(String key,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return navigateKeyRecordsTime(key, records, Time.NONE, creds,
                transaction, environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyRecordsTime(String key,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            return Operations.navigateKeyRecordsAtomic(key,
                    Sets.newLinkedHashSet(records), timestamp, atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyRecordTime(String key,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            return Operations.navigateKeyRecordAtomic(key, record, timestamp,
                    atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Set<TObject>> navigateKeyRecordTimestr(String key,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysCcl(
            List<String> keys, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysCclTime(keys, ccl, Time.NONE, creds, transaction,
                environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysCclTime(
            List<String> keys, String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                return Operations.navigateKeysRecordsAtomic(keys, records,
                        timestamp, atomic);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysCclTimestr(
            List<String> keys, String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysCclTime(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysCriteria(
            List<String> keys, TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysCriteriaTime(keys, criteria, Time.NONE, creds,
                transaction, environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(criteria);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                return Operations.navigateKeysRecordsAtomic(keys, records,
                        timestamp, atomic);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysCriteriaTime(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysRecord(
            List<String> keys, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysRecordTime(keys, record, Time.NONE, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysRecords(
            List<String> keys, List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysRecordsTime(keys, records, Time.NONE, creds,
                transaction, environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            return Operations.navigateKeysRecordsAtomic(keys,
                    Sets.newLinkedHashSet(records), timestamp, atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws SecurityException, TransactionException, TException {
        return navigateKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @SuppressWarnings("deprecation")
    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysRecordTime(
            List<String> keys, long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .navigateKeysRecordAtomic(keys, record, timestamp, atomic));
    }

    @Override
    @TranslateClientExceptions
    @Deprecated
    public Map<Long, Map<String, Set<TObject>>> navigateKeysRecordTimestr(
            List<String> keys, long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return navigateKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    /**
     * Return a service {@link AccessToken token} that can be used to
     * authenticate and authorize non-user requests.
     * 
     * @return the {@link AccessToken service token}
     */
    @PluginRestricted
    public AccessToken newServiceToken() {
        return users.tokens.serviceIssue();
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public boolean pingRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Operations.ping(record, getStore(transaction, environment));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Boolean> pingRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Boolean> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                result.put(record, Operations.ping(record, atomic));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void reconcileKeyRecordValues(String key, long record,
            Set<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Set<TObject> existingValues = store.select(key, record);
            for (TObject existingValue : existingValues) {
                if(!values.remove(existingValue)) {
                    atomic.remove(key, existingValue, record);
                }
            }
            for (TObject value : values) {
                atomic.add(key, value, record);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public boolean removeKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return ((BufferedStore) getStore(transaction, environment)).remove(key,
                value, record);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<Long, Boolean> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                result.put(record, atomic.remove(key, value, record));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void revertKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                Operations.revertAtomic(key, record, timestamp, atomic);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    public void revertKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void revertKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Operations.revertAtomic(key, record, timestamp, atomic);
        });
    }

    @Override
    @TranslateClientExceptions
    public void revertKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeyRecordTime(key, record, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void revertKeysRecordsTime(List<String> keys, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                for (String key : keys) {
                    Operations.revertAtomic(key, record, timestamp, atomic);
                }
            }
        });
    }

    @Override
    @TranslateClientExceptions
    public void revertKeysRecordsTimestr(List<String> keys, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void revertKeysRecordTime(List<String> keys, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (String key : keys) {
                Operations.revertAtomic(key, record, timestamp, atomic);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    public void revertKeysRecordTimestr(List<String> keys, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<Long> search(String key, String query, AccessToken creds,
            TransactionToken transaction, String env) throws TException {
        return getStore(transaction, env).search(key, query);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCcl(String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectCclOrder(ccl, NO_ORDER, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCclOrder(String ccl,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectAstAtomic(ast, Time.NONE, result,
                            null,
                            $result -> $result.sort(Sorting
                                    .byValues(Orders.from(order), atomic)),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclOrderPage(String ccl,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectCclOrder(ccl, order, creds, transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCclPage(String ccl,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectAstAtomic(
                            ast, Time.NONE, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectCclTimeOrder(ccl, timestamp, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCclTimeOrder(String ccl,
            long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectAstAtomic(ast, timestamp, result,
                            null,
                            $result -> $result.sort(Sorting.byValues(
                                    Orders.from(order), atomic), timestamp),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTimeOrderPage(
            String ccl, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectCclTimeOrder(ccl, timestamp, order, creds, transaction,
                        environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCclTimePage(String ccl,
            long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectAstAtomic(
                            ast, timestamp, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTimestr(String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectCclTime(ccl, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTimestrOrder(
            String ccl, String timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectCclTimeOrder(ccl, NaturalLanguage.parseMicros(timestamp),
                order, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTimestrOrderPage(
            String ccl, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectCclTimeOrderPage(ccl,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCclTimestrPage(String ccl,
            String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectCclTimePage(ccl, NaturalLanguage.parseMicros(timestamp),
                page, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteria(
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectCriteriaOrder(criteria, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaOrder(
            TCriteria criteria, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectAstAtomic(ast, Time.NONE, result,
                        null,
                        $result -> $result.sort(
                                Sorting.byValues(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaOrderPage(
            TCriteria criteria, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectCriteriaOrder(criteria, order, creds, transaction,
                        environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaPage(
            TCriteria criteria, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectAstAtomic(ast, Time.NONE, result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTime(
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectCriteriaTimeOrder(criteria, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimeOrder(
            TCriteria criteria, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations
                .executeWithRetry(store,
                        atomic -> Operations.selectAstAtomic(ast, timestamp,
                                result, null,
                                $result -> $result.sort(Sorting.byValues(
                                        Orders.from(order), atomic), timestamp),
                                atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimeOrderPage(
            TCriteria criteria, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectCriteriaTimeOrder(criteria, timestamp, order, creds,
                        transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimePage(
            TCriteria criteria, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectAstAtomic(ast, timestamp, result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectCriteriaTime(criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestrOrder(
            TCriteria criteria, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectCriteriaTimeOrder(criteria,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestrOrderPage(
            TCriteria criteria, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectCriteriaTimeOrderPage(criteria,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestrPage(
            TCriteria criteria, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectCriteriaTimePage(criteria,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCclOrder(key, ccl, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCclOrder(String key, String ccl,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<Set<TObject>> result = SortableColumn
                    .multiValued(key, Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeyAstAtomic(key, ast, Time.NONE,
                            result, null,
                            $result -> $result.sort(Sorting
                                    .byValues(Orders.from(order), atomic)),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclOrderPage(String key, String ccl,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(selectKeyCclOrder(key, ccl, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCclPage(String key, String ccl,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<Set<TObject>> result = SortableColumn
                    .multiValued(key, Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeyAstAtomic(
                            key, ast, Time.NONE, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyCclTimeOrder(key, ccl, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCclTimeOrder(String key, String ccl,
            long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<Set<TObject>> result = SortableColumn
                    .multiValued(key, Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeyAstAtomic(key, ast, timestamp,
                            result, null,
                            $result -> $result.sort(Sorting.byValues(
                                    Orders.from(order), atomic), timestamp),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclTimeOrderPage(String key,
            String ccl, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(selectKeyCclTimeOrder(key, ccl, timestamp, order,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCclTimePage(String key, String ccl,
            long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableColumn<Set<TObject>> result = SortableColumn
                    .multiValued(key, Maps.newLinkedHashMap());
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeyAstAtomic(
                            key, ast, timestamp, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyCclTime(key, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclTimestrOrder(String key,
            String ccl, String timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCclTimeOrder(key, ccl,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclTimestrOrderPage(String key,
            String ccl, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCclTimeOrderPage(key, ccl,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCclTimestrPage(String key,
            String ccl, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCclTimePage(key, ccl,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteria(String key,
            TCriteria criteria, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyCriteriaOrder(key, criteria, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCriteriaOrder(String key,
            TCriteria criteria, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeyAstAtomic(key, ast, Time.NONE,
                        result, null,
                        $result -> $result.sort(
                                Sorting.byValues(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaOrderPage(String key,
            TCriteria criteria, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(selectKeyCriteriaOrder(key, criteria, order,
                creds, transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCriteriaPage(String key,
            TCriteria criteria, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeyAstAtomic(key, ast, Time.NONE,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCriteriaTimeOrder(key, criteria, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCriteriaTimeOrder(String key,
            TCriteria criteria, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations
                .executeWithRetry(store,
                        atomic -> Operations.selectKeyAstAtomic(key, ast,
                                timestamp, result, null,
                                $result -> $result.sort(Sorting.byValues(
                                        Orders.from(order), atomic), timestamp),
                                atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTimeOrderPage(String key,
            TCriteria criteria, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging
                .paginate(
                        selectKeyCriteriaTimeOrder(key, criteria, timestamp,
                                order, creds, transaction, environment),
                        Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyCriteriaTimePage(String key,
            TCriteria criteria, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                Maps.newLinkedHashMap());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeyAstAtomic(key, ast, timestamp,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestrOrder(String key,
            TCriteria criteria, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCriteriaTimeOrder(key, criteria,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestrOrderPage(String key,
            TCriteria criteria, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCriteriaTimeOrderPage(key, criteria,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestrPage(String key,
            TCriteria criteria, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCriteriaTimePage(key, criteria,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<TObject> selectKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Function<Store, Set<TObject>> function = $store -> Stores.select($store,
                key, record);
        try {
            return function.apply(store);
        }
        catch (InsufficientAtomicityException e) {
            return AtomicOperations.supplyWithRetry(store,
                    atomic -> function.apply(atomic));
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecords(String key,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyRecordsOrder(key, records, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyRecordsOrder(String key,
            List<Long> records, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeyRecordsAtomic(key, records,
                        result, null,
                        $result -> $result.sort(
                                Sorting.byValues(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsOrderPage(String key,
            List<Long> records, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(selectKeyRecordsOrder(key, records, order, creds,
                transaction, environment), Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyRecordsPage(String key,
            List<Long> records, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeyRecordsAtomic(key, records,
                        result,
                        $records -> Paging.paginate($records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTime(String key,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordsTimeOrder(key, records, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyRecordsTimeOrder(String key,
            List<Long> records, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        Operations.selectKeyRecordsOptionalAtomic(key, records, timestamp,
                result, null,
                $result -> $result.sort(
                        Sorting.byValues(Orders.from(order), store), timestamp),
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTimeOrderPage(String key,
            List<Long> records, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging
                .paginate(
                        selectKeyRecordsTimeOrder(key, records, timestamp,
                                order, creds, transaction, environment),
                        Pages.from(page));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Set<TObject>> selectKeyRecordsTimePage(String key,
            List<Long> records, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableColumn<Set<TObject>> result = SortableColumn.multiValued(key,
                TMaps.newLinkedHashMapWithCapacity(records.size()));
        Operations.selectKeyRecordsOptionalAtomic(key, records, timestamp,
                result, $records -> Paging.paginate($records, Pages.from(page)),
                null, store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTimestrOrder(String key,
            List<Long> records, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordsTimeOrder(key, records,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTimestrOrderPage(String key,
            List<Long> records, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordsTimeOrderPage(key, records,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Set<TObject>> selectKeyRecordsTimestrPage(String key,
            List<Long> records, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordsTimePage(key, records,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Set<TObject> selectKeyRecordTime(String key, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Stores.select(getStore(transaction, environment), key, record,
                timestamp);
    }

    @Override
    @TranslateClientExceptions
    public Set<TObject> selectKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCcl(List<String> keys,
            String ccl, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeysCclOrder(keys, ccl, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclOrder(
            List<String> keys, String ccl, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeysAstAtomic(keys, ast,
                            Time.NONE, result, null,
                            $result -> $result.sort(Sorting
                                    .byValues(Orders.from(order), atomic)),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclOrderPage(
            List<String> keys, String ccl, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectKeysCclOrder(keys, ccl, order, creds, transaction,
                        environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclPage(
            List<String> keys, String ccl, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeysAstAtomic(keys, ast,
                            Time.NONE, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTime(
            List<String> keys, String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCclTimeOrder(keys, ccl, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimeOrder(
            List<String> keys, String ccl, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeysAstAtomic(keys, ast,
                            timestamp, result, null,
                            $result -> $result.sort(Sorting.byValues(
                                    Orders.from(order), atomic), timestamp),
                            atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimeOrderPage(
            List<String> keys, String ccl, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(
                selectKeysCclTimeOrder(keys, ccl, timestamp, order, creds,
                        transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimePage(
            List<String> keys, String ccl, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            SortableTable<Set<TObject>> result = emptySortableResultDataset();
            AtomicOperations.executeWithRetry(store,
                    atomic -> Operations.selectKeysAstAtomic(keys, ast,
                            timestamp, result, records -> Paging
                                    .paginate(records, Pages.from(page)),
                            null, atomic));
            return result;
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestr(
            List<String> keys, String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCclTime(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestrOrder(
            List<String> keys, String ccl, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCclTimeOrder(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestrOrderPage(
            List<String> keys, String ccl, String timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeysCclTimeOrderPage(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestrPage(
            List<String> keys, String ccl, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCclTimePage(keys, ccl,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteria(
            List<String> keys, TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCriteriaOrder(keys, criteria, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaOrder(
            List<String> keys, TCriteria criteria, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeysAstAtomic(keys, ast, Time.NONE,
                        result, null,
                        $result -> $result.sort(
                                Sorting.byValues(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaOrderPage(
            List<String> keys, TCriteria criteria, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectKeysCriteriaOrder(keys, criteria, order, creds,
                        transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaPage(
            List<String> keys, TCriteria criteria, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeysAstAtomic(keys, ast, Time.NONE,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCriteriaTimeOrder(keys, criteria, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimeOrder(
            List<String> keys, TCriteria criteria, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations
                .executeWithRetry(store,
                        atomic -> Operations.selectKeysAstAtomic(keys, ast,
                                timestamp, result, null,
                                $result -> $result.sort(Sorting.byValues(
                                        Orders.from(order), atomic), timestamp),
                                atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimeOrderPage(
            List<String> keys, TCriteria criteria, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(
                selectKeysCriteriaTimeOrder(keys, criteria, timestamp, order,
                        creds, transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimePage(
            List<String> keys, TCriteria criteria, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDataset();
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeysAstAtomic(keys, ast, timestamp,
                        result,
                        records -> Paging.paginate(records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCriteriaTime(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestrOrder(
            List<String> keys, TCriteria criteria, String timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeysCriteriaTimeOrder(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestrOrderPage(
            List<String> keys, TCriteria criteria, String timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCriteriaTimeOrderPage(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestrPage(
            List<String> keys, TCriteria criteria, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysCriteriaTimePage(keys, criteria,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Set<TObject>> selectKeysRecord(List<String> keys,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Set<TObject>> result = Maps.newLinkedHashMap();
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (String key : keys) {
                result.put(key, Stores.select(atomic, key, record));
            }
        });
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecords(
            List<String> keys, List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsOrder(keys, records, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsOrder(
            List<String> keys, List<Long> records, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeysRecordsAtomic(keys, records,
                        result, null,
                        $result -> $result.sort(
                                Sorting.byValues(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsOrderPage(
            List<String> keys, List<Long> records, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectKeysRecordsOrder(keys, records, order, creds, transaction,
                        environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsPage(
            List<String> keys, List<Long> records, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectKeysRecordsAtomic(keys, records,
                        result,
                        $records -> Paging.paginate($records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsTimeOrder(keys, records, timestamp, NO_ORDER,
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimeOrder(
            List<String> keys, List<Long> records, long timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        Operations.selectKeysRecordsOptionalAtomic(keys, records, timestamp,
                result, null,
                $result -> $result.sort(
                        Sorting.byValues(Orders.from(order), store), timestamp),
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimeOrderPage(
            List<String> keys, List<Long> records, long timestamp, TOrder order,
            TPage page, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return Paging.paginate(
                selectKeysRecordsTimeOrder(keys, records, timestamp, order,
                        creds, transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimePage(
            List<String> keys, List<Long> records, long timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        Operations.selectKeysRecordsOptionalAtomic(keys, records, timestamp,
                result, $records -> Paging.paginate($records, Pages.from(page)),
                null, store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsTime(keys, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestrOrder(
            List<String> keys, List<Long> records, String timestamp,
            TOrder order, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeysRecordsTimeOrder(keys, records,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestrOrderPage(
            List<String> keys, List<Long> records, String timestamp,
            TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsTimeOrderPage(keys, records,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestrPage(
            List<String> keys, List<Long> records, String timestamp, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsTimePage(keys, records,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Set<TObject>> selectKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        Map<String, Set<TObject>> result = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        for (String key : keys) {
            result.put(key, Stores.select(store, key, record, timestamp));
        }
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<String, Set<TObject>> selectKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordTime(keys, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Set<TObject>> selectRecord(long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).select(record);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecords(
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectRecordsOrder(records, NO_ORDER, creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectRecordsOrder(
            List<Long> records, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectRecordsAtomic(records, result, null,
                        $result -> $result.sort(
                                Sorting.byValues(Orders.from(order), atomic)),
                        atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsOrderPage(
            List<Long> records, TOrder order, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectRecordsOrder(records, order, creds, transaction,
                        environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectRecordsPage(
            List<Long> records, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        AtomicOperations.executeWithRetry(store,
                atomic -> Operations.selectRecordsAtomic(records, result,
                        $records -> Paging.paginate($records, Pages.from(page)),
                        null, atomic));
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTime(
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectRecordsTimeOrder(records, timestamp, NO_ORDER, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimeOrder(
            List<Long> records, long timestamp, TOrder order, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        Operations.selectRecordsOptionalAtomic(records, timestamp, result, null,
                $result -> $result.sort(
                        Sorting.byValues(Orders.from(order), store), timestamp),
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimeOrderPage(
            List<Long> records, long timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return Paging.paginate(
                selectRecordsTimeOrder(records, timestamp, order, creds,
                        transaction, environment),
                Pages.from(page), () -> emptySortableResultDataset(),
                (map, entity) -> TMaps.putResultDatasetOptimized(map,
                        entity.getKey(), entity.getValue()));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimePage(
            List<Long> records, long timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        SortableTable<Set<TObject>> result = emptySortableResultDatasetWithCapacity(
                records.size());
        Operations.selectRecordsOptionalAtomic(records, timestamp, result,
                $records -> Paging.paginate($records, Pages.from(page)), null,
                store);
        return result;
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestr(
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectRecordsTime(records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestrOrder(
            List<Long> records, String timestamp, TOrder order,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectRecordsTimeOrder(records,
                NaturalLanguage.parseMicros(timestamp), order, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestrOrderPage(
            List<Long> records, String timestamp, TOrder order, TPage page,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectRecordsTimeOrderPage(records,
                NaturalLanguage.parseMicros(timestamp), order, page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestrPage(
            List<Long> records, String timestamp, TPage page, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return selectRecordsTimePage(records,
                NaturalLanguage.parseMicros(timestamp), page, creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Set<TObject>> selectRecordTime(long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getStore(transaction, environment).select(record, timestamp);
    }

    @Override
    @TranslateClientExceptions
    public Map<String, Set<TObject>> selectRecordTimestr(long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectRecordTime(record, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    public long setKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return addKeyValue(key, value, creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void setKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        ((BufferedStore) getStore(transaction, environment)).set(key, value,
                record);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void setKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            for (long record : records) {
                atomic.set(key, value, record);
            }
        });
    }

    @Override
    @TranslateClientExceptions
    @PluginRestricted
    @VerifyAccessToken
    public TransactionToken stage(AccessToken creds, String env)
            throws TException {
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
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number sum = Operations.sumKeyAtomic(key, Time.NONE, atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyCcl(String key, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number sum = Operations.sumKeyRecordsAtomic(key, records,
                        Time.NONE, atomic);
                return Convert.javaToThrift(sum);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyCclTime(String key, String ccl, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        try {
            AbstractSyntaxTree ast = compiler.parse(ccl);
            AtomicSupport store = getStore(transaction, environment);
            return AtomicOperations.supplyWithRetry(store, (atomic) -> {
                Set<Long> records = ast.accept(Finder.instance(), atomic);
                Number sum = Operations.sumKeyRecordsAtomic(key, records,
                        timestamp, atomic);
                return Convert.javaToThrift(sum);
            });
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    public TObject sumKeyCclTimestr(String key, String ccl, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return sumKeyCclTime(key, ccl, NaturalLanguage.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number sum = Operations.sumKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyCriteriaTime(String key, TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AbstractSyntaxTree ast = compiler.parse(criteria);
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Set<Long> records = ast.accept(Finder.instance(), atomic);
            Number sum = Operations.sumKeyRecordsAtomic(key, records, timestamp,
                    atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject sumKeyCriteriaTimestr(String key, TCriteria criteria,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return sumKeyCriteriaTime(key, criteria,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number sum = Operations.sumKeyRecordAtomic(key, record, Time.NONE,
                    atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number sum = Operations.sumKeyRecordsAtomic(key, records, Time.NONE,
                    atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number sum = Operations.sumKeyRecordsAtomic(key, records, timestamp,
                    atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject sumKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return sumKeyRecordsTime(key, records,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number sum = Operations.sumKeyRecordAtomic(key, record, timestamp,
                    atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject sumKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return sumKeyRecordTime(key, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public TObject sumKeyTime(String key, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws SecurityException, TransactionException, TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, (atomic) -> {
            Number sum = Operations.sumKeyAtomic(key, timestamp, atomic);
            return Convert.javaToThrift(sum);
        });
    }

    @Override
    @TranslateClientExceptions
    public TObject sumKeyTimestr(String key, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return sumKeyTime(key, NaturalLanguage.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long time(AccessToken creds, TransactionToken token,
            String environment) throws TException {
        return Time.now();
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public long timePhrase(String phrase, AccessToken creds,
            TransactionToken token, String environment) throws TException {
        try {
            return NaturalLanguage.parseMicros(phrase);
        }
        catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Set<Long>> traceRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .traceRecordAtomic(record, Time.NONE, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<Long>>> traceRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .traceRecordsAtomic(records, Time.NONE, atomic));

    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<Long, Map<String, Set<Long>>> traceRecordsTime(
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .traceRecordsAtomic(records, timestamp, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public Map<String, Set<Long>> traceRecordTime(long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        AtomicSupport store = getStore(transaction, environment);
        return AtomicOperations.supplyWithRetry(store, atomic -> Operations
                .traceRecordAtomic(record, timestamp, atomic));
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
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
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public boolean verifyKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).verify(key, value, record);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyReadPermission
    public boolean verifyKeyValueRecordTime(String key, TObject value,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return getStore(transaction, environment).verify(key, value, record,
                timestamp);
    }

    @Override
    @TranslateClientExceptions
    public boolean verifyKeyValueRecordTimestr(String key, TObject value,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TException {
        return verifyKeyValueRecordTime(key, value, record,
                NaturalLanguage.parseMicros(timestamp), creds, transaction,
                environment);
    }

    @Override
    @TranslateClientExceptions
    @VerifyAccessToken
    @VerifyWritePermission
    public void verifyOrSet(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String env)
            throws TException {
        AtomicSupport store = getStore(transaction, env);
        AtomicOperations.executeWithRetry(store, (atomic) -> {
            Set<TObject> values = atomic.select(key, record);
            for (TObject val : values) {
                if(!val.equals(value)) {
                    atomic.remove(key, val, record);
                }
            }
            if(!atomic.verify(key, value, record)) {
                atomic.add(key, value, record);
            }
        });
    }

    @Override
    @Internal
    protected String getBufferStore() {
        return bufferStore;
    }

    @Override
    @Internal
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
    @Internal
    protected Engine getEngine(String env) {
        Engine engine = engines.get(env);
        if(engine == null) {
            env = Environments.sanitize(env);
            return getEngineUnsafe(env);
        }
        return engine;
    }

    @Override
    @Internal
    protected PluginManager plugins() {
        return pluginManager;
    }

    @Override
    @Internal
    protected UserService users() {
        return users;
    }

    /**
     * {@link #start() Start} the server as a daemon.
     */
    @Internal
    void spawn() {
        new Thread(() -> {
            try {
                start();
            }
            catch (TTransportException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }).start();
    }

    /**
     * Return the {@link Engine} that is associated with the
     * {@link Default#ENVIRONMENT}.
     *
     * @return the Engine
     */
    @Internal
    private Engine getEngine() {
        return getEngine(DEFAULT_ENVIRONMENT);
    }

    /**
     * Return the {@link Engine} that is associated with {@code env} without
     * performing any sanitization on the name. If such an Engine does not
     * exist, create a new one and add it to the collection.
     */
    @Internal
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
    @Internal
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
    @Internal
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
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        TProcessor core = new ConcourseService.Processor<>(this);
        processor.registerProcessor("core", core);
        processor.registerProcessor("calculate",
                new ConcourseCalculateService.Processor<>(this));
        processor.registerProcessor("navigate",
                new ConcourseNavigateService.Processor<>(this));
        processor.registerDefault(core);
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
        this.users = UserService.create(ACCESS_FILE);
        this.inspector = new Inspector() {

            @Override
            public Role getTokenUserRole(AccessToken token) {
                ByteBuffer username = users.tokens.identify(token);
                return users.getRole(username);
            }

            @Override
            public boolean isValidToken(AccessToken token) {
                return users.tokens.isValid(token);
            }

            @Override
            public boolean isValidTransaction(TransactionToken transaction) {
                return transactions.containsKey(transaction);
            }

            @Override
            public boolean tokenUserHasPermission(AccessToken token,
                    Permission permission, String environment) {
                ByteBuffer username = users.tokens.identify(token);
                return users.can(username, permission,
                        Environments.sanitize(environment));
            }

        };
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
    @Internal
    private void validate(ByteBuffer username, ByteBuffer password)
            throws SecurityException {
        if(!users.authenticate(username, password)) {
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

}
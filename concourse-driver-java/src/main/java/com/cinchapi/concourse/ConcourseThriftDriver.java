/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.config.ConcourseClientPreferences;
import com.cinchapi.concourse.data.transform.DataColumn;
import com.cinchapi.concourse.data.transform.DataIndex;
import com.cinchapi.concourse.data.transform.DataProjection;
import com.cinchapi.concourse.data.transform.DataRow;
import com.cinchapi.concourse.data.transform.DataTable;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.security.ClientSecurity;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.ConcourseCalculateService;
import com.cinchapi.concourse.thrift.ConcourseNavigateService;
import com.cinchapi.concourse.thrift.ConcourseService;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.JavaThriftBridge;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Collections;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Navigation;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * An implementation of the {@link Concourse} interface that interacts with the
 * server via Thrift's RPC protocols.
 * 
 * @author Jeff Nelson
 */
class ConcourseThriftDriver extends Concourse {

    private static String ENVIRONMENT;
    private static String PASSWORD;
    private static String SERVER_HOST;
    private static int SERVER_PORT;
    private static String USERNAME;
    static {
        // If there is a concourse_client.prefs file located in the working
        // directory, parse it and use its values as defaults.
        ConcourseClientPreferences config = ConcourseClientPreferences
                .fromCurrentWorkingDirectory();
        SERVER_HOST = config.getHost();
        SERVER_PORT = config.getPort();
        USERNAME = config.getUsername();
        PASSWORD = new String(config.getPassword());
        ENVIRONMENT = config.getEnvironment();

    }

    /**
     * The Thrift client that actually handles core RPC communication.
     */
    private final ConcourseService.Client core;

    /**
     * The Thrift client that actually handles navigate RPC communication.
     */
    private final ConcourseNavigateService.Client navigate;

    /**
     * The thrift client that actually handles calcuation RPC communication.
     */
    private final ConcourseCalculateService.Client calculate;

    /**
     * A container with all the thrift clients.
     */
    private final Set<TServiceClient> clients;

    /**
     * The client keeps a copy of its {@link AccessToken} and passes it to
     * the server for each remote procedure call. The client will
     * re-authenticate when necessary using the username/password read from
     * the prefs file.
     */
    private AccessToken creds = null;

    /**
     * The environment to which the client is connected.
     */
    private final String environment;

    /**
     * The host of the connection.
     */
    private final String host;

    /**
     * An encrypted copy of the password passed to the constructor.
     */
    private final ByteBuffer password;

    /**
     * The port of the connection.
     */
    private final int port;

    /**
     * Whenever the client starts a Transaction, it keeps a
     * {@link TransactionToken} so that the server can stage the changes in
     * the appropriate place.
     */
    private TransactionToken transaction = null;

    /**
     * An encrypted copy of the username passed to the constructor.
     */
    private final ByteBuffer username;

    /**
     * Create a new Client connection to the environment of the Concourse
     * Server described in {@code concourse_client.prefs} (or the default
     * environment and server if the prefs file does not exist) and return a
     * handler to facilitate database interaction.
     */
    public ConcourseThriftDriver() {
        this(ENVIRONMENT);
    }

    /**
     * Create a new Client connection to the specified {@code environment}
     * of the Concourse Server described in {@code concourse_client.prefs}
     * (or the default server if the prefs file does not exist) and return a
     * handler to facilitate database interaction.
     * 
     * @param environment
     */
    public ConcourseThriftDriver(String environment) {
        this(SERVER_HOST, SERVER_PORT, USERNAME, PASSWORD, environment);
    }

    /**
     * Create a new Client connection to the default environment of the
     * specified Concourse Server and return a handler to facilitate
     * database interaction.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     */
    public ConcourseThriftDriver(String host, int port, String username,
            String password) {
        this(host, port, username, password, "");
    }

    /**
     * Create a new Client connection to the specified {@code environment}
     * of the specified Concourse Server and return a handler to facilitate
     * database interaction.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     */
    public ConcourseThriftDriver(String host, int port, String username,
            String password, String environment) {
        this.host = host;
        this.port = port;
        this.username = ClientSecurity.encrypt(username);
        this.password = ClientSecurity.encrypt(password);
        this.environment = environment;
        final TTransport transport = new TSocket(host, port);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            core = new ConcourseService.Client(
                    new TMultiplexedProtocol(protocol, "core"));
            calculate = new ConcourseCalculateService.Client(
                    new TMultiplexedProtocol(protocol, "calculate"));
            navigate = new ConcourseNavigateService.Client(
                    new TMultiplexedProtocol(protocol, "navigate"));
            clients = ImmutableSet.of(core, calculate, navigate);
            authenticate();
            Runtime.getRuntime().addShutdownHook(new Thread("shutdown") {

                @Override
                public void run() {
                    if(transaction != null && transport.isOpen()) {
                        abort();
                        transport.close();
                    }
                }

            });
        }
        catch (TTransportException e) {
            throw new RuntimeException(
                    "Could not connect to the Concourse Server at " + host + ":"
                            + port);
        }
    }

    @Override
    public void abort() {
        execute(() -> {
            if(transaction != null) {
                final TransactionToken token = transaction;
                transaction = null;
                core.abort(creds, token, environment);
            }
            return null;
        });
    }

    @Override
    public <T> long add(String key, T value) {
        return execute(() -> {
            return core.addKeyValue(key, Convert.javaToThrift(value), creds,
                    transaction, environment);
        });
    }

    @Override
    public <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Boolean> raw = core.addKeyValueRecords(key,
                    Convert.javaToThrift(value),
                    Collections.toLongList(records), creds, transaction,
                    environment);
            Map<Long, Boolean> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", "Successful");
            for (long record : records) {
                pretty.put(record, raw.get(record));
            }
            return pretty;
        });
    }

    @Override
    public <T> boolean add(String key, T value, long record) {
        return execute(() -> {
            return core.addKeyValueRecord(key, Convert.javaToThrift(value),
                    record, creds, transaction, environment);
        });
    }

    @Override
    public Map<Timestamp, String> audit(long record) {
        return execute(() -> {
            Map<Long, String> audit = core.auditRecord(record, creds,
                    transaction, environment);
            return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                    .transformMap(audit, Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
        });
    }

    @Override
    public Map<Timestamp, String> audit(long record, Timestamp start) {
        return execute(() -> {
            Map<Long, String> audit;
            if(start.isString()) {
                audit = core.auditRecordStartstr(record, start.toString(),
                        creds, transaction, environment);
            }
            else {
                audit = core.auditRecordStart(record, start.getMicros(), creds,
                        transaction, environment);
            }
            return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                    .transformMap(audit, Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
        });
    }

    @Override
    public Map<Timestamp, String> audit(long record, Timestamp start,
            Timestamp end) {
        return execute(() -> {
            Map<Long, String> audit;
            if(start.isString()) {
                audit = core.auditRecordStartstrEndstr(record, start.toString(),
                        end.toString(), creds, transaction, environment);
            }
            else {
                audit = core.auditRecordStartEnd(record, start.getMicros(),
                        end.getMicros(), creds, transaction, environment);
            }
            return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                    .transformMap(audit, Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
        });
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record) {
        return execute(() -> {
            Map<Long, String> audit = core.auditKeyRecord(key, record, creds,
                    transaction, environment);
            return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                    .transformMap(audit, Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
        });
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record,
            Timestamp start) {
        return execute(() -> {
            Map<Long, String> audit;
            if(start.isString()) {
                audit = core.auditKeyRecordStartstr(key, record,
                        start.toString(), creds, transaction, environment);
            }
            else {
                audit = core.auditKeyRecordStart(key, record, start.getMicros(),
                        creds, transaction, environment);
            }
            return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                    .transformMap(audit, Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
        });
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record,
            Timestamp start, Timestamp end) {
        return execute(() -> {
            Map<Long, String> audit;
            if(start.isString()) {
                audit = core.auditKeyRecordStartstrEndstr(key, record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                audit = core.auditKeyRecordStartEnd(key, record,
                        start.getMicros(), end.getMicros(), creds, transaction,
                        environment);
            }
            return ((PrettyLinkedHashMap<Timestamp, String>) Transformers
                    .transformMap(audit, Conversions.timestampToMicros()))
                            .setKeyName("DateTime").setValueName("Revision");
        });
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys) {
        return execute(() -> {
            Map<String, Map<TObject, Set<Long>>> data = core.browseKeys(
                    Collections.toList(keys), creds, transaction, environment);
            return DataIndex.of(data);
        });
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys,
            Timestamp timestamp) {
        return execute(() -> {
            Map<String, Map<TObject, Set<Long>>> data;
            if(timestamp.isString()) {
                data = core.browseKeysTimestr(Collections.toList(keys),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.browseKeysTime(Collections.toList(keys),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataIndex.of(data);
        });
    }

    @Override
    public Map<Object, Set<Long>> browse(String key) {
        return execute(() -> {
            Map<TObject, Set<Long>> data = core.browseKey(key, creds,
                    transaction, environment);
            return DataProjection.of(data);
        });
    }

    @Override
    public Map<Object, Set<Long>> browse(String key, Timestamp timestamp) {
        return execute(() -> {
            Map<TObject, Set<Long>> data;
            if(timestamp.isString()) {
                data = core.browseKeyTimestr(key, timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                data = core.browseKeyTime(key, timestamp.getMicros(), creds,
                        transaction, environment);
            }
            return DataProjection.of(data);
        });
    }

    @Override
    public Map<Timestamp, Set<Object>> chronologize(String key, long record) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw = core.chronologizeKeyRecord(key,
                    record, creds, transaction, environment);
            Map<Timestamp, Set<Object>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("DateTime", "Values");
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(Timestamp.fromMicros(entry.getKey()),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public Map<Timestamp, Set<Object>> chronologize(String key, long record,
            Timestamp start) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw;
            if(start.isString()) {
                raw = core.chronologizeKeyRecordStartstr(key, record,
                        start.toString(), creds, transaction, environment);
            }
            else {
                raw = core.chronologizeKeyRecordStart(key, record,
                        start.getMicros(), creds, transaction, environment);
            }
            Map<Timestamp, Set<Object>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("DateTime", "Values");
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(Timestamp.fromMicros(entry.getKey()),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public Map<Timestamp, Set<Object>> chronologize(String key, long record,
            Timestamp start, Timestamp end) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw;
            if(start.isString()) {
                raw = core.chronologizeKeyRecordStartstrEndstr(key, record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                raw = core.chronologizeKeyRecordStartEnd(key, record,
                        start.getMicros(), end.getMicros(), creds, transaction,
                        environment);
            }
            Map<Timestamp, Set<Object>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("DateTime", "Values");
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(Timestamp.fromMicros(entry.getKey()),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public void clear(Collection<Long> records) {
        execute(() -> {
            core.clearRecords(Collections.toLongList(records), creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(Collection<String> keys, Collection<Long> records) {
        execute(() -> {
            core.clearKeysRecords(Collections.toList(keys),
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return null;
        });
    }

    @Override
    public void clear(Collection<String> keys, long record) {
        execute(() -> {
            core.clearKeysRecord(Collections.toList(keys), record, creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(long record) {
        execute(() -> {
            core.clearRecord(record, creds, transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(String key, Collection<Long> records) {
        execute(() -> {
            core.clearKeyRecords(key, Collections.toLongList(records), creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(String key, long record) {
        execute(() -> {
            core.clearKeyRecord(key, record, creds, transaction, environment);
            return null;
        });
    }

    @Override
    public boolean commit() {
        return execute(() -> {
            final TransactionToken token = transaction;
            transaction = null;
            return token != null ? core.commit(creds, token, environment)
                    : false;
        });
    }

    @Override
    public Set<String> describe() {
        return execute(() -> {
            return core.describe(creds, transaction, environment);
        });
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records) {
        return execute(() -> {
            Map<Long, Set<String>> raw = core.describeRecords(
                    Collections.toLongList(records), creds, transaction,
                    environment);
            Map<Long, Set<String>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", "Keys");
            for (Entry<Long, Set<String>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(), entry.getValue());
            }
            return pretty;
        });
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<String>> raw;
            if(timestamp.isString()) {
                raw = core.describeRecordsTimestr(
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = core.describeRecordsTime(Collections.toLongList(records),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Set<String>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", "Keys");
            for (Entry<Long, Set<String>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(), entry.getValue());
            }
            return pretty;
        });
    }

    @Override
    public Set<String> describe(long record) {
        return execute(() -> {
            Set<String> result = core.describeRecord(record, creds, transaction,
                    environment);
            return result;
        });
    }

    @Override
    public Set<String> describe(long record, Timestamp timestamp) {
        return execute(() -> {
            if(timestamp.isString()) {
                return core.describeRecordTimestr(record, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                return core.describeRecordTime(record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
        });
    }

    @Override
    public Set<String> describe(Timestamp timestamp) {
        return execute(() -> {
            if(timestamp.isString()) {
                return core.describeTimestr(timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                return core.describeTime(timestamp.getMicros(), creds,
                        transaction, environment);
            }
        });
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start) {
        return execute(() -> {
            Map<String, Map<Diff, Set<TObject>>> raw;
            if(start.isString()) {
                raw = core.diffRecordStartstr(record, start.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = core.diffRecordStart(record, start.getMicros(), creds,
                        transaction, environment);
            }
            PrettyLinkedTableMap<String, Diff, Set<T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap();
            pretty.setRowName("Key");
            for (Entry<String, Map<Diff, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<Diff> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record, Timestamp start,
            Timestamp end) {
        return execute(() -> {
            Map<String, Map<Diff, Set<TObject>>> raw;
            if(start.isString()) {
                raw = core.diffRecordStartstrEndstr(record, start.toString(),
                        end.toString(), creds, transaction, environment);
            }
            else {
                raw = core.diffRecordStartEnd(record, start.getMicros(),
                        end.getMicros(), creds, transaction, environment);
            }
            PrettyLinkedTableMap<String, Diff, Set<T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap();
            pretty.setRowName("Key");
            for (Entry<String, Map<Diff, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<Diff> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start) {
        return execute(() -> {
            Map<Diff, Set<TObject>> raw;
            if(start.isString()) {
                raw = core.diffKeyRecordStartstr(key, record, start.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = core.diffKeyRecordStart(key, record, start.getMicros(),
                        creds, transaction, environment);
            }
            Map<Diff, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Operation", "Value");
            for (Entry<Diff, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Diff, Set<T>> diff(String key, long record, Timestamp start,
            Timestamp end) {
        return execute(() -> {
            Map<Diff, Set<TObject>> raw;
            if(start.isString()) {
                raw = core.diffKeyRecordStartstrEndstr(key, record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                raw = core.diffKeyRecordStartEnd(key, record, start.getMicros(),
                        end.getMicros(), creds, transaction, environment);
            }
            Map<Diff, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Operation", "Value");
            for (Entry<Diff, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start) {
        return execute(() -> {
            Map<TObject, Map<Diff, Set<Long>>> raw;
            if(start.isString()) {
                raw = core.diffKeyStartstr(key, start.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = core.diffKeyStart(key, start.getMicros(), creds,
                        transaction, environment);
            }
            PrettyLinkedTableMap<T, Diff, Set<Long>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap();
            pretty.setRowName("Value");
            for (Entry<TObject, Map<Diff, Set<Long>>> entry : raw.entrySet()) {
                pretty.put((T) Convert.thriftToJava(entry.getKey()),
                        entry.getValue());
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start,
            Timestamp end) {
        return execute(() -> {
            Map<TObject, Map<Diff, Set<Long>>> raw;
            if(start.isString()) {
                raw = core.diffKeyStartstrEndstr(key, start.toString(),
                        end.toString(), creds, transaction, environment);
            }
            else {
                raw = core.diffKeyStartEnd(key, start.getMicros(),
                        end.getMicros(), creds, transaction, environment);
            }
            PrettyLinkedTableMap<T, Diff, Set<Long>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap();
            pretty.setRowName("Value");
            for (Entry<TObject, Map<Diff, Set<Long>>> entry : raw.entrySet()) {
                pretty.put((T) Convert.thriftToJava(entry.getKey()),
                        entry.getValue());
            }
            return pretty;
        });
    }

    @Override
    public void exit() {
        try {
            core.logout(creds, environment);
            for (TServiceClient client : clients) {
                client.getInputProtocol().getTransport().close();
                client.getOutputProtocol().getTransport().close();
            }
        }
        catch (com.cinchapi.concourse.thrift.SecurityException
                | TTransportException e) {
            // Handle corner case where the client is existing because of
            // (or after the occurrence of) a password change, which means
            // it can't perform a traditional logout. Its worth nothing that
            // we're okay with this scenario because a password change will
            // delete all previously issued tokens.
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    @Override
    public Set<Long> find(Criteria criteria) {
        return execute(() -> {
            return core.findCriteria(
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
        });
    }

    @Override
    public Set<Long> find(Criteria criteria, Order order) {
        return execute(() -> {
            return core.findCriteriaOrder(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
        });
    }

    @Override
    public Set<Long> find(Criteria criteria, Order order, Page page) {
        return execute(() -> {
            return core.findCriteriaOrderPage(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
        });
    }

    @Override
    public Set<Long> find(Criteria criteria, Page page) {
        return execute(() -> {
            return core.findCriteriaPage(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
        });
    }

    @Override
    public Set<Long> find(String ccl) {
        return execute(() -> {
            return core.findCcl(ccl, creds, transaction, environment);
        });
    }

    @Override
    public Set<Long> find(String key, Object value) {
        return executeFind(key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Order order) {
        return executeFind(order, key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Order order, Page page) {
        return executeFind(order, page, key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Page page) {
        return executeFind(page, key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp) {
        return executeFind(key, Operator.EQUALS, value, timestamp);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Order order) {
        return executeFind(timestamp, order, key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Order order, Page page) {
        return executeFind(timestamp, order, page, key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Page page) {
        return executeFind(timestamp, key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value) {
        return executeFind(key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2) {
        return executeFind(key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Order order) {
        return executeFind(order, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Order order, Page page) {
        return executeFind(order, page, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Page page) {
        return executeFind(page, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Order order) {
        return executeFind(timestamp, order, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Order order, Page page) {
        return executeFind(timestamp, order, page, key, operator, value,
                value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Page page) {
        return executeFind(timestamp, page, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Order order) {
        return executeFind(order, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Order order, Page page) {
        return executeFind(order, page, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Page page) {
        return executeFind(page, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Order order) {
        return executeFind(timestamp, order, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Order order, Page page) {
        return executeFind(timestamp, order, page, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Page page) {
        return executeFind(timestamp, page, key, operator, value);
    }

    @Override
    public Set<Long> find(String ccl, Order order) {
        return execute(() -> {
            return core.findCclOrder(ccl, JavaThriftBridge.convert(order),
                    creds, transaction, environment);
        });
    }

    @Override
    public Set<Long> find(String ccl, Order order, Page page) {
        return execute(() -> {
            return core.findCclOrderPage(ccl, JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
        });
    }

    @Override
    public Set<Long> find(String ccl, Page page) {
        return execute(() -> {
            return core.findCclPage(ccl, JavaThriftBridge.convert(page), creds,
                    transaction, environment);
        });
    }

    @Override
    public Set<Long> find(String key, String operator, Object value) {
        return executeFind(key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2) {
        return executeFind(key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Order order) {
        return executeFind(order, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Order order, Page page) {
        return executeFind(order, page, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Page page) {
        return executeFind(page, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Order order) {
        return executeFind(timestamp, order, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Order order, Page page) {
        return executeFind(timestamp, order, page, key, operator, value,
                value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Page page) {
        return executeFind(timestamp, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Order order) {
        return executeFind(order, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Order order, Page page) {
        return executeFind(order, page, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Page page) {
        return executeFind(page, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Order order) {
        return executeFind(timestamp, order, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Order order, Page page) {
        return executeFind(timestamp, order, page, key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Page page) {
        return executeFind(timestamp, page, key, operator, value);
    }

    @Override
    public <T> long findOrAdd(String key, T value)
            throws DuplicateEntryException {
        return execute(() -> {
            return core.findOrAddKeyValue(key, Convert.javaToThrift(value),
                    creds, transaction, environment);
        });
    }

    @Override
    public long findOrInsert(Criteria criteria, String json)
            throws DuplicateEntryException {
        return execute(() -> {
            return core.findOrInsertCriteriaJson(
                    Language.translateToThriftCriteria(criteria), json, creds,
                    transaction, environment);
        });
    }

    @Override
    public long findOrInsert(String ccl, String json)
            throws DuplicateEntryException {
        return execute(() -> {
            return core.findOrInsertCclJson(ccl, json, creds, transaction,
                    environment);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysRecords(
                    Collections.toList(keys), Collections.toLongList(records),
                    creds, transaction, environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysRecordsOrder(
                    Collections.toList(keys), Collections.toLongList(records),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysRecordsOrderPage(
                    Collections.toList(keys), Collections.toLongList(records),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysRecordsPage(
                    Collections.toList(keys), Collections.toLongList(records),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysRecordsTimestr(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeysRecordsTime(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysRecordsTimestrOrder(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysRecordsTimeOrder(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysRecordsTimestrOrderPage(
                        Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysRecordsTimeOrderPage(
                        Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysRecordsTimestrPage(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysRecordsTimePage(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCriteria(
                    Collections.toList(keys),
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCriteriaOrder(
                    Collections.toList(keys),
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core
                    .getKeysCriteriaOrderPage(Collections.toList(keys),
                            Language.translateToThriftCriteria(criteria),
                            JavaThriftBridge.convert(order),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCriteriaPage(
                    Collections.toList(keys),
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCriteriaTimestr(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.getKeysCriteriaTime(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCriteriaTimestrOrder(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeysCriteriaTimeOrder(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCriteriaTimestrOrderPage(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysCriteriaTimeOrderPage(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCriteriaTimestrPage(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeysCriteriaTimePage(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record) {
        return execute(() -> {
            Map<String, TObject> data = core.getKeysRecord(
                    Collections.toList(keys), record, creds, transaction,
                    environment);
            return DataRow.singleValued(data);
        });
    }

    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record,
            Timestamp timestamp) {
        return execute(() -> {
            Map<String, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeysRecordTimestr(Collections.toList(keys),
                        record, timestamp.toString(), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysRecordTime(Collections.toList(keys), record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataRow.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCcl(
                    Collections.toList(keys), ccl, creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCclOrder(
                    Collections.toList(keys), ccl,
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCclOrderPage(
                    Collections.toList(keys), ccl,
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getKeysCclPage(
                    Collections.toList(keys), ccl,
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCclTimestr(Collections.toList(keys), ccl,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.getKeysCclTime(Collections.toList(keys), ccl,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCclTimestrOrder(Collections.toList(keys),
                        ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysCclTimeOrder(Collections.toList(keys), ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCclTimestrOrderPage(Collections.toList(keys),
                        ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeysCclTimeOrderPage(Collections.toList(keys),
                        ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getKeysCclTimestrPage(Collections.toList(keys), ccl,
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeysCclTimePage(Collections.toList(keys), ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCriteria(
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCriteriaOrder(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCriteriaOrderPage(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCriteriaPage(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCriteriaTimestr(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.getCriteriaTime(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCriteriaTimestrOrder(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.getCriteriaTimeOrder(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCriteriaTimestrOrderPage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getCriteriaTimeOrderPage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCriteriaTimestrPage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.getCriteriaTimePage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCcl(ccl, creds,
                    transaction, environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyRecords(key,
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Order order) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyRecordsOrder(key,
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyRecordsOrderPage(key,
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Page page) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyRecordsPage(key,
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeyRecordsTime(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyRecordsTimestrOrder(key,
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeyRecordsTimeOrder(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyRecordsTimestrOrderPage(key,
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeyRecordsTimeOrderPage(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyRecordsTimestrPage(key,
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeyRecordsTimePage(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Order order) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCriteriaOrder(key,
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCriteriaOrderPage(key,
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Page page) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCriteriaPage(key,
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCriteriaTimestr(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.getKeyCriteriaTime(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCriteriaTimestrOrder(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeyCriteriaTimeOrder(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCriteriaTimestrOrderPage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeyCriteriaTimeOrderPage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCriteriaTimestrPage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeyCriteriaTimePage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(String key, long record) {
        return execute(() -> {
            TObject raw = core.getKeyRecord(key, record, creds, transaction,
                    environment);
            return raw == TObject.NULL ? null : (T) Convert.thriftToJava(raw);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(String key, long record, Timestamp timestamp) {
        return execute(() -> {
            TObject raw;
            if(timestamp.isString()) {
                raw = core.getKeyRecordTimestr(key, record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = core.getKeyRecordTime(key, record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return raw == TObject.NULL ? null : (T) Convert.thriftToJava(raw);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCclOrder(ccl,
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCclOrderPage(ccl,
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data = core.getCclPage(ccl,
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCcl(key, ccl, creds,
                    transaction, environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Order order) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCclOrder(key, ccl,
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCclOrderPage(key, ccl,
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Page page) {
        return execute(() -> {
            Map<Long, TObject> data = core.getKeyCclPage(key, ccl,
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCclTimestr(key, ccl, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeyCclTime(key, ccl, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Order order) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCclTimestrOrder(key, ccl,
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.getKeyCclTimeOrder(key, ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCclTimestrOrderPage(key, ccl,
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeyCclTimeOrderPage(key, ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Page page) {
        return execute(() -> {
            Map<Long, TObject> data;
            if(timestamp.isString()) {
                data = core.getKeyCclTimestrPage(key, ccl, timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getKeyCclTimePage(key, ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.singleValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCclTimestr(ccl, timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                data = core.getCclTime(ccl, timestamp.getMicros(), creds,
                        transaction, environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Order order) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCclTimestrOrder(ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.getCclTimeOrder(ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCclTimestrOrderPage(ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getCclTimeOrderPage(ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> data;
            if(timestamp.isString()) {
                data = core.getCclTimestrPage(ccl, timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.getCclTimePage(ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.singleValued(data);
        });
    }

    @Override
    public String getServerEnvironment() {
        return execute(() -> {
            return core.getServerEnvironment(creds, transaction, environment);
        });
    }

    @Override
    public String getServerVersion() {
        return execute(() -> {
            return core.getServerVersion();
        });
    }

    @Override
    public Set<Long> insert(String json) {
        return execute(() -> {
            return core.insertJson(json, creds, transaction, environment);
        });
    }

    @Override
    public Map<Long, Boolean> insert(String json, Collection<Long> records) {
        return execute(() -> {
            return core.insertJsonRecords(json, Collections.toLongList(records),
                    creds, transaction, environment);
        });
    }

    @Override
    public boolean insert(String json, long record) {
        return execute(() -> {
            return core.insertJsonRecord(json, record, creds, transaction,
                    environment);
        });
    }

    @Override
    public Set<Long> inventory() {
        return execute(() -> {
            return core.inventory(creds, transaction, environment);
        });
    }

    @Override
    public <T> T invokePlugin(String id, String method, Object... args) {
        return execute(() -> {
            List<ComplexTObject> params = Lists
                    .newArrayListWithCapacity(args.length);
            for (Object arg : args) {
                params.add(ComplexTObject.fromJavaObject(arg));
            }
            ComplexTObject result = core.invokePlugin(id, method, params, creds,
                    transaction, environment);
            return result.getJavaObject();
        });
    }

    @Override
    public String jsonify(Collection<Long> records) {
        return jsonify(records, true);
    }

    @Override
    public String jsonify(Collection<Long> records, boolean includeId) {
        return execute(() -> {
            return core.jsonifyRecords(Collections.toLongList(records),
                    includeId, creds, transaction, environment);
        });
    }

    @Override
    public String jsonify(Collection<Long> records, Timestamp timestamp) {
        return jsonify(records, timestamp, true);
    }

    @Override
    public String jsonify(Collection<Long> records, Timestamp timestamp,
            boolean includeId) {
        return execute(() -> {
            if(timestamp.isString()) {
                return core.jsonifyRecordsTimestr(
                        Collections.toLongList(records), timestamp.toString(),
                        includeId, creds, transaction, environment);
            }
            else {
                return core.jsonifyRecordsTime(Collections.toLongList(records),
                        timestamp.getMicros(), includeId, creds, transaction,
                        environment);
            }
        });
    }

    @Override
    public String jsonify(long record) {
        return jsonify(java.util.Collections.singletonList(record), true);
    }

    @Override
    public String jsonify(long record, boolean includeId) {
        return jsonify(java.util.Collections.singletonList(record), includeId);
    }

    @Override
    public String jsonify(long record, Timestamp timestamp) {
        return jsonify(java.util.Collections.singletonList(record), timestamp,
                true);
    }

    @Override
    public String jsonify(long record, Timestamp timestamp, boolean includeId) {
        return jsonify(java.util.Collections.singletonList(record), timestamp,
                includeId);
    }

    @Override
    public Map<Long, Boolean> link(String key, Collection<Long> destinations,
            long source) {
        Map<Long, Boolean> result = PrettyLinkedHashMap
                .newPrettyLinkedHashMap("Record", "Result");
        for (long destination : destinations) {
            result.put(destination, link(key, destination, source));
        }
        return result;
    }

    @Override
    public boolean link(String key, long destination, long source) {
        return add(key, Link.to(destination), source);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(
            final Collection<String> keys, final Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = navigate
                    .navigateKeysRecords(Collections.toList(keys),
                            Collections.toLongList(records), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(
            final Collection<String> keys, final Collection<Long> records,
            final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = navigate
                    .navigateKeysRecordsTime(Collections.toList(keys),
                            Collections.toLongList(records),
                            timestamp.getMicros(), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = navigate
                    .navigateKeysCriteria(Collections.toList(keys),
                            Language.translateToThriftCriteria(criteria), creds,
                            transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeysCriteriaTimestr(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = navigate.navigateKeysCriteriaTime(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(
            final Collection<String> keys, final long record) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = navigate
                    .navigateKeysRecord(Collections.toList(keys), record, creds,
                            transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(
            final Collection<String> keys, final long record,
            final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeysRecordTimestr(
                        Collections.toList(keys), record, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = navigate.navigateKeysRecordTime(Collections.toList(keys),
                        record, timestamp.getMicros(), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(
            final Collection<String> keys, final String ccl) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = navigate
                    .navigateKeysCcl(Collections.toList(keys), ccl, creds,
                            transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(
            final Collection<String> keys, final String ccl,
            final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeysCclTimestr(Collections.toList(keys),
                        ccl, timestamp.toString(), creds, transaction,
                        environment);
            }
            else {
                data = navigate.navigateKeysCclTime(Collections.toList(keys),
                        ccl, timestamp.getMicros(), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key,
            final Collection<Long> records) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = navigate.navigateKeyRecords(key,
                    Collections.toLongList(records), creds, transaction,
                    environment);
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key,
            final Collection<Long> records, final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = navigate.navigateKeyRecordsTime(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key,
            final Criteria criteria) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = navigate.navigateKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key,
            final Criteria criteria, final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeyCriteriaTimestr(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = navigate.navigateKeyCriteriaTime(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key, final long record) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = navigate.navigateKeyRecord(key,
                    record, creds, transaction, environment);
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key, final long record,
            final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeyRecordTimestr(key, record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = navigate.navigateKeyRecordTime(key, record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key, final String ccl) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = navigate.navigateKeyCcl(key, ccl,
                    creds, transaction, environment);
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(final String key, final String ccl,
            final Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = navigate.navigateKeyCclTimestr(key, ccl,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = navigate.navigateKeyCclTime(key, ccl,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            String destination = Navigation.getKeyDestination(key);
            return DataColumn.multiValued(destination, data);
        });
    }

    @Override
    public Map<Long, Boolean> ping(Collection<Long> records) {
        return execute(() -> {
            return core.pingRecords(Collections.toLongList(records), creds,
                    transaction, environment);
        });
    }

    @Override
    public boolean ping(long record) {
        return execute(() -> {
            return core.pingRecord(record, creds, transaction, environment);
        });
    }

    @Override
    public <T> void reconcile(String key, long record, Collection<T> values) {
        execute(() -> {
            Set<TObject> valueSet = Sets
                    .newHashSetWithExpectedSize(values.size());
            for (T value : values) {
                valueSet.add(Convert.javaToThrift(value));
            }
            core.reconcileKeyRecordValues(key, record, valueSet, creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Boolean> raw = core.removeKeyValueRecords(key,
                    Convert.javaToThrift(value),
                    Collections.toLongList(records), creds, transaction,
                    environment);
            Map<Long, Boolean> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", "Result");
            for (long record : records) {
                pretty.put(record, raw.get(record));
            }
            return pretty;
        });
    }

    @Override
    public <T> boolean remove(String key, T value, long record) {
        return execute(() -> {
            return core.removeKeyValueRecord(key, Convert.javaToThrift(value),
                    record, creds, transaction, environment);
        });
    }

    @Override
    public void revert(Collection<String> keys, Collection<Long> records,
            Timestamp timestamp) {
        execute(() -> {
            if(timestamp.isString()) {
                core.revertKeysRecordsTimestr(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                core.revertKeysRecordsTime(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return null;
        });
    }

    @Override
    public void revert(Collection<String> keys, long record,
            Timestamp timestamp) {
        execute(() -> {
            if(timestamp.isString()) {
                core.revertKeysRecordTimestr(Collections.toList(keys), record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                core.revertKeysRecordTime(Collections.toList(keys), record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return null;
        });
    }

    @Override
    public void revert(String key, Collection<Long> records,
            Timestamp timestamp) {
        execute(() -> {
            if(timestamp.isString()) {
                core.revertKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                core.revertKeyRecordsTime(key, Collections.toLongList(records),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return null;
        });
    }

    @Override
    public void revert(String key, long record, Timestamp timestamp) {
        execute(() -> {
            if(timestamp.isString()) {
                core.revertKeyRecordTimestr(key, record, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                core.revertKeyRecordTime(key, record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return null;
        });
    }

    @Override
    public Set<Long> search(String key, String query) {
        return execute(() -> {
            return core.search(key, query, creds, transaction, environment);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectRecords(
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectRecordsOrder(
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectRecordsOrderPage(Collections.toLongList(records),
                            JavaThriftBridge.convert(order),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectRecordsPage(
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectRecordsTimestr(
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.selectRecordsTime(Collections.toLongList(records),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectRecordsTimestrOrder(
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.selectRecordsTimeOrder(
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectRecordsTimestrOrderPage(
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectRecordsTimeOrderPage(
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectRecordsTimestrPage(
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectRecordsTimePage(
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectKeysRecords(
                    Collections.toList(keys), Collections.toLongList(records),
                    creds, transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysRecordsOrder(Collections.toList(keys),
                            Collections.toLongList(records),
                            JavaThriftBridge.convert(order), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysRecordsOrderPage(Collections.toList(keys),
                            Collections.toLongList(records),
                            JavaThriftBridge.convert(order),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysRecordsPage(Collections.toList(keys),
                            Collections.toLongList(records),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysRecordsTimestr(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeysRecordsTime(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysRecordsTimestrOrder(
                        Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysRecordsTimeOrder(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysRecordsTimestrOrderPage(
                        Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysRecordsTimeOrderPage(
                        Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysRecordsTimestrPage(
                        Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysRecordsTimePage(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectKeysCriteria(
                    Collections.toList(keys),
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysCriteriaOrder(Collections.toList(keys),
                            Language.translateToThriftCriteria(criteria),
                            JavaThriftBridge.convert(order), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysCriteriaOrderPage(Collections.toList(keys),
                            Language.translateToThriftCriteria(criteria),
                            JavaThriftBridge.convert(order),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysCriteriaPage(Collections.toList(keys),
                            Language.translateToThriftCriteria(criteria),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCriteriaTimestr(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.selectKeysCriteriaTime(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCriteriaTimestrOrder(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeysCriteriaTimeOrder(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCriteriaTimestrOrderPage(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysCriteriaTimeOrderPage(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCriteriaTimestrPage(
                        Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeysCriteriaTimePage(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys,
            long record) {
        return execute(() -> {
            Map<String, Set<TObject>> data = core.selectKeysRecord(
                    Collections.toList(keys), record, creds, transaction,
                    environment);
            return DataRow.multiValued(data);
        });
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys, long record,
            Timestamp timestamp) {
        return execute(() -> {
            Map<String, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeysRecordTimestr(Collections.toList(keys),
                        record, timestamp.toString(), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysRecordTime(Collections.toList(keys),
                        record, timestamp.getMicros(), creds, transaction,
                        environment);
            }
            return DataRow.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectKeysCcl(
                    Collections.toList(keys), ccl, creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectKeysCclOrder(
                    Collections.toList(keys), ccl,
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectKeysCclOrderPage(Collections.toList(keys), ccl,
                            JavaThriftBridge.convert(order),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectKeysCclPage(
                    Collections.toList(keys), ccl,
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCclTimestr(Collections.toList(keys), ccl,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.selectKeysCclTime(Collections.toList(keys), ccl,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCclTimestrOrder(Collections.toList(keys),
                        ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysCclTimeOrder(Collections.toList(keys),
                        ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCclTimestrOrderPage(
                        Collections.toList(keys), ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysCclTimeOrderPage(Collections.toList(keys),
                        ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectKeysCclTimestrPage(Collections.toList(keys),
                        ccl, timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeysCclTimePage(Collections.toList(keys), ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectCriteria(
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectCriteriaOrder(
                            Language.translateToThriftCriteria(criteria),
                            JavaThriftBridge.convert(order), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core
                    .selectCriteriaOrderPage(
                            Language.translateToThriftCriteria(criteria),
                            JavaThriftBridge.convert(order),
                            JavaThriftBridge.convert(page), creds, transaction,
                            environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectCriteriaPage(
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCriteriaTimestr(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.selectCriteriaTime(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCriteriaTimestrOrder(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.selectCriteriaTimeOrder(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCriteriaTimestrOrderPage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectCriteriaTimeOrderPage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCriteriaTimestrPage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.selectCriteriaTimePage(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public Map<String, Set<Object>> select(long record) {
        return execute(() -> {
            Map<String, Set<TObject>> data = core.selectRecord(record, creds,
                    transaction, environment);
            return DataRow.multiValued(data);
        });
    }

    @Override
    public Map<String, Set<Object>> select(long record, Timestamp timestamp) {
        return execute(() -> {
            Map<String, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectRecordTimestr(record, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.selectRecordTime(record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataRow.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectCcl(ccl,
                    creds, transaction, environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyRecords(key,
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Order order) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyRecordsOrder(key,
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyRecordsOrderPage(key,
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyRecordsPage(key,
                    Collections.toLongList(records),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeyRecordsTime(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyRecordsTimestrOrder(key,
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeyRecordsTimeOrder(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyRecordsTimestrOrderPage(key,
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeyRecordsTimeOrderPage(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyRecordsTimestrPage(key,
                        Collections.toLongList(records), timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeyRecordsTimePage(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Order order) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCriteriaOrder(key,
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Order order, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCriteriaOrderPage(key,
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCriteriaPage(key,
                    Language.translateToThriftCriteria(criteria),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCriteriaTimestr(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                data = core.selectKeyCriteriaTime(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCriteriaTimestrOrder(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeyCriteriaTimeOrder(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCriteriaTimestrOrderPage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeyCriteriaTimeOrderPage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCriteriaTimestrPage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeyCriteriaTimePage(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Set<T> select(String key, long record) {
        return execute(() -> {
            Set<TObject> values = core.selectKeyRecord(key, record, creds,
                    transaction, environment);
            return Transformers.transformSetLazily(values,
                    Conversions.<T> thriftToJavaCasted());
        });
    }

    @Override
    public <T> Set<T> select(String key, long record, Timestamp timestamp) {
        return execute(() -> {
            Set<TObject> values;
            if(timestamp.isString()) {
                values = core.selectKeyRecordTimestr(key, record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                values = core.selectKeyRecordTime(key, record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return Transformers.transformSetLazily(values,
                    Conversions.<T> thriftToJavaCasted());
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectCclOrder(ccl,
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectCclOrderPage(
                    ccl, JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data = core.selectCclPage(ccl,
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCcl(key, ccl, creds,
                    transaction, environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Order order) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCclOrder(key, ccl,
                    JavaThriftBridge.convert(order), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Order order,
            Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCclOrderPage(key, ccl,
                    JavaThriftBridge.convert(order),
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data = core.selectKeyCclPage(key, ccl,
                    JavaThriftBridge.convert(page), creds, transaction,
                    environment);
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCclTimestr(key, ccl, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeyCclTime(key, ccl, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCclTimestrOrder(key, ccl,
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeyCclTimeOrder(key, ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCclTimestrOrderPage(key, ccl,
                        timestamp.toString(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectKeyCclTimeOrderPage(key, ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Set<TObject>> data;
            if(timestamp.isString()) {
                data = core.selectKeyCclTimestrPage(key, ccl,
                        timestamp.toString(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            else {
                data = core.selectKeyCclTimePage(key, ccl,
                        timestamp.getMicros(), JavaThriftBridge.convert(page),
                        creds, transaction, environment);
            }
            return DataColumn.multiValued(key, data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCclTimestr(ccl, timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                data = core.selectCclTime(ccl, timestamp.getMicros(), creds,
                        transaction, environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Order order) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCclTimestrOrder(ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                data = core.selectCclTimeOrder(ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Order order, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCclTimestrOrderPage(ccl, timestamp.toString(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectCclTimeOrderPage(ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Page page) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> data;
            if(timestamp.isString()) {
                data = core.selectCclTimestrPage(ccl, timestamp.toString(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                data = core.selectCclTimePage(ccl, timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            return DataTable.multiValued(data);
        });
    }

    @Override
    public void set(String key, Object value, Collection<Long> records) {
        execute(() -> {
            core.setKeyValueRecords(key, Convert.javaToThrift(value),
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return null;
        });
    }

    @Override
    public <T> void set(String key, T value, long record) {
        execute(() -> {
            core.setKeyValueRecord(key, Convert.javaToThrift(value), record,
                    creds, transaction, environment);
            return null;
        });
    }

    @Override
    public void stage() throws TransactionException {
        execute(() -> {
            transaction = core.stage(creds, environment);
            return null;
        });
    }

    @Override
    public Timestamp time() {
        return execute(() -> {
            return Timestamp
                    .fromMicros(core.time(creds, transaction, environment));
        });
    }

    @Override
    public Timestamp time(String phrase) {
        return execute(() -> {
            return Timestamp.fromMicros(
                    core.timePhrase(phrase, creds, transaction, environment));
        });
    }

    @Override
    public String toString() {
        return "Connected to " + host + ":" + port + " as "
                + new String(ClientSecurity.decrypt(username).array());
    }

    @Override
    public boolean unlink(String key, long destination, long source) {
        return remove(key, Link.to(destination), source);
    }

    @Override
    public boolean verify(String key, Object value, long record) {
        return execute(() -> {
            return core.verifyKeyValueRecord(key, Convert.javaToThrift(value),
                    record, creds, transaction, environment);
        });
    }

    @Override
    public boolean verify(String key, Object value, long record,
            Timestamp timestamp) {
        return execute(() -> {
            if(timestamp.isString()) {
                return core.verifyKeyValueRecordTimestr(key,
                        Convert.javaToThrift(value), record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                return core.verifyKeyValueRecordTime(key,
                        Convert.javaToThrift(value), record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
        });
    }

    @Override
    public boolean verifyAndSwap(String key, Object expected, long record,
            Object replacement) {
        return execute(() -> {
            return core.verifyAndSwap(key, Convert.javaToThrift(expected),
                    record, Convert.javaToThrift(replacement), creds,
                    transaction, environment);
        });
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        execute(() -> {
            core.verifyOrSet(key, Convert.javaToThrift(value), record, creds,
                    transaction, environment);
            return null;
        });
    }

    /**
     * Authenticate the {@link #username} and {@link #password} and populate
     * {@link #creds} with the appropriate AccessToken.
     */
    private void authenticate() {
        try {
            creds = core.login(ClientSecurity.decrypt(username),
                    ClientSecurity.decrypt(password), environment);
        }
        catch (TException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values}.
     * 
     * @param order
     * @param page
     * @param key
     * @param operator
     * @param values
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(Order order, Page page, final String key,
            final Object operator, final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesOrderPage(key,
                        (Operator) operator, tValues,
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                return core.findKeyOperatorstrValuesOrderPage(key,
                        operator.toString(), tValues,
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values}.
     * 
     * @param order
     * @param key
     * @param operator
     * @param values
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(Order order, final String key,
            final Object operator, final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesOrder(key, (Operator) operator,
                        tValues, JavaThriftBridge.convert(order), creds,
                        transaction, environment);
            }
            else {
                return core.findKeyOperatorstrValuesOrder(key,
                        operator.toString(), tValues,
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values}.
     * 
     * @param page
     * @param key
     * @param operator
     * @param values
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(Page page, final String key,
            final Object operator, final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesPage(key, (Operator) operator,
                        tValues, JavaThriftBridge.convert(page), creds,
                        transaction, environment);
            }
            else {
                return core.findKeyOperatorstrValuesPage(key,
                        operator.toString(), tValues,
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values}.
     * 
     * @param key
     * @param operator
     * @param values
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(final String key, final Object operator,
            final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValues(key, (Operator) operator,
                        tValues, creds, transaction, environment);
            }
            else {
                return core.findKeyOperatorstrValues(key, operator.toString(),
                        tValues, creds, transaction, environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values} at {@code timestamp}.
     * 
     * @param timestamp
     * @param order
     * @param page
     * @param key
     * @param operator
     * @param values
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(final Timestamp timestamp, Order order,
            Page page, final String key, final Object operator,
            final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesTimeOrderPage(key,
                        (Operator) operator, tValues, timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                return core.findKeyOperatorstrValuesTimeOrderPage(key,
                        operator.toString(), tValues, timestamp.getMicros(),
                        JavaThriftBridge.convert(order),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values} at {@code timestamp}.
     * 
     * @param key
     * @param operator
     * @param values
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(final Timestamp timestamp, Order order,
            final String key, final Object operator, final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesTimeOrder(key,
                        (Operator) operator, tValues, timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
            else {
                return core.findKeyOperatorstrValuesTimeOrder(key,
                        operator.toString(), tValues, timestamp.getMicros(),
                        JavaThriftBridge.convert(order), creds, transaction,
                        environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values} at {@code timestamp}.
     * 
     * @param timestamp
     * @param page
     * @param key
     * @param operator
     * @param values
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(final Timestamp timestamp, Page page,
            final String key, final Object operator, final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesTimePage(key,
                        (Operator) operator, tValues, timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
            else {
                return core.findKeyOperatorstrValuesTimePage(key,
                        operator.toString(), tValues, timestamp.getMicros(),
                        JavaThriftBridge.convert(page), creds, transaction,
                        environment);
            }
        });
    }

    /**
     * Perform an old-school/simple find operation where {@code key}
     * satisfied {@code operation} in relation to the specified
     * {@code values} at {@code timestamp}.
     * 
     * @param key
     * @param operator
     * @param values
     * @param timestamp a {@link Timestamp} that represents the historical
     *            instant to use in the lookup – created from either a
     *            {@link Timestamp#fromString(String) natural language
     *            description} of a point in time (i.e. two weeks ago), OR
     *            the {@link Timestamp#fromMicros(long) number
     *            of microseconds} since the Unix epoch, OR
     *            a {@link Timestamp#fromJoda(org.joda.time.DateTime) Joda
     *            DateTime} object
     * @return the records that match the criteria.
     */
    private Set<Long> executeFind(final Timestamp timestamp, final String key,
            final Object operator, final Object... values) {
        final List<TObject> tValues = Arrays.stream(values)
                .map(Convert::javaToThrift).collect(Collectors.toList());
        return execute(() -> {
            if(operator instanceof Operator) {
                return core.findKeyOperatorValuesTime(key, (Operator) operator,
                        tValues, timestamp.getMicros(), creds, transaction,
                        environment);
            }
            else {
                return core.findKeyOperatorstrValuesTime(key,
                        operator.toString(), tValues, timestamp.getMicros(),
                        creds, transaction, environment);
            }
        });
    }

    @Override
    protected Concourse copyConnection() {
        return new ConcourseThriftDriver(host, port,
                ByteBuffers.getString(ClientSecurity.decrypt(username)),
                ByteBuffers.getString(ClientSecurity.decrypt(password)),
                environment);
    }

    /**
     * Return the thrift calculate RPC client.
     * 
     * @return the {@link #calculate client}
     */
    ConcourseCalculateService.Client $calculate() {
        return calculate;
    }

    /**
     * Return the thrift RPC client.
     * 
     * @return the {@link ConcourseService#Client}
     */
    ConcourseService.Client $core() {
        return core;
    }

    /**
     * Return the current {@link AccessToken}
     * 
     * @return the creds
     */
    AccessToken creds() {
        return creds;
    }

    /**
     * Return the environment to which the driver is connected.
     * 
     * @return the environment
     */
    String environment() {
        return environment;
    }

    /**
     * Execute the task defined in {@code callable}. This method contains
     * retry logic to handle cases when {@code creds} expires and must be
     * updated.
     * 
     * @param callable
     * @return the task result
     */
    <T> T execute(Callable<T> callable) {
        try {
            return callable.call();
        }
        catch (SecurityException e) {
            authenticate();
            return execute(callable);
        }
        catch (com.cinchapi.concourse.thrift.TransactionException e) {
            throw new TransactionException();
        }
        catch (com.cinchapi.concourse.thrift.DuplicateEntryException e) {
            throw new DuplicateEntryException(e);
        }
        catch (com.cinchapi.concourse.thrift.InvalidArgumentException e) {
            throw new InvalidArgumentException(e);
        }
        catch (com.cinchapi.concourse.thrift.InvalidOperationException e) {
            throw new UnsupportedOperationException(e);
        }
        catch (com.cinchapi.concourse.thrift.ParseException e) {
            throw new ParseException(e);
        }
        catch (com.cinchapi.concourse.thrift.PermissionException e) {
            throw new PermissionException(e);
        }
        catch (com.cinchapi.concourse.thrift.ManagementException e) {
            throw new ManagementException(e);
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Return the current {@link TransactionToken}.
     * 
     * @return the transaction token
     */
    @Nullable
    TransactionToken transaction() {
        return transaction;
    }

}

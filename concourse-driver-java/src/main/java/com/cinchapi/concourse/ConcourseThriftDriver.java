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
package com.cinchapi.concourse;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cinchapi.concourse.config.ConcourseClientPreferences;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.security.ClientSecurity;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.ConcourseService;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Collections;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.base.Throwables;
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
        ConcourseClientPreferences config;
        try {
            config = ConcourseClientPreferences.open("concourse_client.prefs");
        }
        catch (Exception e) {
            config = null;
        }
        SERVER_HOST = "localhost";
        SERVER_PORT = 1717;
        USERNAME = "admin";
        PASSWORD = "admin";
        ENVIRONMENT = "";
        if(config != null) {
            SERVER_HOST = config.getString("host", SERVER_HOST);
            SERVER_PORT = config.getInt("port", SERVER_PORT);
            USERNAME = config.getString("username", USERNAME);
            PASSWORD = config.getString("password", PASSWORD);
            ENVIRONMENT = config.getString("environment", ENVIRONMENT);
        }
    }

    /**
     * The Thrift client that actually handles all RPC communication.
     */
    private final ConcourseService.Client client;

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
            client = new ConcourseService.Client(protocol);
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
                client.abort(creds, token, environment);
            }
            return null;
        });
    }

    @Override
    public <T> long add(String key, T value) {
        return execute(() -> {
            return client.addKeyValue(key, Convert.javaToThrift(value), creds,
                    transaction, environment);
        });
    }

    @Override
    public <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Boolean> raw = client.addKeyValueRecords(key,
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
            return client.addKeyValueRecord(key, Convert.javaToThrift(value),
                    record, creds, transaction, environment);
        });
    }

    @Override
    public Map<Timestamp, String> audit(long record) {
        return execute(() -> {
            Map<Long, String> audit = client.auditRecord(record, creds,
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
                audit = client.auditRecordStartstr(record, start.toString(),
                        creds, transaction, environment);
            }
            else {
                audit = client.auditRecordStart(record, start.getMicros(),
                        creds, transaction, environment);
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
                audit = client.auditRecordStartstrEndstr(record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                audit = client.auditRecordStartEnd(record, start.getMicros(),
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
            Map<Long, String> audit = client.auditKeyRecord(key, record, creds,
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
                audit = client.auditKeyRecordStartstr(key, record,
                        start.toString(), creds, transaction, environment);
            }
            else {
                audit = client.auditKeyRecordStart(key, record,
                        start.getMicros(), creds, transaction, environment);
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
                audit = client.auditKeyRecordStartstrEndstr(key, record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                audit = client.auditKeyRecordStartEnd(key, record,
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
            Map<String, Map<TObject, Set<Long>>> raw = client.browseKeys(
                    Collections.toList(keys), creds, transaction, environment);
            Map<String, Map<Object, Set<Long>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Key");
            for (Entry<String, Map<TObject, Set<Long>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.thriftToJava(),
                                Conversions.<Long> none()));
            }
            return pretty;
        });
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys,
            Timestamp timestamp) {
        return execute(() -> {
            Map<String, Map<TObject, Set<Long>>> raw;
            if(timestamp.isString()) {
                raw = client.browseKeysTimestr(Collections.toList(keys),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.browseKeysTime(Collections.toList(keys),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<String, Map<Object, Set<Long>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Key");
            for (Entry<String, Map<TObject, Set<Long>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.thriftToJava(),
                                Conversions.<Long> none()));
            }
            return pretty;
        });
    }

    @Override
    public Map<Object, Set<Long>> browse(String key) {
        return execute(() -> {
            Map<TObject, Set<Long>> raw = client.browseKey(key, creds,
                    transaction, environment);
            Map<Object, Set<Long>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap(key, "Records");
            for (Entry<TObject, Set<Long>> entry : raw.entrySet()) {
                pretty.put(Convert.thriftToJava(entry.getKey()),
                        entry.getValue());
            }
            return pretty;
        });
    }

    @Override
    public Map<Object, Set<Long>> browse(String key, Timestamp timestamp) {
        return execute(() -> {
            Map<TObject, Set<Long>> raw;
            if(timestamp.isString()) {
                raw = client.browseKeyTimestr(key, timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = client.browseKeyTime(key, timestamp.getMicros(), creds,
                        transaction, environment);
            }
            Map<Object, Set<Long>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap(key, "Records");
            for (Entry<TObject, Set<Long>> entry : raw.entrySet()) {
                pretty.put(Convert.thriftToJava(entry.getKey()),
                        entry.getValue());
            }
            return pretty;
        });
    }

    @Override
    public Map<Timestamp, Set<Object>> chronologize(String key, long record) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw = client.chronologizeKeyRecord(key,
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
                raw = client.chronologizeKeyRecordStartstr(key, record,
                        start.toString(), creds, transaction, environment);
            }
            else {
                raw = client.chronologizeKeyRecordStart(key, record,
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
                raw = client.chronologizeKeyRecordStartstrEndstr(key, record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                raw = client.chronologizeKeyRecordStartEnd(key, record,
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
            client.clearRecords(Collections.toLongList(records), creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(Collection<String> keys, Collection<Long> records) {
        execute(() -> {
            client.clearKeysRecords(Collections.toList(keys),
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return null;
        });
    }

    @Override
    public void clear(Collection<String> keys, long record) {
        execute(() -> {
            client.clearKeysRecord(Collections.toList(keys), record, creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(long record) {
        execute(() -> {
            client.clearRecord(record, creds, transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(String key, Collection<Long> records) {
        execute(() -> {
            client.clearKeyRecords(key, Collections.toLongList(records), creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public void clear(String key, long record) {
        execute(() -> {
            client.clearKeyRecord(key, record, creds, transaction, environment);
            return null;
        });
    }

    @Override
    public boolean commit() {
        return execute(() -> {
            final TransactionToken token = transaction;
            transaction = null;
            return token != null ? client.commit(creds, token, environment)
                    : false;
        });
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records) {
        return execute(() -> {
            Map<Long, Set<String>> raw = client.describeRecords(
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
                raw = client.describeRecordsTimestr(
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.describeRecordsTime(
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
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
            Set<String> result = client.describeRecord(record, creds,
                    transaction, environment);
            return result;
        });
    }

    @Override
    public Set<String> describe(long record, Timestamp timestamp) {
        return execute(() -> {
            if(timestamp.isString()) {
                return client.describeRecordTimestr(record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                return client.describeRecordTime(record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
        });
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start) {
        return execute(() -> {
            Map<String, Map<Diff, Set<TObject>>> raw;
            if(start.isString()) {
                raw = client.diffRecordStartstr(record, start.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = client.diffRecordStart(record, start.getMicros(), creds,
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
                raw = client.diffRecordStartstrEndstr(record, start.toString(),
                        end.toString(), creds, transaction, environment);
            }
            else {
                raw = client.diffRecordStartEnd(record, start.getMicros(),
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
                raw = client.diffKeyRecordStartstr(key, record,
                        start.toString(), creds, transaction, environment);
            }
            else {
                raw = client.diffKeyRecordStart(key, record, start.getMicros(),
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
                raw = client.diffKeyRecordStartstrEndstr(key, record,
                        start.toString(), end.toString(), creds, transaction,
                        environment);
            }
            else {
                raw = client.diffKeyRecordStartEnd(key, record,
                        start.getMicros(), end.getMicros(), creds, transaction,
                        environment);
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
                raw = client.diffKeyStartstr(key, start.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = client.diffKeyStart(key, start.getMicros(), creds,
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
                raw = client.diffKeyStartstrEndstr(key, start.toString(),
                        end.toString(), creds, transaction, environment);
            }
            else {
                raw = client.diffKeyStartEnd(key, start.getMicros(),
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
            client.logout(creds, environment);
            client.getInputProtocol().getTransport().close();
            client.getOutputProtocol().getTransport().close();
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
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<Long> find(Criteria criteria) {
        return execute(() -> {
            return client.findCriteria(
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
        });
    }

    @Override
    public Set<Long> find(Object criteria) {
        if(criteria instanceof BuildableState) {
            return find(((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the find method");
        }
    }

    @Override
    public Set<Long> find(String ccl) {
        return execute(() -> {
            return client.findCcl(ccl, creds, transaction, environment);
        });
    }

    @Override
    public Set<Long> find(String key, Object value) {
        return executeFind(key, Operator.EQUALS, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp) {
        return executeFind(key, Operator.EQUALS, value, timestamp);
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
            Object value2, Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value);
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
            Object value2, Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp) {
        return executeFind(timestamp, key, operator, value);
    }

    @Override
    public <T> long findOrAdd(String key, T value)
            throws DuplicateEntryException {
        return execute(() -> {
            return client.findOrAddKeyValue(key, Convert.javaToThrift(value),
                    creds, transaction, environment);
        });
    }

    @Override
    public long findOrInsert(Criteria criteria, String json)
            throws DuplicateEntryException {
        return execute(() -> {
            return client.findOrInsertCriteriaJson(
                    Language.translateToThriftCriteria(criteria), json, creds,
                    transaction, environment);
        });
    }

    @Override
    public long findOrInsert(String ccl, String json)
            throws DuplicateEntryException {
        return execute(() -> {
            return client.findOrInsertCclJson(ccl, json, creds, transaction,
                    environment);
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw = client.getKeysRecords(
                    Collections.toList(keys), Collections.toLongList(records),
                    creds, transaction, environment);
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw;
            if(timestamp.isString()) {
                raw = client.getKeysRecordsTimestr(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.getKeysRecordsTime(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw = client.getKeysCriteria(
                    Collections.toList(keys),
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw;
            if(timestamp.isString()) {
                raw = client.getKeysCriteriaTimestr(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.getKeysCriteriaTime(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record) {
        return execute(() -> {
            Map<String, TObject> raw = client.getKeysRecord(
                    Collections.toList(keys), record, creds, transaction,
                    environment);
            Map<String, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Key", "Value");
            for (Entry<String, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record,
            Timestamp timestamp) {
        return execute(() -> {
            Map<String, TObject> raw;
            if(timestamp.isString()) {
                raw = client.getKeysRecordTimestr(Collections.toList(keys),
                        record, timestamp.toString(), creds, transaction,
                        environment);
            }
            else {
                raw = client.getKeysRecordTime(Collections.toList(keys), record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<String, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Key", "Value");
            for (Entry<String, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria) {
        if(criteria instanceof BuildableState) {
            return get(keys, ((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria, Timestamp timestamp) {
        if(criteria instanceof BuildableState) {
            return get(keys, ((BuildableState) criteria).build(), timestamp);
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw = client.getKeysCcl(
                    Collections.toList(keys), ccl, creds, transaction,
                    environment);
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw;
            if(timestamp.isString()) {
                raw = client.getKeysCclTimestr(Collections.toList(keys), ccl,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.getKeysCclTime(Collections.toList(keys), ccl,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw = client.getCriteria(
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw;
            if(timestamp.isString()) {
                raw = client.getCriteriaTimestr(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.getCriteriaTime(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Object criteria) {
        if(criteria instanceof BuildableState) {
            return get(((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Object criteria,
            Timestamp timestamp) {
        if(criteria instanceof BuildableState) {
            return get(((BuildableState) criteria).build(), timestamp);
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw = client.getCcl(ccl, creds,
                    transaction, environment);
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records) {
        return execute(() -> {
            Map<Long, TObject> raw = client.getKeyRecords(key,
                    Collections.toLongList(records), creds, transaction,
                    environment);
            Map<Long, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, TObject> raw;
            if(timestamp.isString()) {
                raw = client.getKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.getKeyRecordsTime(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<Long, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria) {
        return execute(() -> {
            Map<Long, TObject> raw = client.getKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            Map<Long, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, TObject> raw;
            if(timestamp.isString()) {
                raw = client.getKeyCriteriaTimestr(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.getKeyCriteriaTime(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(String key, long record) {
        return execute(() -> {
            TObject raw = client.getKeyRecord(key, record, creds, transaction,
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
                raw = client.getKeyRecordTimestr(key, record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.getKeyRecordTime(key, record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return raw == TObject.NULL ? null : (T) Convert.thriftToJava(raw);
        });
    }

    @Override
    public <T> Map<Long, T> get(String key, Object criteria) {
        if(criteria instanceof BuildableState) {
            return get(key, ((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, T> get(String key, Object criteria,
            Timestamp timestamp) {
        if(criteria instanceof BuildableState) {
            return get(key, ((BuildableState) criteria).build(), timestamp);
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<Long, T> get(String key, String ccl) {
        return execute(() -> {
            Map<Long, TObject> raw = client.getKeyCcl(key, ccl, creds,
                    transaction, environment);
            Map<Long, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, TObject> raw;
            if(timestamp.isString()) {
                raw = client.getKeyCclTimestr(key, ccl, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.getKeyCclTime(key, ccl, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<Long, T> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, TObject> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        (T) Convert.thriftToJava(entry.getValue()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, TObject>> raw;
            if(timestamp.isString()) {
                raw = client.getCclTimestr(ccl, timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = client.getCclTime(ccl, timestamp.getMicros(), creds,
                        transaction, environment);
            }
            Map<Long, Map<String, T>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapValues(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public String getServerEnvironment() {
        return execute(() -> {
            return client.getServerEnvironment(creds, transaction, environment);
        });
    }

    @Override
    public String getServerVersion() {
        return execute(() -> {
            return client.getServerVersion();
        });
    }

    @Override
    public Set<Long> insert(String json) {
        return execute(() -> {
            return client.insertJson(json, creds, transaction, environment);
        });
    }

    @Override
    public Map<Long, Boolean> insert(String json, Collection<Long> records) {
        return execute(() -> {
            return client.insertJsonRecords(json,
                    Collections.toLongList(records), creds, transaction,
                    environment);
        });
    }

    @Override
    public boolean insert(String json, long record) {
        return execute(() -> {
            return client.insertJsonRecord(json, record, creds, transaction,
                    environment);
        });
    }

    @Override
    public Set<Long> inventory() {
        return execute(() -> {
            return client.inventory(creds, transaction, environment);
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
            ComplexTObject result = client.invokePlugin(id, method, params,
                    creds, transaction, environment);
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
            return client.jsonifyRecords(Collections.toLongList(records),
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
                return client.jsonifyRecordsTimestr(
                        Collections.toLongList(records), timestamp.toString(),
                        includeId, creds, transaction, environment);
            }
            else {
                return client.jsonifyRecordsTime(
                        Collections.toLongList(records), timestamp.getMicros(),
                        includeId, creds, transaction, environment);
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
    public Map<Long, Boolean> ping(Collection<Long> records) {
        return execute(() -> {
            return client.pingRecords(Collections.toLongList(records), creds,
                    transaction, environment);
        });
    }

    @Override
    public boolean ping(long record) {
        return execute(() -> {
            return client.pingRecord(record, creds, transaction, environment);
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
            client.reconcileKeyRecordValues(key, record, valueSet, creds,
                    transaction, environment);
            return null;
        });
    }

    @Override
    public <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Boolean> raw = client.removeKeyValueRecords(key,
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
            return client.removeKeyValueRecord(key, Convert.javaToThrift(value),
                    record, creds, transaction, environment);
        });
    }

    @Override
    public void revert(Collection<String> keys, Collection<Long> records,
            Timestamp timestamp) {
        execute(() -> {
            if(timestamp.isString()) {
                client.revertKeysRecordsTimestr(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                client.revertKeysRecordsTime(Collections.toList(keys),
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
                client.revertKeysRecordTimestr(Collections.toList(keys), record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                client.revertKeysRecordTime(Collections.toList(keys), record,
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
                client.revertKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                client.revertKeyRecordsTime(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return null;
        });
    }

    @Override
    public void revert(String key, long record, Timestamp timestamp) {
        execute(() -> {
            if(timestamp.isString()) {
                client.revertKeyRecordTimestr(key, record, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                client.revertKeyRecordTime(key, record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            return null;
        });
    }

    @Override
    public Set<Long> search(String key, String query) {
        return execute(() -> {
            return client.search(key, query, creds, transaction, environment);
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw = client.selectRecords(
                    Collections.toLongList(records), creds, transaction,
                    environment);
            Map<Long, Map<String, Set<Object>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw;
            if(timestamp.isString()) {
                raw = client.selectRecordsTimestr(
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.selectRecordsTime(Collections.toLongList(records),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, Set<Object>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw = client.selectKeysRecords(
                    Collections.toList(keys), Collections.toLongList(records),
                    creds, transaction, environment);
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeysRecordsTimestr(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.selectKeysRecordsTime(Collections.toList(keys),
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw = client
                    .selectKeysCriteria(Collections.toList(keys),
                            Language.translateToThriftCriteria(criteria), creds,
                            transaction, environment);
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeysCriteriaTimestr(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.selectKeysCriteriaTime(Collections.toList(keys),
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys,
            long record) {
        return execute(() -> {
            Map<String, Set<TObject>> raw = client.selectKeysRecord(
                    Collections.toList(keys), record, creds, transaction,
                    environment);
            Map<String, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Key", "Values");
            for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys, long record,
            Timestamp timestamp) {
        return execute(() -> {
            Map<String, Set<TObject>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeysRecordTimestr(Collections.toList(keys),
                        record, timestamp.toString(), creds, transaction,
                        environment);
            }
            else {
                raw = client.selectKeysRecordTime(Collections.toList(keys),
                        record, timestamp.getMicros(), creds, transaction,
                        environment);
            }
            Map<String, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Key", "Values");
            for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Object criteria) {
        if(criteria instanceof BuildableState) {
            return select(keys, ((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(criteria
                    + " is not a valid argument for the select method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Object criteria, Timestamp timestamp) {
        if(criteria instanceof BuildableState) {
            return select(keys, ((BuildableState) criteria).build(), timestamp);
        }
        else {
            throw new IllegalArgumentException(criteria
                    + " is not a valid argument for the select method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw = client.selectKeysCcl(
                    Collections.toList(keys), ccl, creds, transaction,
                    environment);
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeysCclTimestr(Collections.toList(keys), ccl,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.selectKeysCclTime(Collections.toList(keys), ccl,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw = client.selectCriteria(
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw;
            if(timestamp.isString()) {
                raw = client.selectCriteriaTimestr(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.selectCriteriaTime(
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public Map<String, Set<Object>> select(long record) {
        return execute(() -> {
            Map<String, Set<TObject>> raw = client.selectRecord(record, creds,
                    transaction, environment);
            Map<String, Set<Object>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Key", "Values");
            for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(), Transformers.transformSetLazily(
                        entry.getValue(), Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public Map<String, Set<Object>> select(long record, Timestamp timestamp) {
        return execute(() -> {
            Map<String, Set<TObject>> raw;
            if(timestamp.isString()) {
                raw = client.selectRecordTimestr(record, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.selectRecordTime(record, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<String, Set<Object>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Key", "Values");
            for (Entry<String, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(), Transformers.transformSetLazily(
                        entry.getValue(), Conversions.thriftToJava()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Object criteria) {
        if(criteria instanceof BuildableState) {
            return select(((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Object criteria,
            Timestamp timestamp) {
        if(criteria instanceof BuildableState) {
            return select(((BuildableState) criteria).build(), timestamp);
        }
        else {
            throw new IllegalArgumentException(
                    criteria + " is not a valid argument for the get method");
        }
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw = client.selectCcl(ccl,
                    creds, transaction, environment);
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw = client.selectKeyRecords(key,
                    Collections.toLongList(records), creds, transaction,
                    environment);
            Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeyRecordsTimestr(key,
                        Collections.toLongList(records), timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.selectKeyRecordsTime(key,
                        Collections.toLongList(records), timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw = client.selectKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria), creds,
                    transaction, environment);
            Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeyCriteriaTimestr(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                raw = client.selectKeyCriteriaTime(key,
                        Language.translateToThriftCriteria(criteria),
                        timestamp.getMicros(), creds, transaction, environment);
            }
            Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Set<T> select(String key, long record) {
        return execute(() -> {
            Set<TObject> values = client.selectKeyRecord(key, record, creds,
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
                values = client.selectKeyRecordTimestr(key, record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                values = client.selectKeyRecordTime(key, record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
            return Transformers.transformSetLazily(values,
                    Conversions.<T> thriftToJavaCasted());
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Object criteria) {
        if(criteria instanceof BuildableState) {
            return select(key, ((BuildableState) criteria).build());
        }
        else {
            throw new IllegalArgumentException(criteria
                    + " is not a valid argument for the select method");
        }
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Object criteria,
            Timestamp timestamp) {
        if(criteria instanceof BuildableState) {
            return select(key, ((BuildableState) criteria).build(), timestamp);
        }
        else {
            throw new IllegalArgumentException(criteria
                    + " is not a valid argument for the select method");
        }
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw = client.selectKeyCcl(key, ccl, creds,
                    transaction, environment);
            Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Set<TObject>> raw;
            if(timestamp.isString()) {
                raw = client.selectKeyCclTimestr(key, ccl, timestamp.toString(),
                        creds, transaction, environment);
            }
            else {
                raw = client.selectKeyCclTime(key, ccl, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            Map<Long, Set<T>> pretty = PrettyLinkedHashMap
                    .newPrettyLinkedHashMap("Record", key);
            for (Entry<Long, Set<TObject>> entry : raw.entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformSetLazily(entry.getValue(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp) {
        return execute(() -> {
            Map<Long, Map<String, Set<TObject>>> raw;
            if(timestamp.isString()) {
                raw = client.selectCclTimestr(ccl, timestamp.toString(), creds,
                        transaction, environment);
            }
            else {
                raw = client.selectCclTime(ccl, timestamp.getMicros(), creds,
                        transaction, environment);
            }
            Map<Long, Map<String, Set<T>>> pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            for (Entry<Long, Map<String, Set<TObject>>> entry : raw
                    .entrySet()) {
                pretty.put(entry.getKey(),
                        Transformers.transformMapSet(entry.getValue(),
                                Conversions.<String> none(),
                                Conversions.<T> thriftToJavaCasted()));
            }
            return pretty;
        });
    }

    @Override
    public void set(String key, Object value, Collection<Long> records) {
        execute(() -> {
            client.setKeyValueRecords(key, Convert.javaToThrift(value),
                    Collections.toLongList(records), creds, transaction,
                    environment);
            return null;
        });
    }

    @Override
    public <T> void set(String key, T value, long record) {
        execute(() -> {
            client.setKeyValueRecord(key, Convert.javaToThrift(value), record,
                    creds, transaction, environment);
            return null;
        });
    }

    @Override
    public void stage() throws TransactionException {
        execute(() -> {
            transaction = client.stage(creds, environment);
            return null;
        });
    }

    @Override
    public Timestamp time() {
        return execute(() -> {
            return Timestamp
                    .fromMicros(client.time(creds, transaction, environment));
        });
    }

    @Override
    public Timestamp time(String phrase) {
        return execute(() -> {
            return Timestamp.fromMicros(
                    client.timePhrase(phrase, creds, transaction, environment));
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
            return client.verifyKeyValueRecord(key, Convert.javaToThrift(value),
                    record, creds, transaction, environment);
        });
    }

    @Override
    public boolean verify(String key, Object value, long record,
            Timestamp timestamp) {
        return execute(() -> {
            if(timestamp.isString()) {
                return client.verifyKeyValueRecordTimestr(key,
                        Convert.javaToThrift(value), record,
                        timestamp.toString(), creds, transaction, environment);
            }
            else {
                return client.verifyKeyValueRecordTime(key,
                        Convert.javaToThrift(value), record,
                        timestamp.getMicros(), creds, transaction, environment);
            }
        });
    }

    @Override
    public boolean verifyAndSwap(String key, Object expected, long record,
            Object replacement) {
        return execute(() -> {
            return client.verifyAndSwap(key, Convert.javaToThrift(expected),
                    record, Convert.javaToThrift(replacement), creds,
                    transaction, environment);
        });
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        execute(() -> {
            client.verifyOrSet(key, Convert.javaToThrift(value), record, creds,
                    transaction, environment);
            return null;
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
     * Authenticate the {@link #username} and {@link #password} and populate
     * {@link #creds} with the appropriate AccessToken.
     */
    private void authenticate() {
        try {
            creds = client.login(ClientSecurity.decrypt(username),
                    ClientSecurity.decrypt(password), environment);
        }
        catch (TException e) {
            throw Throwables.propagate(e);
        }
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
        final List<TObject> tValues = Lists.transform(
                Lists.newArrayList(values), Conversions.javaToThrift());
        return execute(() -> {
            if(operator instanceof Operator) {
                return client.findKeyOperatorValues(key, (Operator) operator,
                        tValues, creds, transaction, environment);
            }
            else {
                return client.findKeyOperatorstrValues(key, operator.toString(),
                        tValues, creds, transaction, environment);
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
        final List<TObject> tValues = Lists.transform(
                Lists.newArrayList(values), Conversions.javaToThrift());
        return execute(() -> {
            if(operator instanceof Operator) {
                return client.findKeyOperatorValuesTime(key,
                        (Operator) operator, tValues, timestamp.getMicros(),
                        creds, transaction, environment);
            }
            else {
                return client.findKeyOperatorstrValuesTime(key,
                        operator.toString(), tValues, timestamp.getMicros(),
                        creds, transaction, environment);
            }
        });
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
        catch (com.cinchapi.concourse.thrift.ParseException e) {
            throw new ParseException(e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the thrift RPC client.
     * 
     * @return the {@link ConcourseService#Client}
     */
    ConcourseService.Client thrift() {
        return client;
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
     * Return the current {@link TransactionToken}.
     * 
     * @return the transaction token
     */
    @Nullable
    TransactionToken transaction() {
        return transaction;
    }

    /**
     * Return the environment to which the driver is connected.
     * 
     * @return the environment
     */
    String environment() {
        return environment;
    }

}

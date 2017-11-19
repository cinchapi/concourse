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
package com.cinchapi.mockcourse;

import com.cinchapi.concourse.thrift.ConcourseService;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.nio.charset.StandardCharsets;

import org.apache.thrift.TException;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ConcourseService;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.ParseException;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.DuplicateEntryException;
import com.cinchapi.concourse.thrift.TransactionException;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.thrift.Type;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

import com.cinchapi.concourse.util.Version;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;

import groovy.json.JsonSlurper;
import groovy.json.JsonOutput;

/**
 * An in-memory implementation of ConcourseServer to use as a mock server
 * in unit tests.
 *
 * MOCKOURSE IS NOT SUITABLE FOR PRODUCTION!!!
 *
 * Mockcourse is designed to be used as in unit tests for client drivers. As
 * such this implementation is likely incomplete and somewhat buggy. When unit
 * testing client drivers, we are not looking to validate server side
 * functionality, instead we are only looking to show that the driver is able to * correctly send data over the wire and recieve the same data back.
 *
 * @author jnelson
 */
 @GrabConfig(systemClassLoader= true)
 @Grapes([
 @Grab('org.apache.thrift:libthrift:0.9.3'),
 @Grab('org.slf4j:slf4j-api:1.7.5'),
 @Grab('org.slf4j:slf4j-nop:1.7.12'),
 @Grab('org.codehaus.groovy:groovy-json:2.4.3'),
 @Grab('com.google.guava:guava:19.0')
 ])
class Mockcourse implements ConcourseService.Iface {

    /**
     * The port on which Mockcourse listens.
     */
    static int PORT = 1818;

    /**
     * Run the program...
     */
    public static main(args) {
        Mockcourse mockcourse = null;
        try{
          // Attempt to get a port number passed in as an argument
          mockcourse = args.length > 0 ? new Mockcourse(Integer.parseInt(args[0])) : new Mockcourse();
        }
        catch(NumberFormatException e){
          mockcourse = new Mockcourse();
        }
        mockcourse.start();
    }

    /**
     * The thrift server.
     */
    private TServer server;

    /**
     * A fake access token to return from the "login" method since Mockcourse
     * does not perform real auth.
     */
    private AccessToken fakeAccessToken;

    /**
     * A fake transaction token to return from the "stage" method since
     * Mockcourse does not support multiple concurrent transactions.
     */
    private TransactionToken fakeTransactionToken;

    /**
     * The release version for Mockcourse
     */
    private String version = Version.getVersion(Concourse.class).toString();

    /**
     * An append only list of writes that represents the data that is stored in
     * Mockcourse.
     */
    private List<Write> writes = new ArrayList<Write>();

    /**
     * The start timestamp of the transaction that is in process.
     */
    private Long txnStart = null;

    /**
     * A parser for JSON data.
     */
    private JsonSlurper jsonParser = new JsonSlurper();

    /**
     * A fake value to return from methods that aren't implemented.
     */
    private final TObject fakeValue;

    /**
     * Construct a new instance.
     *
     * @param port
     */
    public Mockcourse(int port){
      TServerSocket socket = new TServerSocket(port);
      ConcourseService.Processor<ConcourseService.Iface> processor = new ConcourseService.Processor<ConcourseService.Iface>(
              this);
      TServer.Args args = new TServer.Args(socket);
      args.processor(processor);
      this.server = new TSimpleServer(args);

      // Create a fake AccessToken
      ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.putInt(1);
      buffer.rewind();
      this.fakeAccessToken = new AccessToken(buffer);

      // Create a fake TransactionToken
      this.fakeTransactionToken = new TransactionToken(fakeAccessToken,
              Time.now());

      // Create the fake value
      ByteBuffer data = ByteBuffer.allocate(4);
      data.putInt(17);
      data.rewind();
      this.fakeValue = new TObject(data, Type.INTEGER);
    }

    /**
     * Construct a new instance
     */
    public Mockcourse() {
        this(PORT)
    }

    /**
     * Start the server
     */
    public void start() {
    println "Mockcourse is now running...";
    server.serve();
  }

    @Override
    public void abort(AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        if(txnStart != null) {
            ListIterator<Write> lit = writes.listIterator(writes.size());
            while (lit.hasPrevious()) {
                Write write = lit.previous();
                if(write.timestamp >= txnStart) {
                    lit.remove();
                }
            }
            txnStart = null;
        }
    }

    @Override
    public boolean commit(AccessToken creds, TransactionToken transaction,
            String environment) throws SecurityException,
            TransactionException, TException {
        txnStart = null;
        return true;
    }

    @Override
    public AccessToken login(ByteBuffer username, ByteBuffer password,
            String environment) throws SecurityException, TException {
        return fakeAccessToken;
    }

    @Override
    public void logout(AccessToken token, String environment)
            throws SecurityException, TException {
        writes.clear();
    }

    @Override
    public TransactionToken stage(AccessToken token, String environment)
            throws TException {
        txnStart = Time.now();
        return fakeTransactionToken;
    }

    @Override
    public boolean addKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        if(!selectKeyRecord(key, record, creds, transaction, environment)
                .contains(value)) {
            Write write = new Write(key, value, record, WriteType.ADD);
            writes.add(write);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public long addKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        long record = Time.now();
        addKeyValueRecord(key, value, record, creds, transaction, environment);
        return record;
    }

    @Override
    public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, Boolean> result = new HashMap<Long, Boolean>();
        for (long record : records) {
            result.put(
                    record,
                    addKeyValueRecord(key, value, record, creds, transaction,
                            environment));
        }
        return result;
    }

    @Override
    public void clearRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Set<String> keys = describeRecord(record, creds, transaction,
                environment);
        for (String key : keys) {
            clearKeyRecord(key, record, creds, transaction, environment);
        }
    }

    @Override
    public void clearRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        for (long record : records) {
            clearRecord(record, creds, transaction, environment);
        }
    }

    @Override
  public void clearKeyRecord(String key, long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Set<TObject> values = selectKeyRecord(key, record, creds, transaction, environment);
      for(TObject value : values){
        removeKeyValueRecord(key, value, record, creds, transaction, environment)
      }
  }

    @Override
    public void clearKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        for (String key : keys) {
            clearKeyRecord(key, record, creds, transaction, environment);
        }
    }

    @Override
    public void clearKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        for (long record : records) {
            clearKeyRecord(key, record, creds, transaction, environment);
        }
    }

    @Override
    public void clearKeysRecords(List<String> keys, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        for (long record : records) {
            for (String key : keys) {
                clearKeyRecord(key, record, creds, transaction, environment);
            }
        }
    }

    /**
     * Insert the {@code data} into {@code record}
     *
     * @param data
     * @param record
     * @return {@code true} if all the data is successfully added
     */
    private boolean doInsert(Map<String, Object> data, long record, AccessToken creds, TransactionToken transaction, String environment){
    Iterator<Map.Entry<String, Object>> it = data.entrySet().iterator();
    boolean allGood = true;
    while(it.hasNext()){
      Map.Entry<String, Object> entry = it.next();
      String key = entry.getKey();
      Object value = entry.getValue();
      ByteBuffer bytes = null;
      Type type = null;
      if(value instanceof List){
        for(Object item : value){
          Map<String, Object> data0 = new HashMap<String, Object>();
          data0.put(key, item);
          if(!doInsert(data0, record, creds, transaction, environment)){
            allGood = false;
          }
        }
        continue;
      }
      else if(value instanceof Boolean) {
        bytes = ByteBuffer.allocate(1);
        Integer num = new Integer(value ? 1 : 0);
        bytes.put(num.byteValue());
        type = Type.BOOLEAN;
      }
      else if(value instanceof BigDecimal) {
        Map<String, Object> data0 = new HashMap<String, Object>();
        data0.put(key, value.doubleValue());
        if(!doInsert(data0, record, creds, transaction, environment)){
          allGood = false
        }
        continue;
      }
      else if(value instanceof Double) {
        bytes = ByteBuffer.allocate(8);
        bytes.putDouble((double) value);
        type = Type.DOUBLE;
      }
      else if(value instanceof Float) {
        bytes = ByteBuffer.allocate(4);
        bytes.putFloat((float) value);
        type = Type.FLOAT;
      }
      else if(value instanceof Link) {
        bytes = ByteBuffer.allocate(8);
        bytes.putLong(((Link) value).longValue());
        type = Type.LINK;
      }
      else if(value instanceof Long) {
        bytes = ByteBuffer.allocate(8);
        bytes.putLong((long) value);
        type = Type.LONG;
      }
      else if(value instanceof Integer) {
        bytes = ByteBuffer.allocate(4);
        bytes.putInt((int) value);
        type = Type.INTEGER;
      }
      else if(value instanceof Tag) {
        bytes = ByteBuffer.wrap(value.toString().getBytes(
          StandardCharsets.UTF_8));
        type = Type.TAG;
      }
      else if(value.toString().matches('^@[-]{0,1}[0-9]+$')){
        value = Long.parseLong(value.toString().substring(1,
          value.toString().length()));
        bytes = ByteBuffer.allocate(8);
        bytes.putLong((long) value);
        type = Type.LINK;
      }
      else if(value.toString().matches('^@.+@$')){
          String ccl = value.toString().substring(1, value.toString().length() - 1)
          Set<Long> items = findCcl(ccl, creds, transaction, environment);
          for(long item : items){
              Map<String, Object> data0 = new HashMap<String, Object>();
              data0.put(key, Link.to(item));
              if(!doInsert(data0, record, creds, transaction, environment)){
                allGood = false;
              }
              continue;
          }
      }
      else {
        bytes = ByteBuffer.wrap(value.toString().getBytes(
          StandardCharsets.UTF_8));
        type = Type.STRING;
      }
      if(bytes != null){
          bytes.rewind();
          if(!addKeyValueRecord(key, new TObject(bytes, type), record, creds, transaction, environment)){
              allGood = false;
          }
      }
    }
    return allGood;
  }

    @Override
    public Set<Long> insertJson(String json, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Set<Long> records = new HashSet<Long>();
        def data = jsonParser.parseText(json);
        if(data instanceof List) {
            for (Map<String, Object> item : data) {
                long record = Time.now();
                doInsert(item, record, creds, transaction, environment);
                records.add(record);
            }
        }
        else if(data instanceof Map) {
            long record = Time.now();
            doInsert(data, record, creds, transaction, environment);
            records.add(record);
        }
        else {
            throw new Exception("Error parsing JSON");
        }
        return records;
    }

    @Override
    public boolean insertJsonRecord(String json, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        def data = jsonParser.parseText(json);
        if(data instanceof Map) {
            stage(creds, environment);
            if(doInsert(data, record, creds, transaction, environment)) {
                commit(creds, transaction, environment);
                return true;
            }
            else {
                abort(creds, transaction, environment);
                return false;
            }
        }
        else {
            return false;
        }
    }

    @Override
    public Map<Long, Boolean> insertJsonRecords(String json,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, Boolean> data = new LinkedHashMap<Long, Boolean>();
        for (long record : records) {
            data.put(
                    record,
                    insertJsonRecord(json, record, creds, transaction,
                            environment));
        }
        return data;
    }

    @Override
    public TObject invokePlugin(String clazz, String method, List<TObject> params, AccessToken creds, TransactionToken transaction, String environment) throws TException {
        ByteBuffer data = ByteBuffer.allocate(4);
        data.putInt(params.size());
        data.rewind();
        return new TObject(data, Type.INTEGER);
    }

    @Override
    public boolean removeKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        if(selectKeyRecord(key, record, creds, transaction, environment)
                .contains(value)) {
            Write write = new Write(key, value, record, WriteType.REMOVE);
            writes.add(write);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, Boolean> data = new HashMap<Long, Boolean>();
        for (long record : records) {
            data.put(
                    record,
                    removeKeyValueRecord(key, value, record, creds,
                            transaction, environment));
        }
        return data;
    }

    @Override
    public void setKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        clearKeyRecord(key, record, creds, transaction, environment);
        addKeyValueRecord(key, value, record, creds, transaction, environment);
    }

    @Override
    public long setKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        long record = Time.now();
        setKeyValueRecord(key, value, record, creds, transaction, environment);
        return record;
    }

    @Override
  public void setKeyValueRecords(String key, TObject value,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      for(long record : records){
        setKeyValueRecord(key, value, record, creds, transaction, environment)
      }
  }

    @Override
    public Set<Long> inventory(AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Set<Long> records = new HashSet<Long>();
        for (Write write : writes) {
            records.add(write.record);
        }
        return records;
    }

    @Override
    public Map<String, Set<TObject>> selectRecord(long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectRecordTime(record, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectRecords(
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, Map<String, Set<TObject>>> data = new LinkedHashMap<Long, Map<String, Set<TObject>>>();
        for (long record : records) {
            data.put(record,
                    selectRecord(record, creds, transaction, environment));
        }
        return data;
    }

    @Override
    public Map<String, Set<TObject>> selectRecordTime(long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Map<String, Set<TObject>> data = new HashMap<String, Set<TObject>>();
        for (Write write : writes) {
            if(write.timestamp > timestamp) {
                break;
            }
            if(write.record == record) {
                Set<TObject> values = data.get(write.key);
                if(values == null) {
                    values = new LinkedHashSet<TObject>();
                    data.put(write.key, values);
                }
                if(write.type == WriteType.ADD) {
                    values.add(write.value);
                }
                else {
                    values.remove(write.value);
                    if(values.isEmpty()) {
                        data.remove(write.key);
                    }
                }
            }
        }
        return data;
    }

    @Override
    public Map<String, Set<TObject>> selectRecordTimestr(long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectRecordTime(record, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTime(
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, Map<String, Set<TObject>>> data = new LinkedHashMap<Long, Map<String, Set<TObject>>>();
        for (long record : records) {
            Map<String, Set<TObject>> recordData = selectRecordTime(record,
                    timestamp, creds, transaction, environment);
            if(!recordData.isEmpty()) {
                data.put(record, recordData);
            }
        }
        return data;
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestr(
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectRecordsTime(records, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<TObject, Set<Long>> browseKey(String key, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return browseKeyTime(key, Time.now(), creds, transaction, environment);
    }

    @Override
    public Map<String, Map<TObject, Set<Long>>> browseKeys(List<String> keys,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<String, Map<TObject, Set<Long>>> data = new LinkedHashMap<String, Map<TObject, Set<Long>>>();
        for (String key : keys) {
            Map<TObject, Set<Long>> map = browseKey(key, creds, transaction,
                    environment);
            if(!map.isEmpty()) {
                data.put(key, map);
            }
        }
        return data;
    }

    @Override
    public Map<TObject, Set<Long>> browseKeyTime(String key, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<TObject, Set<Long>> data = new TreeMap<TObject, Set<Long>>();
        for (Write write : writes) {
            if(write.timestamp > timestamp) {
                break;
            }
            if(write.key.equals(key)) {
                Set<Long> records = data.get(write.value);
                if(records == null) {
                    records = new HashSet<Long>();
                    data.put(write.value, records);
                }
                if(write.type == WriteType.ADD) {
                    records.add(write.record);
                }
                else {
                    records.remove(write.record);
                    if(records.isEmpty()) {
                        data.remove(write.value, records);
                    }
                }
            }
        }
        return data;
    }

    @Override
    public Map<TObject, Set<Long>> browseKeyTimestr(String key,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return browseKeyTime(key, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<String, Map<TObject, Set<Long>>> browseKeysTime(
            List<String> keys, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<String, Map<TObject, Set<Long>>> data = new LinkedHashMap<String, Map<TObject, Set<Long>>>();
        for (String key : keys) {
            Map<TObject, Set<Long>> map = browseKeyTime(key, timestamp, creds,
                    transaction, environment);
            if(!map.isEmpty()) {
                data.put(key, map);
            }
        }
        return data;
    }

    @Override
    public Map<String, Map<TObject, Set<Long>>> browseKeysTimestr(
            List<String> keys, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return browseKeysTime(keys, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Set<String> describeRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return describeRecordTime(record, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Set<String> describeRecordTime(long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectRecordTime(record, timestamp, creds, transaction,
                environment).keySet();
    }

    @Override
    public Set<String> describeRecordTimestr(long record, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return describeRecordTime(record, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Set<String>> describeRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return describeRecordsTime(records, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws SecurityException,
            TransactionException, TException {
        Map<Long, Set<String>> data = new LinkedHashMap<Long, Set<String>>();
        for (long record : records) {
            Set<String> set = describeRecordTime(record, timestamp, creds,
                    transaction, environment);
            if(!set.isEmpty()) {
                data.put(record, set);
            }
        }
        return data;
    }

    @Override
    public Map<Long, Set<String>> describeRecordsTimestr(List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return describeRecordsTime(records, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public Set<TObject> selectKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyRecordTime(key, record, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Set<TObject> selectKeyRecordTime(String key, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Set<TObject> values = new LinkedHashSet<TObject>();
        for (Write write : writes) {
            if(write.timestamp > timestamp) {
                break;
            }
            if(write.key.equals(key) && write.record == record) {
                if(write.type == WriteType.ADD) {
                    values.add(write.value);
                }
                else {
                    values.remove(write.value);
                }
            }
        }
        return values;
    }

    @Override
    public Set<TObject> selectKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyRecordTime(key, record, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public Map<String, Set<TObject>> selectKeysRecord(List<String> keys,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeysRecordTime(keys, record, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public Map<String, Set<TObject>> selectKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<String, Set<TObject>> data = new HashMap<String, Set<TObject>>();
        for (String key : keys) {
            Set<TObject> values = selectKeyRecordTime(key, record, timestamp,
                    creds, transaction, environment);
            if(!values.isEmpty()) {
                data.put(key, values);
            }
        }
        return data;
    }

    @Override
    public Map<String, Set<TObject>> selectKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectKeysRecordTime(keys, record,
                Parser.parseMicros(timestamp), creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecords(
            List<String> keys, List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectKeysRecordsTime(keys, records, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyRecords(String key,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectKeyRecordsTime(key, records, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyRecordsTime(String key,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, Set<TObject>> data = new LinkedHashMap<Long, Set<TObject>>();
        for (long record : records) {
            Set<TObject> values = selectKeyRecordTime(key, record, timestamp,
                    creds, transaction, environment);
            if(!values.isEmpty()) {
                data.put(record, values);
            }
        }
        return data;
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectKeyRecordsTime(key, records,
                Parser.parseMicros(timestamp), creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<Long, Map<String, Set<TObject>>> data = new LinkedHashMap<Long, Map<String, Set<TObject>>>();
        for (long record : records) {
            Map<String, Set<TObject>> map = selectKeysRecordTime(keys, record,
                    timestamp, creds, transaction, environment);
            if(!map.isEmpty()) {
                data.put(record, map);
            }
        }
        return data;
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeysRecordsTime(keys, records,
                Parser.parseMicros(timestamp), creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCriteria(
            TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCcl(String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectCclTime(ccl, Time.now(), creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTime(
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        List<Long> records = new ArrayList<Long>(findCcl(ccl, creds,
                transaction, environment));
        return selectRecordsTime(records, timestamp, creds, transaction,
                environment);
    }

    @Override
  public Map<Long, Map<String, Set<TObject>>> selectCclTimestr(String ccl,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TException {
      return selectCclTime(ccl, Parser.parseMicros(timestamp), creds, transaction, environment)
  }

    @Override
    public Map<Long, Set<TObject>> selectKeyCriteria(String key,
            TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return selectKeyCclTime(key, ccl, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        List<Long> records = new ArrayList<Long>(findCcl(ccl, creds,
                transaction, environment));
        return selectKeyRecordsTime(key, records, timestamp, creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Set<TObject>> selectKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return selectKeyCclTime(key, ccl, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteria(
            List<String> keys, TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCcl(
            List<String> keys, String ccl, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectKeysCclTime(keys, ccl, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTime(
            List<String> keys, String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        List<Long> records = new ArrayList<Long>(findCcl(ccl, creds,
                transaction, environment));
        return selectKeysRecordsTime(keys, records, timestamp, creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestr(
            List<String> keys, String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return selectKeysCclTime(keys, ccl, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public TObject getKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Set<TObject> values = selectKeyRecord(key, record, creds, transaction,
                environment);
        return Iterables.getLast(values, TObject.NULL);
    }

    @Override
    public TObject getKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Set<TObject> values = selectKeyRecordTime(key, record, timestamp,
                creds, transaction, environment);
        return Iterables.getLast(values, TObject.NULL);
    }

    @Override
    public TObject getKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyRecordTime(key, record, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public Map<String, TObject> getKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordTime(keys, record, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<String, TObject> getKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<String, TObject> data = new HashMap<String, TObject>();
        for (String key : keys) {
            TObject value = Iterables.getLast(
                    selectKeyRecordTime(key, record, timestamp, creds,
                            transaction, environment), TObject.NULL);
            data.put(key, value);
        }
        return data;
    }

    @Override
    public Map<String, TObject> getKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return getKeysRecordTime(keys, record, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysRecords(List<String> keys,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return getKeysRecordsTime(keys, records, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, TObject> getKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyRecordsTime(key, records, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, TObject> getKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Map<Long, TObject> data = new LinkedHashMap<Long, TObject>();
        for (long record : records) {
            TObject value = getKeyRecordTime(key, record, timestamp, creds,
                    transaction, environment);
            data.put(record, value);
        }
        return data;
    }

    @Override
    public Map<Long, TObject> getKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return getKeyRecordsTime(key, records, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<Long, Map<String, TObject>> data = new LinkedHashMap<Long, Map<String, TObject>>();
        for (long record : records) {
            Map<String, TObject> map = getKeysRecordTime(keys, record,
                    timestamp, creds, transaction, environment);
            if(!map.isEmpty()) {
                data.put(record, map);
            }
        }
        return data;
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeysRecordsTime(keys, records, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public Map<Long, TObject> getKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getCriteria(TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getCcl(String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getCclTime(ccl, Time.now(), creds, transaction, environment);
    }

    @Override
    public Map<Long, Map<String, TObject>> getCriteriaTime(TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws SecurityException,
            TransactionException, TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        String[] toks = ccl.split(" ");
        Criteria criteria = new Criteria(toks[0], toks[1], toks[2]);
        Set<Long> records = findKeyOperatorValues(criteria.key,
                criteria.operator, criteria.values, creds, transaction,
                environment);
        Map<Long, Map<String, TObject>> data = new HashMap<Long, Map<String, TObject>>();
        for (long record : records) {
            Set<String> keys = describeRecordTime(record, timestamp, creds,
                    transaction, environment);
            Map<String, TObject> entry = new HashMap<String, TObject>();
            for (String key : keys) {
                TObject value = getKeyRecordTime(key, record, timestamp, creds,
                        transaction, environment);
                entry.put(key, value);
            }
            data.put(record, entry);
        }
        return data;
    }

    @Override
  public Map<Long, Map<String, TObject>> getCclTimestr(String ccl,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws SecurityException,
          TransactionException, ParseException, TException {
      return getCclTime(ccl, Parser.parseMicros(timestamp), creds, transaction, environment)
  }

    @Override
    public Map<Long, TObject> getKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return getKeyCclTime(key, ccl, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, TObject> getKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, TObject> getKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, TObject> getKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        List<Long> records = new ArrayList<Long>(findCcl(ccl, creds,
                transaction, environment));
        return getKeyRecordsTime(key, records, timestamp, creds, transaction,
                environment);
    }

    @Override
    public Map<Long, TObject> getKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeyCclTime(key, ccl, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysCriteria(List<String> keys,
            TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysCcl(List<String> keys,
            String ccl, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return getKeysCclTime(keys, ccl, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {

        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysCclTime(List<String> keys,
            String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        List<Long> records = new ArrayList<Long>(findCcl(ccl, creds,
                transaction, environment));
        return getKeysRecordsTime(keys, records, timestamp, creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Map<String, TObject>> getKeysCclTimestr(List<String> keys,
            String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return getKeysCclTime(keys, ccl, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public boolean verifyKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return verifyKeyValueRecordTime(key, value, record, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public boolean verifyKeyValueRecordTime(String key, TObject value,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        boolean result = false;
        for (Write write : writes) {
            if(write.timestamp > timestamp) {
                break;
            }
            if(write.key.equals(key) && write.value.equals(value)
                    && write.record == write.record) {
                result = !result;
            }
        }
        return result;
    }

    @Override
    public boolean verifyKeyValueRecordTimestr(String key, TObject value,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return verifyKeyValueRecordTime(key, value, record,
                Parser.parseMicros(timestamp), creds, transaction, environment);
    }

    @Override
    public String jsonifyRecords(List<Long> records, boolean identifier,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return jsonifyRecordsTime(records, Time.now(), identifier, creds,
                transaction, environment);
    }

    @Override
  public String jsonifyRecordsTime(List<Long> records, long timestamp,
          boolean identifier, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      List<Map<String, Set<Object>>> data = new ArrayList<Map<String, Set<Object>>>();
      for(long record : records){
        Map<String, Set<TObject>> stored = selectRecordTime(record, timestamp, creds, transaction, environment);
        Map<String, Set<Object>> entry = new HashMap<String, Set<Object>>();
        for(String key : stored.keySet()){
          Set<TObject> values = stored.get(key);
          if(!values.isEmpty()){
            Set<Object> converted = new HashSet<Object>();
            for(TObject value : values){
              Object obj = TObjects.toInt(value);
              converted.add(obj);
            }
            entry.put(key, converted);
          }
        }
        if(identifier){
          entry.put('$id$', record);
        }
        data.add(entry);
      }
      return JsonOutput.toJson(data);
  }

    @Override
    public String jsonifyRecordsTimestr(List<Long> records, String timestamp,
            boolean identifier, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return jsonifyRecordsTime(records, Parser.parseMicros(timestamp),
                identifier, creds, transaction, environment);
    }

    @Override
    public Set<Long> findCriteria(TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Set<Long> fakeResults = new HashSet<Long>();
        fakeResults.add(18L);
        return fakeResults;
    }

    @Override
  public Set<Long> findCcl(String ccl, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      if(ccl.equals("throw parse exception")){
        throw new ParseException("This is a fake parse exception")
      }
      else if(ccl.equals("throw transaction exception")){
          throw new TransactionException()
      }
      else{
        String[] toks = ccl.split(" ");
        Criteria criteria = new Criteria(toks[0], toks[1], toks[2]);
        return findKeyOperatorValues(criteria.key, criteria.operator, criteria.values, creds, transaction, environment);
      }
  }

    @Override
    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return findKeyOperatorValuesTime(key, operator, values, Time.now(),
                creds, transaction, environment);
    }

    @Override
  public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
          List<TObject> values, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Set<Long> records = new HashSet<Long>();
      int me = TObjects.toInt(values.get(0));
      for(Write write : writes){
        if(write.timestamp > timestamp){
          break;
        }
        int stored = TObjects.toInt(write.value)
        if(operator == Operator.EQUALS && stored == me){
          records.add(write.record);
        }
        else if(operator == Operator.NOT_EQUALS && stored != me){
          records.add(write.record);
        }
        else if(operator == Operator.GREATER_THAN && stored > me){
          records.add(write.record);
        }
        else if(operator == Operator.GREATER_THAN_OR_EQUALS && stored >= me){
          records.add(write.record);
        }
        else if(operator == Operator.LESS_THAN && stored < me){
          records.add(write.record);
        }
        else if(operator == Operator.LESS_THAN_OR_EQUALS && stored <= me){
          records.add(write.record);
        }
        else{
          if(operator == Operator.BETWEEN){
            int me2 = TObjects.toInt(values.get(1));
            if(stored >= me && stored < me2 ){
              records.add(write.record);
            }
          }
          else{
            continue;
          }
        }
      }
      return records;
  }

    @Override
    public Set<Long> findKeyOperatorValuesTimestr(String key,
            Operator operator, List<TObject> values, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorValuesTime(key, operator, values,
                Parser.parseMicros(timestamp), creds, transaction, environment);
    }

    @Override
    public Set<Long> findKeyOperatorstrValues(String key, String operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return findKeyOperatorValues(key, Parser.parseOperator(operator),
                values, creds, transaction, environment);
    }

    @Override
    public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return findKeyOperatorValuesTime(key, Parser.parseOperator(operator),
                values, timestamp, creds, transaction, environment);
    }

    @Override
    public Set<Long> findKeyOperatorstrValuesTimestr(String key,
            String operator, List<TObject> values, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return findKeyOperatorstrValuesTime(key, operator, values,
                Parser.parseMicros(timestamp), creds, transaction, environment);
    }

    @Override
    public Set<Long> search(String key, String query, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<TObject, Set<Long>> data = browseKey(key, creds, transaction,
                environment);
        Set<Long> matches = new HashSet<Long>();
        for (TObject value : data.keySet()) {
            if(value.type == Type.STRING || value.type == Type.TAG) {
                CharBuffer cbuf = StandardCharsets.UTF_8.decode(value
                        .bufferForData());
                String text = cbuf.toString();
                if(text.contains(query)) {
                    matches.addAll(data.get(value));
                }
            }
        }
        return matches;
    }

    @Override
    public Map<Long, String> auditRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, String> audit = new LinkedHashMap<Long, String>();
        for (Write write : writes) {
            if(write.record == record) {
                audit.put(write.timestamp, write.toString());
            }
        }
        return audit;
    }

    @Override
    public Map<Long, String> auditRecordStart(long record, long start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<Long, String> audit = new LinkedHashMap<Long, String>();
        for (Write write : writes) {
            if(write.timestamp < start) {
                continue;
            }
            if(write.record == record) {
                audit.put(write.timestamp, write.toString());
            }
        }
        return audit;
    }

    @Override
    public Map<Long, String> auditRecordStartstr(long record, String start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return auditRecordStart(record, Parser.parseMicros(start), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, String> auditRecordStartEnd(long record, long start,
            long tend, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Map<Long, String> audit = new LinkedHashMap<Long, String>();
        for (Write write : writes) {
            if(write.timestamp < start) {
                continue;
            }
            else if(write.timestamp > tend) {
                break;
            }
            if(write.record == record) {
                audit.put(write.timestamp, write.toString());
            }
        }
        return audit;
    }

    @Override
    public Map<Long, String> auditRecordStartstrEndstr(long record,
            String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return auditRecordStartEnd(record, Parser.parseMicros(start),
                Parser.parseMicros(tend), creds, transaction, environment);
    }

    @Override
    public Map<Long, String> auditKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<Long, String> audit = new LinkedHashMap<Long, String>();
        for (Write write : writes) {
            if(write.record == record && write.key.equals(key)) {
                audit.put(write.timestamp, write.toString());
            }
        }
        return audit;
    }

    @Override
    public Map<Long, String> auditKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Map<Long, String> audit = new LinkedHashMap<Long, String>();
        for (Write write : writes) {
            if(write.timestamp < start) {
                continue;
            }
            else if(write.record == record && write.key.equals(key)) {
                audit.put(write.timestamp, write.toString());
            }
        }
        return audit;
    }

    @Override
    public Map<Long, String> auditKeyRecordStartstr(String key, long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return auditKeyRecordStart(key, record, Parser.parseMicros(start),
                creds, transaction, environment);
    }

    @Override
    public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
            long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<Long, String> audit = new LinkedHashMap<Long, String>();
        for (Write write : writes) {
            if(write.timestamp < start) {
                continue;
            }
            else if(write.timestamp > tend) {
                break;
            }
            if(write.record == record && write.key.equals(key)) {
                audit.put(write.timestamp, write.toString());
            }
        }
        return audit;
    }

    @Override
    public Map<Long, String> auditKeyRecordStartstrEndstr(String key,
            long record, String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return auditKeyRecordStartEnd(key, record, Parser.parseMicros(start),
                Parser.parseMicros(tend), creds, transaction, environment);
    }

    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecord(String key,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return chronologizeKeyRecordStart(key, record, 0, creds, transaction,
                environment);
    }

    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStart(String key,
            long record, long start, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return chronologizeKeyRecordStartEnd(key, record, start, Time.now(),
                creds, transaction, environment);
    }

    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return chronologizeKeyRecordStart(key, record,
                Parser.parseMicros(start), creds, transaction, environment);
    }

    @Override
  public Map<Long, Set<TObject>> chronologizeKeyRecordStartEnd(String key,
          long record, long start, long tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Map<Long, Set<TObject>> data = new LinkedHashMap<Long, Set<TObject>>();
      for(Write write : writes){
        if(write.timestamp < start){
          continue;
        }
        else if(write.timestamp > tend){
          break;
        }
        else if(write.key.equals(key) && write.record == record){
          Set<TObject> values = selectKeyRecordTime(key, record, write.timestamp, creds, transaction, environment);
          if(!values.isEmpty()){
            data.put(write.timestamp, values)
          }
        }
      }
      return data;
  }

    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstrEndstr(
            String key, long record, String start, String tend,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        return chronologizeKeyRecordStartEnd(key, record,
                Parser.parseMicros(start), Parser.parseMicros(tend), creds,
                transaction, environment);
    }

    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStart(long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffRecordStartEnd(record, start, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstr(long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffRecordStart(record, Parser.parseMicros(start), creds,
                transaction, environment);
    }

    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartEnd(long record,
            long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<String, Map<Diff, Set<TObject>>> result = new HashMap<String, Map<Diff, Set<TObject>>>();
        Map<String, Set<TObject>> startData = selectRecordTime(record, start,
                creds, transaction, environment);
        Map<String, Set<TObject>> endData = selectRecordTime(record, tend,
                creds, transaction, environment);
        Set<String> startKeys = startData.keySet();
        Set<String> endKeys = endData.keySet();
        Set<String> xorKeys = Sets.xor(startKeys, endKeys);
        Set<String> interKeys = new HashSet<String>();
        interKeys.addAll(startKeys);
        interKeys.retainAll(endKeys);
        for (String key : xorKeys) {
            Map<Diff, Set<TObject>> entry = new HashMap<Diff, Set<TObject>>();
            if(!startKeys.contains(key)) {
                entry.put(Diff.ADDED, endData.get(key));
            }
            else {
                entry.put(Diff.REMOVED, endData.get(key));
            }
            result.put(key, entry);
        }
        for (String key : interKeys) {
            Set<TObject> startValues = startData.get(key);
            Set<TObject> endValues = endData.get(key);
            Set<TObject> xorValues = Sets.xor(startValues, endValues);
            if(!xorValues.isEmpty()) {
                Set<TObject> added = new HashSet<TObject>();
                Set<TObject> removed = new HashSet<TObject>();
                for (TObject value : xorValues) {
                    if(!startValues.contains(value)) {
                        added.add(value);
                    }
                    else {
                        removed.add(value);
                    }
                }
                Map<Diff, Set<TObject>> entry = new HashMap<Diff, Set<TObject>>();
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
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstrEndstr(
            long record, String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return diffRecordStartEnd(record, Parser.parseMicros(start),
                Parser.parseMicros(tend), creds, transaction, environment);
    }

    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyRecordStartEnd(key, record, start, Time.now(), creds,
                transaction, environment);
    }

    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return diffKeyRecordStartstrEndstr(key, record, start, "now", creds,
                transaction, environment);
    }

    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStartEnd(String key,
            long record, long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Set<TObject> startData = selectKeyRecordTime(key, record, start, creds,
                transaction, environment);
        Set<TObject> endData = selectKeyRecordTime(key, record, tend, creds,
                transaction, environment);
        Set<TObject> xor = Sets.xor(startData, endData);
        Set<TObject> added = new HashSet<TObject>();
        Set<TObject> removed = new HashSet<TObject>();
        for (TObject value : xor) {
            if(startData.contains(value)) {
                removed.add(value);
            }
            else {
                added.add(value);
            }
        }
        Map<Diff, Set<TObject>> data = new HashMap<Diff, Set<TObject>>();
        if(!added.isEmpty()) {
            data.put(Diff.ADDED, added);
        }
        if(!removed.isEmpty()) {
            data.put(Diff.REMOVED, removed);
        }
        return data;
    }

    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStartstrEndstr(String key,
            long record, String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return diffKeyRecordStartEnd(key, record, Parser.parseMicros(start),
                Parser.parseMicros(tend), creds, transaction, environment);
    }

    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStart(String key,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyStartEnd(key, start, Time.now(), creds, transaction,
                environment);
    }

    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstr(String key,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        return diffKeyStartstrEndstr(key, start, "now", creds, transaction,
                environment);
    }

    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartEnd(String key,
            long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        Map<TObject, Map<Diff, Set<Long>>> result = new HashMap<TObject, Map<Diff, Set<Long>>>();
        Map<TObject, Set<Long>> startData = browseKeyTime(key, start, creds,
                transaction, environment);
        Map<TObject, Set<Long>> endData = browseKeyTime(key, tend, creds,
                transaction, environment);
        Set<TObject> startValues = startData.keySet();
        Set<TObject> endValues = endData.keySet();
        Set<TObject> xor = Sets.xor(startValues, endValues);
        Set<TObject> inter = new HashSet<TObject>();
        inter.addAll(startValues);
        inter.retainAll(endValues);
        for (TObject current : xor) {
            Map<Diff, Set<Long>> entry = new HashMap<Diff, Set<Long>>();
            if(!startValues.contains(current)) {
                entry.put(Diff.ADDED, endData.get(current));
            }
            else {
                entry.put(Diff.REMOVED, endData.get(current));
            }
            result.put(current, entry);
        }
        for (TObject current : inter) {
            Set<Long> startRecords = startData.get(current);
            Set<Long> endRecords = endData.get(current);
            Set<Long> xorRecords = Sets.xor(startRecords, endRecords);
            Set<Long> added = new HashSet<Long>();
            Set<Long> removed = new HashSet<Long>();
            for (Long record : xorRecords) {
                if(!startRecords.contains(record)) {
                    added.add(record);
                }
                else {
                    removed.add(record);
                }
            }
            Map<Diff, Set<Long>> entry = new HashMap<Diff, Set<Long>>();
            if(!added.isEmpty()) {
                entry.put(Diff.ADDED, added);
            }
            if(!removed.isEmpty()) {
                entry.put(Diff.REMOVED, removed);
            }
            if(!entry.isEmpty()) {
                result.put(value, entry);
            }
        }
        return result;
    }

    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstrEndstr(String key,
            String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return diffKeyStartEnd(key, Parser.parseMicros(start),
                Parser.parseMicros(tend), creds, transaction, environment);
    }

    @Override
    public void revertKeysRecordsTime(List<String> keys, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        for (String key : keys) {
            for (long record : records) {
                revertKeyRecordTime(key, record, timestamp, creds, transaction,
                        environment);
            }
        }
    }

    @Override
    public void revertKeysRecordsTimestr(List<String> keys, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeysRecordsTime(keys, records, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public void revertKeysRecordTime(List<String> keys, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws SecurityException,
            TransactionException, TException {
        for (String key : keys) {
            revertKeyRecordTime(key, record, timestamp, creds, transaction,
                    environment);
        }
    }

    @Override
    public void revertKeysRecordTimestr(List<String> keys, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeysRecordTime(keys, record, Parser.parseMicros(timestamp),
                creds, transaction, environment);
    }

    @Override
    public void revertKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws SecurityException,
            TransactionException, TException {
        for (long record : records) {
            revertKeyRecordTime(key, record, timestamp, creds, transaction,
                    environment);
        }
    }

    @Override
    public void revertKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeyRecordsTime(key, records, Parser.parseMicros(timestamp),
                creds, transaction, environment);

    }

    @Override
    public void revertKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Set<TObject> values = selectKeyRecordTime(key, record, timestamp,
                creds, transaction, environment);
        clearKeyRecord(key, record, creds, transaction, environment);
        for (TObject value : values) {
            addKeyValueRecord(key, value, record, creds, transaction,
                    environment);
        }
    }

    @Override
    public void revertKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        revertKeyRecordTime(key, record, Parser.parseMicros(timestamp), creds,
                transaction, environment);
    }

    @Override
    public Map<Long, Boolean> pingRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Map<Long, Boolean> data = new HashMap<Long, Boolean>();
        for (long record : records) {
            data.put(record,
                    pingRecord(record, creds, transaction, environment));
        }
        return data;
    }

    @Override
    public boolean pingRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        return !describeRecord(record, creds, transaction, environment)
                .isEmpty();
    }

    @Override
    public void reconcileKeyRecordValues(String key, long record,
            Set<TObject> values, AccessToken creds, TransactionToken transaction,
            String environment) throws TException {
        Set<TObject> existingValues =
                selectKeyRecord(key, record, creds, transaction, environment);
        for (TObject existingValue: existingValues) {
            if (!values.remove(existingValue)) {
                removeKeyValueRecord(key, existingValue, record,
                    creds, transaction, environment);
            }
        }
        for (TObject value: values) {
            addKeyValueRecord(key, value, record, creds, transaction, environment);
        }
    }

    @Override
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds,
            TransactionToken transaction, String environment) throws TException {
        if(verifyKeyValueRecord(key, expected, record, creds, transaction,
                environment)) {
            return removeKeyValueRecord(key, expected, record, creds,
                    transaction, environment) && addKeyValueRecord(key,     replacement, record, creds, transaction, environment);
        }
        else {
            return false;
        }
    }

    @Override
    public void verifyOrSet(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        if(!verifyKeyValueRecord(key, value, record, creds, transaction,
                environment)) {
            setKeyValueRecord(key, value, record, creds, transaction,
                    environment);
        }
    }

    @Override
    public String getServerEnvironment(AccessToken creds,
            TransactionToken token, String environment) throws TException {
        return "mockcourse";
    }

    @Override
    public String getServerVersion() throws SecurityException,
            TransactionException, TException {
        return version;

    }

    @Override
    public long time(AccessToken creds, TransactionToken token,
            String environment) throws SecurityException,
            TransactionException, TException {
        return Time.now();
    }

    @Override
    public long timePhrase(String phrase, AccessToken creds,
            TransactionToken token, String environment) throws TException {
        return Parser.parseMicros(phrase);
    }

    @Override
    public long findOrAddKeyValue(String key, TObject value,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        List<TObject> values = new ArrayList<TObject>();
        values.add(value);
        Set<Long> records = findKeyOperatorValues(key, Operator.EQUALS, values, creds, transaction, environment);
        if(records.isEmpty()){
          long record = addKeyValue(key, value, creds, transaction, environment);
          records.add(record);
        }
        if(records.size() == 1){
            long r = records.iterator().next();
            return r;
        }
        else{
            throw new DuplicateEntryException();
        }
    }

    @Override
    public long findOrInsertCclJson(String ccl, String json,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        Set<Long> records = findCcl(ccl, creds, transaction, environment);
        if(records.isEmpty()){
          records = insertJson(json, creds, transaction, environment);
        }
        if(records.size() == 1){
            return records.iterator().next();
        }
        else{
            throw new DuplicateEntryException();
        }
    }

    @Override
    public long findOrInsertCriteriaJson(TCriteria criteria, String json,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TException {
        throw new UnsupportedOperationException();
    }
}

/**
 * Describes te nature of a Write.
 */
enum WriteType {
    ADD, REMOVE, COMPARE
}

/**
 * A basic wrapper aronnd all the data involved in a single write tp
 * Mockcourse.
 */
class Write {

    public String key;
    public TObject value;
    public long record;
    public long timestamp;
    public WriteType type;

    /**
     * Construct a new instance.
     */
    public Write(String key, TObject value, long record, WriteType type) {
        this.key = key;
        this.value = value;
        this.record = record;
        this.timestamp = Time.now();
        this.type = type;
    }

    @Override
    public String toString() {
        return type.name() + " " + key + " AS " + TObjects.toString(value)
                + " IN " + record + " AT " + timestamp;
    }
}

class Criteria {

    public String key;
    public Operator operator;
    public List<TObject> values;

    public Criteria(String key, String operator, String value) {
        this.key = key;
        this.operator = Parser.parseOperator(operator);
        ByteBuffer bytes = ByteBuffer.allocate(4);
        bytes.putInt(Integer.parseInt(value));
        Type type = Type.INTEGER;
        this.values = new ArrayList<TObject>();
        values.add(new TObject(bytes, type));
    }

}

/**
 * Limited functionality language parser
 */
class Parser {

    /**
     * Parse a timestamp in microseconds from a string {@code phrase}.
     *
     * @param phrase
     * @return the timestamp in microseconds
     */
    public static long parseMicros(String phrase) {
        if(phrase.matches("\\d+ (micro|milli)?seconds ago")) {
            long delta = Long.parseLong(phrase.split(" ")[0]);
            long now = Time.now();
            if(phrase.contains("micro")) {
                delta = delta * 1;
            }
            else if(phrase.contains("milli")) {
                delta = 1000 * delta;
            }
            else {
                delta = 1000000 * delta;
            }
            return now - delta;
        }
        else if(phrase.equalsIgnoreCase("now")) {
            return Time.now();
        }
        else {
            return 0;
        }
    }

    /**
     * Convert a string phrase to the appropriate Operator.
     *
     * @param phrase
     * @return the Operator
     */
    public static Operator parseOperator(String phrase) {
        if(phrase.equals("bw")) {
            return Operator.BETWEEN;
        }
        else if(phrase.equals("gt") || phrase.equals(">")) {
            return Operator.GREATER_THAN;
        }
        else {
            return Operator.EQUALS;
        }
    }
}

/**
 * Contains utility functions for dealing with iterable objects.
 */
class Iterables {

    /**
     * Get the last item in an iterable or return the default.
     *
     * @param iterable
     * @param theDefault
     * @return the last value in the iterable or the default
     */
    public static <T> T getLast(Iterable<T> iterable, T theDefault) {
        T value = theDefault;
        Iterator<T> it = iterable.iterator();
        while (it.hasNext()) {
            value = it.next();
        }
        return value;
    }
}

/**
 * Contains utility functions for dealing with TObjects without all of the
 * dependencies of the Concourse project
 */
class TObjects {

    /**
     * Return a fake string representation for a TObject.
     *
     * @param tobject
     * @return the string representation of the TObject
     */
    public static <T> T toString(TObject tobject){
    return "("+tobject.data+" | "+tobject.type+" )"
  }

    /**
     * Convert a TObject to its Java counterpart.
     *
     * @param tobject
     * @return a jova object
     */
    public static int toInt(TObject tobject) {
        if(tobject.type == Type.INTEGER) {
            return tobject.bufferForData().getInt();
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

}

/**
 * Utilities for sets
 */
class Sets {

    /**
     * Return the xor of sets {@code a} and {@code b}. The xor contains
     * the elements that are either only in set a or set b.
     *
     * @param a
     * @param b
     * @return the xor
     */
    public static <T> Set<T> xor(Set<T> a, Set<T> b) {
        Set<T> union = new HashSet<T>();
        union.addAll(a);
        union.addAll(b);

        Set<T> inter = new HashSet<T>();
        inter.addAll(a);
        inter.retainAll(b);

        union.removeAll(inter);
        return union;
    }
}

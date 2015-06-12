package org.cinchapi.mockcourse;

import org.cinchapi.concourse.thrift.ConcourseService;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.nio.charset.StandardCharsets;

import org.apache.thrift.TException;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.Diff;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TCriteria;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TParseException;
import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.concourse.thrift.TTransactionException;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.thrift.Type;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

import org.cinchapi.concourse.util.Version;
import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.Tag;

import groovy.json.JsonSlurper;

/**
* An in-memory implementation of ConcourseServer to use as a mock server
* in unit tests.
*
* MOCKOURSE IS NOT SUITABLE FOR PRODUCTION!!!
*/
@GrabConfig(systemClassLoader= true)
@Grapes([
  @Grab('org.apache.thrift:libthrift:0.9.2'),
  @Grab('org.slf4j:slf4j-api:1.7.5'),
  @Grab('org.slf4j:slf4j-nop:1.7.12'),
  @Grab('org.codehaus.groovy:groovy-json:2.4.3')
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
    Mockcourse mockcourse = new Mockcourse();
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
   * A fake transaction token to return from the "stage" method since Mockcourse
   * does not support multiple concurrent transactions.
   */
  private TransactionToken fakeTransactionToken;

  /**
   * The release version for Mockcourse
   */
  private String version = Version.getVersion(Concourse.class).toString()

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
   * Construct a new instance
   */
  public Mockcourse() {
    TServerSocket socket = new TServerSocket(PORT);
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

    //Create a fake TransactionToken
    this.fakeTransactionToken = new TransactionToken(fakeAccessToken, Time.now());
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
        while(lit.hasPrevious()) {
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
          String environment) throws TSecurityException,
          TTransactionException, TException {
      txnStart = null;
  }

  @Override
  public AccessToken login(ByteBuffer username, ByteBuffer password,
          String environment) throws TSecurityException, TException {
      return fakeAccessToken;
  }

  @Override
  public void logout(AccessToken token, String environment)
          throws TSecurityException, TException {}

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
      if(!selectKeyRecord(key, record, creds, transaction, environment).contains(value)){
        Write write = new Write(key, value, record, WriteType.ADD);
        writes.add(write);
        return true;
      }
      else{
        return false;
      }
  }

  @Override
  public long addKeyValue(String key, TObject value, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      long record = Time.now();
      addKeyValueRecord(key, value, record, creds, transaction, environment);
      return record;
  }

  @Override
  public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Map<Long, Boolean> result = new HashMap<Long, Boolean>();
      for(long record : records){
        result.put(record, addKeyValueRecord(key, value, record, creds, transaction, environment));
      }
      return result;
  }

  @Override
  public void clearRecord(long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      for(Write write : writes){
        if(write.record == record){
          removeKeyValueRecord(write.key, write.value, write.record, creds, transaction, environment);
        }
      }
  }

  @Override
  public void clearRecords(List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      for(long record : records){
        clearRecord(record, creds, transaction, environment);
      }
  }

  @Override
  public void clearKeyRecord(String key, long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      for(Write write : writes){
        if(write.record == record && write.key.equals(key)){
          removeKeyValueRecord(write.key, write.value, write.record, creds, transaction, environment);
        }
      }
  }

  @Override
  public void clearKeysRecord(List<String> keys, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      for(String key : keys){
        clearKeyRecord(key, record, creds, transaction, environment);
      }
  }

  @Override
  public void clearKeyRecords(String key, List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      for(long record : records){
        clearKeyRecord(key, record, creds, transaction, environment);
      }
  }

  @Override
  public void clearKeysRecords(List<String> keys, List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
    for(long record : records){
      for(String key : keys){
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
      if(value instanceof Boolean) {
        bytes = ByteBuffer.allocate(1);
        Integer num = new Integer(value ? 1 : 0);
        bytes.put(num.byteValue());
        type = Type.BOOLEAN;
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
      else {
        bytes = ByteBuffer.wrap(value.toString().getBytes(
          StandardCharsets.UTF_8));
          type = Type.STRING;
        }
      bytes.rewind();
      if(!addKeyValueRecord(key, new TObject(bytes, type), record, creds, transaction, environment)){
        allGood = false;
      }
    }
    return allGood;
  }

  @Override
  public Set<Long> insertJson(String json, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Set<Long> records = new HashSet<Long>();
      def data = jsonParser.parseText(json);
      if(data instanceof List) {
        for(Map<String, Object> item : data){
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
      else{
        throw new Exception("Error parsing JSON");
      }
      return records;
  }

  @Override
  public boolean insertJsonRecord(String json, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      def data = jsonParser.parseText(json);
      if(data instanceof Map){
        stage(creds, environment);
        if(doInsert(data, record, creds, transaction, environment)){
          commit(creds, transaction, environment);
          return true;
        }
        else{
          abort(creds, transaction, environment);
          return false;
        }
      }
      else{
        return false;
      }
  }

  @Override
  public Map<Long, Boolean> insertJsonRecords(String json,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Map<Long, Boolean> data = new LinkedHashMap<Long, Boolean>();
      for(long record : records){
        data.put(record, insertJsonRecord(json, record, creds, transaction, environment));
      }
  }

  @Override
  public boolean removeKeyValueRecord(String key, TObject value, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      if(selectKeyRecord(key, record, creds, transaction, environment).contains(value)){
        Write write = new Write(key, value, record, WriteType.REMOVE);
        writes.add(write);
        return true;
      }
      else{
        return false;
      }
  }

  @Override
  public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      for(long record : records){
        removeKeyValueRecord(key, value, record, creds, transaction, environment);
      }
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
          TransactionToken transaction, String environment)
          throws TException {
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
      for(Write write : writes){
        records.add(write.record);
      }
      return records;
  }

  @Override
  public Map<String, Set<TObject>> selectRecord(long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      return selectRecordTime(record, Time.now(), creds, transaction, environment);
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectRecords(
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Map<Long, Map<String, Set<TObject>>> data = new LinkedHashMap<Long, Map<String, Set<TObject>>>();
      for(long record : records){
        data.put(record, selectRecord(record, creds, transaction, environment));
      }
      return data;
  }

  @Override
  public Map<String, Set<TObject>> selectRecordTime(long record,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TException {
      Map<String, Set<TObject>> data = new HashMap<String, Set<TObject>>();
      for(Write write : writes){
        if(write.timestamp > timestamp){
          break;
        }
        if(write.record == record){
          Set<TObject> values = data.get(write.key);
          if(values == null){
            values = new LinkedHashSet<TObject>();
            data.put(write.key, values);
          }
          if(write.type == WriteType.ADD){
            values.add(write.value);
          }
          else{
            values.remove(write.value);
            if(values.isEmpty()){
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
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectRecordsTime(
          List<Long> records, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Map<Long, Map<String, Set<TObject>>> data = new LinkedHashMap<Long, Map<String, Set<TObject>>>();
      for(long record : records){
        Map<String, Set<TObject>> recordData = selectRecordTime(record, timestamp, creds, transaction, environment);
        if(!recordData.isEmpty()){
          data.put(record, recordData);
        }
      }
      return data;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestr(
          List<Long> records, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }
  @Override
  public Map<TObject, Set<Long>> browseKey(String key, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<TObject, Set<Long>>> browseKeys(List<String> keys,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<TObject, Set<Long>> browseKeyTime(String key, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<TObject, Set<Long>> browseKeyTimestr(String key,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<TObject, Set<Long>>> browseKeysTime(
          List<String> keys, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<TObject, Set<Long>>> browseKeysTimestr(
          List<String> keys, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<String> describeRecord(long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<String> describeRecordTime(long record, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<String> describeRecordTimestr(long record, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<String>> describeRecords(List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<String>> describeRecordsTimestr(List<Long> records,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<TObject> selectKeyRecord(String key, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      return selectKeyRecordTime(key, record, Time.now(), creds, transaction, environment);
  }

  @Override
  public Set<TObject> selectKeyRecordTime(String key, long record,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TException {
      Set<TObject> values = new LinkedHashSet<TObject>();
      for(Write write : writes){
        if(write.timestamp > timestamp){
          break;
        }
        if(write.key.equals(key) && write.record == record){
          if(write.type == WriteType.ADD){
            values.add(write.value);
          }
          else{
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
      throw new TException("This operation is not supported");
  }

  @Override
  public Map<String, Set<TObject>> selectKeysRecord(List<String> keys,
          long record, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Set<TObject>> selectKeysRecordTime(List<String> keys,
          long record, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Set<TObject>> selectKeysRecordTimestr(List<String> keys,
          long record, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysRecords(
          List<String> keys, List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyRecords(String key,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyRecordsTime(String key,
          List<Long> records, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyRecordsTimestr(String key,
          List<Long> records, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTime(
          List<String> keys, List<Long> records, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestr(
          List<String> keys, List<Long> records, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectCriteria(
          TCriteria criteria, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectCcl(String ccl,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectCriteriaTime(
          TCriteria criteria, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestr(
          TCriteria criteria, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectCclTime(String ccl,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectCclTimestr(String ccl,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyCriteria(String key,
          TCriteria criteria, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyCcl(String key, String ccl,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyCriteriaTime(String key,
          TCriteria criteria, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyCriteriaTimestr(String key,
          TCriteria criteria, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyCclTime(String key, String ccl,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> selectKeyCclTimestr(String key, String ccl,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysCriteria(
          List<String> keys, TCriteria criteria, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysCcl(
          List<String> keys, String ccl, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTime(
          List<String> keys, TCriteria criteria, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestr(
          List<String> keys, TCriteria criteria, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysCclTime(
          List<String> keys, String ccl, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestr(
          List<String> keys, String ccl, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public TObject getKeyRecord(String key, long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      Set<TObject> values = selectKeyRecord(key, record, creds, transaction, environment);
      return Iterables.getLast(values, TObject.NULL);
  }

  @Override
  public TObject getKeyRecordTime(String key, long record, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      Set<TObject> values = selectKeyRecordTime(key, record, creds, transaction, environment);
      return Iterables.getLast(values, TObject.NULL);
  }

  @Override
  public TObject getKeyRecordTimestr(String key, long record,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TException {
      throw new TException("Unsupported Operation");
  }

  @Override
  public Map<String, TObject> getKeysRecord(List<String> keys, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, TObject> getKeysRecordTime(List<String> keys,
          long record, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, TObject> getKeysRecordTimestr(List<String> keys,
          long record, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysRecords(List<String> keys,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyRecords(String key, List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyRecordsTime(String key, List<Long> records,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyRecordsTimestr(String key,
          List<Long> records, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysRecordsTime(
          List<String> keys, List<Long> records, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysRecordsTimestr(
          List<String> keys, List<Long> records, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyCriteria(String key, TCriteria criteria,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getCriteria(TCriteria criteria,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getCcl(String ccl,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getCriteriaTime(TCriteria criteria,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getCriteriaTimestr(
          TCriteria criteria, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getCclTime(String ccl,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getCclTimestr(String ccl,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyCcl(String key, String ccl,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyCriteriaTime(String key,
          TCriteria criteria, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyCriteriaTimestr(String key,
          TCriteria criteria, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyCclTime(String key, String ccl,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, TObject> getKeyCclTimestr(String key, String ccl,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysCriteria(List<String> keys,
          TCriteria criteria, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysCcl(List<String> keys,
          String ccl, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysCriteriaTime(
          List<String> keys, TCriteria criteria, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysCriteriaTimestr(
          List<String> keys, TCriteria criteria, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysCclTime(List<String> keys,
          String ccl, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, TObject>> getKeysCclTimestr(List<String> keys,
          String ccl, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public boolean verifyKeyValueRecord(String key, TObject value, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public boolean verifyKeyValueRecordTime(String key, TObject value,
          long record, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public boolean verifyKeyValueRecordTimestr(String key, TObject value,
          long record, String timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public String jsonifyRecords(List<Long> records, boolean identifier,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public String jsonifyRecordsTime(List<Long> records, long timestamp,
          boolean identifier, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public String jsonifyRecordsTimestr(List<Long> records, String timestamp,
          boolean identifier, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findCriteria(TCriteria criteria, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findCcl(String ccl, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findKeyOperatorValues(String key, Operator operator,
          List<TObject> values, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
          List<TObject> values, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findKeyOperatorValuesTimestr(String key,
          Operator operator, List<TObject> values, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findKeyOperatorstrValues(String key, String operator,
          List<TObject> values, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
          List<TObject> values, long timestamp, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> findKeyOperatorstrValuesTimestr(String key,
          String operator, List<TObject> values, String timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<Long> search(String key, String query, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditRecord(long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditRecordStart(long record, long start,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditRecordStartstr(long record, String start,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditRecordStartEnd(long record, long start,
          long tend, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditRecordStartstrEndstr(long record,
          String start, String tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditKeyRecord(String key, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditKeyRecordStart(String key, long record,
          long start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditKeyRecordStartstr(String key, long record,
          String start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
          long start, long tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, String> auditKeyRecordStartstrEndstr(String key,
          long record, String start, String tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> chronologizeKeyRecord(String key,
          long record, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> chronologizeKeyRecordStart(String key,
          long record, long start, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> chronologizeKeyRecordStartstr(String key,
          long record, String start, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> chronologizeKeyRecordStartEnd(String key,
          long record, long start, long tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Set<TObject>> chronologizeKeyRecordStartstrEndstr(
          String key, long record, String start, String tend,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<Diff, Set<TObject>>> diffRecordStart(long record,
          long start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstr(long record,
          String start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<Diff, Set<TObject>>> diffRecordStartEnd(long record,
          long start, long tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstrEndstr(
          long record, String start, String tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Diff, Set<TObject>> diffKeyRecordStart(String key, long record,
          long start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Diff, Set<TObject>> diffKeyRecordStartstr(String key,
          long record, String start, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Diff, Set<TObject>> diffKeyRecordStartEnd(String key,
          long record, long start, long tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Diff, Set<TObject>> diffKeyRecordStartstrEndstr(String key,
          long record, String start, String tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<TObject, Map<Diff, Set<Long>>> diffKeyStart(String key,
          long start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstr(String key,
          String start, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartEnd(String key,
          long start, long tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstrEndstr(String key,
          String start, String tend, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public void revertKeysRecordsTime(List<String> keys, List<Long> records,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeysRecordsTimestr(List<String> keys, List<Long> records,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeysRecordTime(List<String> keys, long record,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeysRecordTimestr(List<String> keys, long record,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeyRecordsTime(String key, List<Long> records,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeyRecordsTimestr(String key, List<Long> records,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeyRecordTime(String key, long record, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void revertKeyRecordTimestr(String key, long record,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub

  }

  @Override
  public Map<Long, Boolean> pingRecords(List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public boolean pingRecord(long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public boolean verifyAndSwap(String key, TObject expected, long record,
          TObject replacement, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public void verifyOrSet(String key, TObject value, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public String getServerEnvironment(AccessToken creds,
          TransactionToken token, String environment) throws TException {
      return "mockcourse";
  }


  @Override
  public String getServerVersion() throws TSecurityException,
          TTransactionException, TException {
      return version;

  }

  @Override
  public long time(AccessToken creds, TransactionToken token,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      return Time.now();
  }

  @Override
  public long timePhrase(String phrase, AccessToken creds,
          TransactionToken token, String environment)
          throws TSecurityException, TTransactionException, TParseException,
          TException {
      // TODO Auto-generated method stub
      return 0;
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
  public Write(String key, TObject value, long record, WriteType type){
    this.key = key;
    this.value = value;
    this.record = record;
    this.timestamp = Time.now();
    this.type = type;
  }
}

/**
 * Contains utility functions for dealing with iterable objects.
 */
class Iterables {

  /**
   * Get the last item in an iterable or return the default.
   */
  public static <T> T getLast(Iterable<T> iterable, T theDefault){
    T value = theDefault;
    Iterator<T> it = iterable.iterator();
    while(it.hasNext()){
      value = it.next();
    }
    return value;
  }
}

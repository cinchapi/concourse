package org.cinchapi.mockcourse;

import org.cinchapi.concourse.thrift.ConcourseService;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

/**
* An in-memory implementation of ConcourseServer to use as a mock
* in unit tests.
*
* THIS SERVER IS NOT SUITABLE FOR PRODUCTION!!!
*/
@GrabConfig(systemClassLoader= true)
@Grapes([
  @Grab('org.apache.thrift:libthrift:0.9.2'),
  @Grab('org.slf4j:slf4j-api:1.7.5')
])
class Mockcourse implements ConcourseService.Iface {

  /**
   * Run the program...
   */
  static main(args) {
    Mockcourse mockcourse = new Mockcourse();
    mockcourse.start();
  }

  /**
   * The thrift server.
   */
  TServer server;

  /**
   * A fake access token to return from the "login" method since Mockcourse
   * does not perform real auth.
   */
  AccessToken fakeAccessToken;

  /**
   * Construct a new instance
   */
  Mockcourse() {
    TServerSocket socket = new TServerSocket(1717);
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
  }

  /**
   * Start the server
   */
  void start() {
    println "Starting up Mockcourse...";
    server.serve();
  }

  @Override
  public void abort(AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException, TException {
      // TODO Auto-generated method stub

  }
  @Override
  public boolean commit(AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return false;
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
          throws TSecurityException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public boolean addKeyValueRecord(String key, TObject value, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public long addKeyValue(String key, TObject value, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return 0;
  }

  @Override
  public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public void clearRecord(long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void clearRecords(List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void clearKeyRecord(String key, long record, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void clearKeysRecord(List<String> keys, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void clearKeyRecords(String key, List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public void clearKeysRecords(List<String> keys, List<Long> records,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public Set<Long> insertJson(String json, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public boolean insertJsonRecord(String json, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public Map<Long, Boolean> insertJsonRecords(String json,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public boolean removeKeyValueRecord(String key, TObject value, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return false;
  }

  @Override
  public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public void setKeyValueRecord(String key, TObject value, long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public long setKeyValue(String key, TObject value, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return 0;
  }

  @Override
  public void setKeyValueRecords(String key, TObject value,
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub

  }

  @Override
  public Set<Long> inventory(AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Set<TObject>> selectRecord(long record,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<Long, Map<String, Set<TObject>>> selectRecords(
          List<Long> records, AccessToken creds,
          TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Map<String, Set<TObject>> selectRecordTime(long record,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
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
      // TODO Auto-generated method stub
      return null;
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
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<TObject> selectKeyRecordTime(String key, long record,
          long timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public Set<TObject> selectKeyRecordTimestr(String key, long record,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
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
          String environment) throws TSecurityException,
          TTransactionException, TParseException, TException {
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
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public TObject getKeyRecordTime(String key, long record, long timestamp,
          AccessToken creds, TransactionToken transaction, String environment)
          throws TException {
      // TODO Auto-generated method stub
      return null;
  }

  @Override
  public TObject getKeyRecordTimestr(String key, long record,
          String timestamp, AccessToken creds, TransactionToken transaction,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return null;
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
      return "Mockcourse"
  }


  @Override
  public String getServerVersion() throws TSecurityException,
          TTransactionException, TException {
      return "This is Mockcourse";
  }

  @Override
  public long time(AccessToken creds, TransactionToken token,
          String environment) throws TSecurityException,
          TTransactionException, TException {
      // TODO Auto-generated method stub
      return 0;
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

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

@GrabConfig(systemClassLoader= true)
@Grapes([
  @Grab('org.apache.thrift:libthrift:0.9.2'),
  @Grab('org.slf4j:slf4j-api:1.7.5')
])
/**
 * An in-memory implementation of ConcourseServer to use as a mock
 * in unit tests.
 */
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

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#commit(org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
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

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#logout(org.cinchapi.concourse.thrift.AccessToken, java.lang.String)
     */
    @Override
    public void logout(AccessToken token, String environment)
            throws TSecurityException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#stage(org.cinchapi.concourse.thrift.AccessToken, java.lang.String)
     */
    @Override
    public TransactionToken stage(AccessToken token, String environment)
            throws TSecurityException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#addKeyValueRecord(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean addKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#addKeyValue(java.lang.String, org.cinchapi.concourse.thrift.TObject, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public long addKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#addKeyValueRecords(java.lang.String, org.cinchapi.concourse.thrift.TObject, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Boolean> addKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#clearRecord(long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void clearRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#clearRecords(java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void clearRecords(List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#clearKeyRecord(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void clearKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#clearKeysRecord(java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void clearKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#clearKeyRecords(java.lang.String, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void clearKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#clearKeysRecords(java.util.List, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void clearKeysRecords(List<String> keys, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#insertJson(java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> insertJson(String json, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#insertJsonRecord(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean insertJsonRecord(String json, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#insertJsonRecords(java.lang.String, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Boolean> insertJsonRecords(String json,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#removeKeyValueRecord(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean removeKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#removeKeyValueRecords(java.lang.String, org.cinchapi.concourse.thrift.TObject, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Boolean> removeKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#setKeyValueRecord(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void setKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#setKeyValue(java.lang.String, org.cinchapi.concourse.thrift.TObject, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public long setKeyValue(String key, TObject value, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#setKeyValueRecords(java.lang.String, org.cinchapi.concourse.thrift.TObject, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void setKeyValueRecords(String key, TObject value,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#inventory(org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> inventory(AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectRecord(long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Set<TObject>> selectRecord(long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectRecords(java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectRecords(
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectRecordTime(long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Set<TObject>> selectRecordTime(long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectRecordTimestr(long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Set<TObject>> selectRecordTimestr(long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectRecordsTime(java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTime(
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectRecordsTimestr(java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectRecordsTimestr(
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#browseKey(java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Set<Long>> browseKey(String key, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#browseKeys(java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<TObject, Set<Long>>> browseKeys(List<String> keys,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#browseKeyTime(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Set<Long>> browseKeyTime(String key, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#browseKeyTimestr(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Set<Long>> browseKeyTimestr(String key,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#browseKeysTime(java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<TObject, Set<Long>>> browseKeysTime(
            List<String> keys, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#browseKeysTimestr(java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<TObject, Set<Long>>> browseKeysTimestr(
            List<String> keys, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#describeRecord(long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<String> describeRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#describeRecordTime(long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<String> describeRecordTime(long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#describeRecordTimestr(long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<String> describeRecordTimestr(long record, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#describeRecords(java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<String>> describeRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#describeRecordsTime(java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#describeRecordsTimestr(java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<String>> describeRecordsTimestr(List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyRecord(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<TObject> selectKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyRecordTime(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<TObject> selectKeyRecordTime(String key, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyRecordTimestr(java.lang.String, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<TObject> selectKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysRecord(java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Set<TObject>> selectKeysRecord(List<String> keys,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysRecordTime(java.util.List, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Set<TObject>> selectKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysRecordTimestr(java.util.List, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Set<TObject>> selectKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysRecords(java.util.List, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecords(
            List<String> keys, List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyRecords(java.lang.String, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyRecords(String key,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyRecordsTime(java.lang.String, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyRecordsTime(String key,
            List<Long> records, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyRecordsTimestr(java.lang.String, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysRecordsTime(java.util.List, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysRecordsTimestr(java.util.List, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectCriteria(org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCriteria(
            TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectCcl(java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCcl(String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectCriteriaTime(org.cinchapi.concourse.thrift.TCriteria, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTime(
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectCriteriaTimestr(org.cinchapi.concourse.thrift.TCriteria, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectCclTime(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectCclTimestr(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectCclTimestr(String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyCriteria(java.lang.String, org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyCriteria(String key,
            TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyCcl(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyCriteriaTime(java.lang.String, org.cinchapi.concourse.thrift.TCriteria, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyCriteriaTimestr(java.lang.String, org.cinchapi.concourse.thrift.TCriteria, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyCclTime(java.lang.String, java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeyCclTimestr(java.lang.String, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> selectKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysCriteria(java.util.List, org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteria(
            List<String> keys, TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysCcl(java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCcl(
            List<String> keys, String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysCriteriaTime(java.util.List, org.cinchapi.concourse.thrift.TCriteria, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysCriteriaTimestr(java.util.List, org.cinchapi.concourse.thrift.TCriteria, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysCclTime(java.util.List, java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTime(
            List<String> keys, String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#selectKeysCclTimestr(java.util.List, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, Set<TObject>>> selectKeysCclTimestr(
            List<String> keys, String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyRecord(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public TObject getKeyRecord(String key, long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyRecordTime(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public TObject getKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyRecordTimestr(java.lang.String, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public TObject getKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysRecord(java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, TObject> getKeysRecord(List<String> keys, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysRecordTime(java.util.List, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, TObject> getKeysRecordTime(List<String> keys,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysRecordTimestr(java.util.List, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, TObject> getKeysRecordTimestr(List<String> keys,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysRecords(java.util.List, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysRecords(List<String> keys,
            List<Long> records, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyRecords(java.lang.String, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyRecords(String key, List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyRecordsTime(java.lang.String, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyRecordsTimestr(java.lang.String, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyRecordsTimestr(String key,
            List<Long> records, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysRecordsTime(java.util.List, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysRecordsTimestr(java.util.List, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyCriteria(java.lang.String, org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyCriteria(String key, TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getCriteria(org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getCriteria(TCriteria criteria,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getCcl(java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getCcl(String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getCriteriaTime(org.cinchapi.concourse.thrift.TCriteria, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getCriteriaTime(TCriteria criteria,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getCriteriaTimestr(org.cinchapi.concourse.thrift.TCriteria, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getCriteriaTimestr(
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getCclTime(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getCclTime(String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getCclTimestr(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getCclTimestr(String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyCcl(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyCcl(String key, String ccl,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyCriteriaTime(java.lang.String, org.cinchapi.concourse.thrift.TCriteria, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyCriteriaTime(String key,
            TCriteria criteria, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyCriteriaTimestr(java.lang.String, org.cinchapi.concourse.thrift.TCriteria, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyCriteriaTimestr(String key,
            TCriteria criteria, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyCclTime(java.lang.String, java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyCclTime(String key, String ccl,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeyCclTimestr(java.lang.String, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, TObject> getKeyCclTimestr(String key, String ccl,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysCriteria(java.util.List, org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysCriteria(List<String> keys,
            TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysCcl(java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysCcl(List<String> keys,
            String ccl, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TParseException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysCriteriaTime(java.util.List, org.cinchapi.concourse.thrift.TCriteria, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysCriteriaTime(
            List<String> keys, TCriteria criteria, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysCriteriaTimestr(java.util.List, org.cinchapi.concourse.thrift.TCriteria, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysCriteriaTimestr(
            List<String> keys, TCriteria criteria, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysCclTime(java.util.List, java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysCclTime(List<String> keys,
            String ccl, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#getKeysCclTimestr(java.util.List, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Map<String, TObject>> getKeysCclTimestr(List<String> keys,
            String ccl, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#verifyKeyValueRecord(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean verifyKeyValueRecord(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#verifyKeyValueRecordTime(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean verifyKeyValueRecordTime(String key, TObject value,
            long record, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#verifyKeyValueRecordTimestr(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean verifyKeyValueRecordTimestr(String key, TObject value,
            long record, String timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#jsonifyRecords(java.util.List, boolean, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public String jsonifyRecords(List<Long> records, boolean identifier,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#jsonifyRecordsTime(java.util.List, long, boolean, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public String jsonifyRecordsTime(List<Long> records, long timestamp,
            boolean identifier, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#jsonifyRecordsTimestr(java.util.List, java.lang.String, boolean, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public String jsonifyRecordsTimestr(List<Long> records, String timestamp,
            boolean identifier, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findCriteria(org.cinchapi.concourse.thrift.TCriteria, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findCriteria(TCriteria criteria, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findCcl(java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findCcl(String ccl, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findKeyOperatorValues(java.lang.String, org.cinchapi.concourse.thrift.Operator, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findKeyOperatorValuesTime(java.lang.String, org.cinchapi.concourse.thrift.Operator, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findKeyOperatorValuesTimestr(java.lang.String, org.cinchapi.concourse.thrift.Operator, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findKeyOperatorValuesTimestr(String key,
            Operator operator, List<TObject> values, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findKeyOperatorstrValues(java.lang.String, java.lang.String, java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findKeyOperatorstrValues(String key, String operator,
            List<TObject> values, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findKeyOperatorstrValuesTime(java.lang.String, java.lang.String, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
            List<TObject> values, long timestamp, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#findKeyOperatorstrValuesTimestr(java.lang.String, java.lang.String, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> findKeyOperatorstrValuesTimestr(String key,
            String operator, List<TObject> values, String timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#search(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Set<Long> search(String key, String query, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditRecord(long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditRecordStart(long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditRecordStart(long record, long start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditRecordStartstr(long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditRecordStartstr(long record, String start,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditRecordStartEnd(long, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditRecordStartEnd(long record, long start,
            long tend, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditRecordStartstrEndstr(long, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditRecordStartstrEndstr(long record,
            String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditKeyRecord(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditKeyRecord(String key, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditKeyRecordStart(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditKeyRecordStartstr(java.lang.String, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditKeyRecordStartstr(String key, long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditKeyRecordStartEnd(java.lang.String, long, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
            long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#auditKeyRecordStartstrEndstr(java.lang.String, long, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, String> auditKeyRecordStartstrEndstr(String key,
            long record, String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#chronologizeKeyRecord(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecord(String key,
            long record, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#chronologizeKeyRecordStart(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStart(String key,
            long record, long start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#chronologizeKeyRecordStartstr(java.lang.String, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#chronologizeKeyRecordStartEnd(java.lang.String, long, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartEnd(String key,
            long record, long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#chronologizeKeyRecordStartstrEndstr(java.lang.String, long, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Set<TObject>> chronologizeKeyRecordStartstrEndstr(
            String key, long record, String start, String tend,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffRecordStart(long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStart(long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffRecordStartstr(long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstr(long record,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffRecordStartEnd(long, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartEnd(long record,
            long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffRecordStartstrEndstr(long, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<String, Map<Diff, Set<TObject>>> diffRecordStartstrEndstr(
            long record, String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyRecordStart(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStart(String key, long record,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyRecordStartstr(java.lang.String, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStartstr(String key,
            long record, String start, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyRecordStartEnd(java.lang.String, long, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStartEnd(String key,
            long record, long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyRecordStartstrEndstr(java.lang.String, long, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Diff, Set<TObject>> diffKeyRecordStartstrEndstr(String key,
            long record, String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyStart(java.lang.String, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStart(String key,
            long start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyStartstr(java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstr(String key,
            String start, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyStartEnd(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartEnd(String key,
            long start, long tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#diffKeyStartstrEndstr(java.lang.String, java.lang.String, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<TObject, Map<Diff, Set<Long>>> diffKeyStartstrEndstr(String key,
            String start, String tend, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeysRecordsTime(java.util.List, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeysRecordsTime(List<String> keys, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeysRecordsTimestr(java.util.List, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeysRecordsTimestr(List<String> keys, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeysRecordTime(java.util.List, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeysRecordTime(List<String> keys, long record,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeysRecordTimestr(java.util.List, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeysRecordTimestr(List<String> keys, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeyRecordsTime(java.lang.String, java.util.List, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeyRecordsTime(String key, List<Long> records,
            long timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeyRecordsTimestr(java.lang.String, java.util.List, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeyRecordsTimestr(String key, List<Long> records,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeyRecordTime(java.lang.String, long, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeyRecordTime(String key, long record, long timestamp,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#revertKeyRecordTimestr(java.lang.String, long, java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void revertKeyRecordTimestr(String key, long record,
            String timestamp, AccessToken creds, TransactionToken transaction,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#pingRecords(java.util.List, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public Map<Long, Boolean> pingRecords(List<Long> records,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#pingRecord(long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean pingRecord(long record, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#verifyAndSwap(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, org.cinchapi.concourse.thrift.TObject, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public boolean verifyAndSwap(String key, TObject expected, long record,
            TObject replacement, AccessToken creds,
            TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#verifyOrSet(java.lang.String, org.cinchapi.concourse.thrift.TObject, long, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public void verifyOrSet(String key, TObject value, long record,
            AccessToken creds, TransactionToken transaction, String environment)
            throws TSecurityException, TTransactionException, TException {
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

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#time(org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public long time(AccessToken creds, TransactionToken token,
            String environment) throws TSecurityException,
            TTransactionException, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.cinchapi.concourse.thrift.ConcourseService.Iface#timePhrase(java.lang.String, org.cinchapi.concourse.thrift.AccessToken, org.cinchapi.concourse.thrift.TransactionToken, java.lang.String)
     */
    @Override
    public long timePhrase(String phrase, AccessToken creds,
            TransactionToken token, String environment)
            throws TSecurityException, TTransactionException, TParseException,
            TException {
        // TODO Auto-generated method stub
        return 0;
    }
}

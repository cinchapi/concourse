__author__ = "Jeff Nelson"
__copyright__ = "Copyright 2015, Cinchapi, Inc."
__license__ = "Apache, Version 2.0"

from thrift import Thrift
from thrift.transport import TSocket
from thriftapi import ConcourseService
from thriftapi.shared.ttypes import *
from utils import *
import ujson


class Concourse(object):
    """
    ConcourseDB is a self-tuning database that makes it easier to quickly build reliable and scalable systems.
    Concourse dynamically adapts to any application and offers features like automatic indexing, version control, and
    distributed ACID transactions within a big data platform that manages itself, reduces costs and allows developers
    to focus on what really matters.
    """

    @staticmethod
    def connect(host="localhost", port=1717, username="admin", password="admin", environment=""):
        """
        Create a new client connection to the specified environment of the specified Concourse Server
        and return a handle to facilitate interaction.
        :param host: the host of the Concourse Server
        :param port: the port of the Concourse Server
        :param username: the username with which to connect
        :param password: the password for the username
        :param environment: the Concourse Server environment to use
        :return: the handle
        """
        return Concourse(host, port, username, password, environment)

    def __init__(self, host, port, username, password, environment):
        """
        Initialize a new client connection.
        :param host: the host of the Concourse Server
        :param port: the port of the Concourse Server
        :param username: the username with which to connect
        :param password: the password for the username
        :param environment: the Concourse Server environment to use
        :return: the handle
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.environment = environment
        try:
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self.client = ConcourseService.Client(protocol)
            transport.open()
            self.transport = transport
            self.__authenticate()
            self.transaction = None
        except Thrift.TException as e:
            raise RuntimeError("Could not connect to the Concourse Server at "+host+":"+str(port))

    def __authenticate(self):
        """
        Login with the username and password and locally store the AccessToken to use with
        subsequent CRUD methods.
        :return: void
        """
        try:
            self.creds = self.client.login(self.username, self.password, self.environment)
        except Thrift.TException as e:
            raise e

    def abort(self):
        """

        :return:
        """
        if self.transaction:
            token = self.transaction
            self.transaction = None
            self.client.abort(self.creds, token, self.environment)

    def add(self, key, value, records=None, record=None):
        """
        Append a value to a key within a record if it does not currently exist.
        :param (string) key: the key for the value
        :param (object) value: the value to add
        :param (int or list of int) records: the record(s) for the key/value mappings.
            If this parameter is omitted, then the data is added to a new record
        :return:  
        """
        value = python_to_thrift(value)
        records = records or record
        if records is None:
            return self.client.addKeyValue(key, value, self.creds,
                                           self.transaction, self.environment)
        elif isinstance(records, list):
            return self.client.addKeyValueRecords(key, value, records,
                                                  self.creds, self.transaction, self.environment)
        elif isinstance(records, (int, long)):
            return self.client.addKeyValueRecord(key, value, records,
                                                 self.creds, self.transaction, self.environment)
        else:
            raise ValueError("Must specify records as either an integer, long, list of integers or list of longs")

    def audit(self, key=None, record=None, start=None, end=None):
        """

        :param kwargs:
        :return:
        """
        start = start if not isinstance(start, basestring) else strtotime(start)
        end = end if not isinstance(end, basestring) else strtotime(end)
        if key and record and start and end:
            return self.client.auditKeyRecordStartEnd(key, record, start, end, self.creds, self.transaction,
                                                      self.environment)
        elif key and record and start:
            return self.client.auditKeyRecordStart(key, record, start, self.creds, self.transaction, self.environment)
        elif key and record:
            return self.client.auditKeyRecord(key, record, self.creds, self.transaction, self.environment)
        elif record and start and end:
            return self.client.auditKeyRecordStartEnd(record, start, end, self.creds, self.transaction,
                                                      self.environment)
        elif record and start:
            return self.client.auditKeyRecordStart(record, start, self.creds, self.transaction, self.environment)
        else:
            return self.client.auditRecord(record, self.creds, self.transaction, self.environment)

    def browse(self, keys=None, key=None, timestamp=None):
        """

        :param keys:
        :param timestamp:
        :return:
        """
        keys = keys or key
        timestamp_is_string = isinstance(timestamp, basestring)
        if isinstance(keys, list) and timestamp and not timestamp_is_string:
            data = self.client.browseKeysTime(keys, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and timestamp and timestamp_is_string:
            data = self.client.browseKeysTimestr(keys, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list):
            data = self.client.browseKeys(keys, self.creds, self.transaction, self.environment)
        elif timestamp and not timestamp_is_string:
            data = self.client.browseKeyTime(keys, timestamp, self.creds, self.transaction, self.environment)
        elif timestamp and timestamp_is_string:
            data = self.client.browseKeyTimestr(keys, timestamp, self.creds, self.transaction, self.environment)
        else:
            data = self.client.browseKey(keys, self.creds, self.transaction, self.environment)
        return pythonify(data)

    def chronologize(self, key, record, start=None, end=None):
        """

        :param key:
        :param record:
        :param start:
        :param end:
        :return:
        """
        start = start if not isinstance(start, basestring) else strtotime(start)
        end = end if not isinstance(end, basestring) else strtotime(end)
        if start and end:
            data = self.client.chronologizeKeyRecordStartEnd(key, record, start, end, self.creds, self.transaction,
                                                             self.environment)
        elif start:
            data = self.client.chronologizeKeyRecordStart(key, record, start, self.creds, self.transaction,
                                                          self.environment)
        else:
            data = self.client.chronologizeKeyRecord(key, record, self.creds, self.transaction, self.environment)
        return pythonify(data)

    def clear(self, keys=None, key=None, records=None, record=None):
        """

        :param keys:
        :param key:
        :param records:
        :param record:
        :return:
        """
        keys = keys or key
        records = records or record
        if isinstance(keys, list) and isinstance(records, list):
            return self.client.clearKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and not keys:
            return self.client.clearRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records:
            return self.client.clearKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and not keys:
            return self.client.clearKeyRecords(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records:
            return self.client.clearKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif records:
            return self.client.clearRecord(records, self.creds, self.transaction, self.environment)
        else:
            raise StandardError

    def commit(self):
        """

        :return:
        """
        token = self.transaction
        self.transaction = None
        return self.client.commit(self.creds, token, self.environment)

    def describe(self, records=None, record=None, timestamp=None):
        """

        :param records:
        :param record:
        :param timestamp:
        :return:
        """
        timestamp = timestamp if not isinstance(timestamp, basestring) else strtotime(timestamp)
        records = records or record
        if isinstance(records, list) and timestamp:
            return self.client.describeRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            return self.client.describeRecords(records, self.creds, self.transaction, self.environment)
        elif timestamp:
            return self.client.describeRecordTime(records, timestamp, self.creds, self.transaction, self.environment)
        else:
            return self.client.describeRecord(records, self.creds, self.transaction, self.environment)

    def close(self):
        """

        :return:
        """
        self.exit()

    def exit(self):
        """

        :return:
        """
        self.client.logout(self.creds, self.environment)
        self.transport.close()

    def find(self, criteria=None):
        """

        :param criteria:
        :return:
        """
        if criteria:
            return self.client.findCcl(criteria, self.creds, self.transaction, self.environment)
        else:
            return self.client.find(self.creds, self.transaction, self.environment)

    def get(self, keys=None, key=None, criteria=None, where=None, records=None, record=None, timestamp=None):
        """

        :param keys:
        :param criteria:
        :param records:
        :param timestamp:
        :return:
        """
        criteria = criteria or where
        keys = keys or key
        records = records or record
        timestamp = timestamp if not isinstance(timestamp, basestring) else strtotime(timestamp)
        if isinstance(records, list) and not keys and not timestamp:
            data = self.client.getRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and not keys:
            data = self.client.getRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and not timestamp:
            data = self.client.getKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp:
            data = self.client.getKeysRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                  self.environment)
        elif isinstance(keys, list) and criteria and not timestamp:
            data = self.client.getKeysCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp:
            data = self.client.getKeysCclTime(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and not timestamp:
            data = self.client.getKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and timestamp:
            data = self.client.getKeysRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                 self.environment)
        elif criteria and not keys and not timestamp:
            data = self.client.getCcl(criteria, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and not keys:
            data = self.client.getCclTime(criteria, self.creds, self.transaction, self.environment)
        elif records and not keys and not timestamp:
            data = self.client.getRecord(records, self.creds, self.transaction, self.environment)
        elif records and timestamp and not keys:
            data = self.client.getRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and criteria and not timestamp:
            data = self.client.getKeyCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif keys and criteria and timestamp:
            data = self.client.getKeyCclTime(keys, criteria, timestamp, self.creds, self.transaction,
                                             self.environment)
        elif keys and records and not timestamp:
            data = self.client.getKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and timestamp:
            data = self.client.getKeyRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                self.environment)
        else:
            raise StandardError
        return pythonify(data)

    def get_server_environment(self):
        return self.client.getServerEnvironment(self.creds, self.transaction, self.environment)

    def get_server_version(self):
        return self.client.getServerVersion()

    def insert(self, data, records=None, record=None):
        """

        :param data:
        :param records:
        :param record:
        :return:
        """
        records = records or record
        if isinstance(data, dict):
            data = ujson.dumps(data)

        if isinstance(records, list):
            return self.client.insertJsonRecords(data, records, self.creds, self.transaction, self.environment)
        elif records:
            return self.client.insertJsonRecord(data, records, self.creds, self.transaction, self.environment)
        else:
            return self.client.insertJson(data, self.creds, self.transaction, self.environment)

    def link(self, key, source, destinations=None, destination=None):
        """

        :param key:
        :param source:
        :param destinations:
        :param destination:
        :return:
        """
        destinations = destinations or destination
        if isinstance(destinations, list):
            return self.add(key, Link.to(destinations), source)
        else:
            data = dict()
            for dest in destinations:
                data[dest] = self.add(key, Link.to(destination), source)
            return data

    def ping(self, records, record=None):
        """

        :param records:
        :return:
        """
        records = records or record
        if isinstance(records, list):
            return self.client.pingRecords(records, self.creds, self.transaction, self.environment)
        else:
            return self.client.pingRecord(records, self.creds, self.transaction, self.environment)

    def remove(self, key, value, records=None, record=None):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
        value = python_to_thrift(value)
        records = records or record
        if isinstance(records, list):
            return self.client.removeKeyValueRecords(key, value, records, self.creds, self.transaction,
                                                     self.environment)
        else:
            return self.client.removeKeyValueRecord(key, value, records, self.creds, self.transaction, self.environment)

    def revert(self, keys=None, key=None, records=None, record=None, timestamp=None):
        """

        :param keys:
        :param records:
        :param timestamp:
        :return:
        """
        keys = keys or key
        records = records or record
        timestamp = timestamp if not isinstance(timestamp, basestring) else strtotime(timestamp)
        if not timestamp:
            raise ValueError
        elif isinstance(keys, list) and isinstance(records, list):
            self.client.revertKeysRecordsTime(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list):
            self.client.revertKeysRecordTime(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            self.client.revertKeyRecordsTime(keys, records, self.creds, self.transaction, self.environment)
        else:
            self.client.revertKeyRecordTime(keys, records, timestamp, self.creds, self.transaction, self.environment)

    def search(self, key, query):
        """

        :param key:
        :param query:
        :return:
        """
        return self.client.search(key, query, self.creds, self.transaction, self.environment)

    def select(self, keys=None, key=None, criteria=None, records=None, record=None, timestamp=None, **kwargs):
        """

        :param keys:
        :param criteria:
        :param records:
        :param timestamp:
        :return:
        """
        keys = keys or key
        records = records or record
        criteria = criteria or kwargs.get('ccl') or kwargs.get('query')
        timestamp_is_string = isinstance(timestamp, basestring)
        if isinstance(records, list) and not keys and not timestamp:
            data = self.client.selectRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and not timestamp_is_string and not keys:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and timestamp_is_string and not keys:
            data = self.client.selectRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and not timestamp:
            data = self.client.selectKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp and not timestamp_is_string:
            data = self.client.selectKeysRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                     self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp and timestamp_is_string:
            data = self.client.selectKeysRecordsTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                        self.environment)
        elif isinstance(keys, list) and criteria and not timestamp:
            data = self.client.selectKeysCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp and not timestamp_is_string:
            data = self.client.selectKeysCclTime(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp and timestamp_is_string:
            data = self.client.selectKeysCclTimestr(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and not timestamp:
            data = self.client.selectKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and timestamp and not timestamp_is_string:
            data = self.client.selectKeysRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif isinstance(keys, list) and records and timestamp and timestamp_is_string:
            data = self.client.selectKeysRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                       self.environment)
        elif criteria and not keys and not timestamp:
            data = self.client.selectCcl(criteria, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and not timestamp_is_string and not keys:
            data = self.client.selectCclTime(criteria, timestamp, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and timestamp_is_string and not keys:
            data = self.client.selectCclTimestr(criteria, timestamp, self.creds, self.transaction, self.environment)
        elif records and not keys and not timestamp:
            data = self.client.selectRecord(records, self.creds, self.transaction, self.environment)
        elif records and timestamp and not timestamp_is_string and not keys:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and timestamp and timestamp_is_string and not keys:
            data = self.client.selectRecordTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and criteria and not timestamp:
            data = self.client.selectKeyCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif keys and criteria and timestamp and not timestamp_is_string:
            data = self.client.selectKeyCclTime(keys, criteria, timestamp, self.creds, self.transaction,
                                                self.environment)
        elif keys and criteria and timestamp and timestamp_is_string:
            data = self.client.selectKeyCclTimestr(keys, criteria, timestamp, self.creds, self.transaction,
                                                   self.environment)
        elif keys and records and not timestamp:
            data = self.client.selectKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and timestamp and not timestamp_is_string:
            data = self.client.selectKeyRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                   self.environment)
        elif keys and records and timestamp and timestamp_is_string:
            data = self.client.selectKeyRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                      self.environment)
        else:
            raise StandardError
        return pythonify(data)

    def set(self, key, value, records=None, record=None):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
        records = records or record
        value = python_to_thrift(value)
        if not records:
            return self.client.setKeyValue(key, value, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            self.client.setKeyValueRecords(key, value, records, self.creds, self.transaction, self.environment)
        else:
            self.client.setKeyValueRecord(key, value, records, self.creds, self.transaction, self.environment)

    def stage(self):
        """

        :return:
        """
        self.transaction = self.client.stage(self.creds, self.environment)

    def unlink(self, key, source, destination):
        """

        :param key:
        :param source:
        :param destination:
        :return:
        """
        return self.client.unlink(key, source, destination, self.creds, self.transaction, self.environment)

    def verify(self, key, value, record, timestamp=None):
        value = python_to_thrift(value)
        timestamp = timestamp if not isinstance(timestamp, basestring) else strtotime(timestamp)
        if not timestamp:
            return self.client.verifyKeyValueRecord(
                key,
                value,
                record,
                self.creds,
                self.transaction,
                self.environment)
        else:
            return self.client.verifyKeyValueRecordTime(
                key,
                value,
                record,
                timestamp,
                self.creds,
                self.transaction,
                self.environment)

    def verify_and_swap(self, key, expected, record, replacement):
        """

        :param key:
        :param expected:
        :param record:
        :param replacement:
        :return:
        """
        expected = python_to_thrift(expected)
        replacement = python_to_thrift(replacement)
        return self.client.verifyAndSwap(key, expected, record, replacement, self.creds,  self.transaction,
                                         self.environment)

    def verify_or_set(self, key, value, record):
        """

        :param key:
        :param value:
        :param record:
        :return:
        """
        value = python_to_thrift(value)
        return self.client.verifyOrSet(key, value, record, self.creds, self.transaction, self.environment)



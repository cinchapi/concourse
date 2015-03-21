from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thriftapi import ConcourseService
from thriftapi.ttypes import *
from thriftapi.shared.ttypes import *
from utils import *


class Concourse:

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
        Create a new client connection to the specified environment of the specified Concourse Server
        and return a handle to facilitate interaction.
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

    def add(self, key, value, records=None):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
        value = python_to_thrift(value)
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
            raise ValueError

    def audit(self, key=None, record=None, start=None, end=None):
        """

        :param kwargs:
        :return:
        """
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

    def browse(self, keys, timestamp=None):
        """

        :param keys:
        :param timestamp:
        :return:
        """
        # todo need to transform values
        if isinstance(keys, list):
            if timestamp:
                return self.client.browseKeysTime(keys, timestamp, self.creds, self.transaction, self.environment)
            else:
                return self.client.browseKeys(keys, self.creds, self.transaction,
                                              self.environment)
        else:
            if timestamp:
                return self.client.browseKeyTime(
                    keys,
                    self.creds,
                    self.transaction,
                    self.environment)
            else:
                return self.client.browseKey(
                    keys,
                    self.creds,
                    self.transaction,
                    self.environment)

    def ping(self, records, record=None):
        """

        :param records:
        :return:
        """
        records = records if records else record
        if isinstance(records, list):
            return self.client.pingRecords(records, self.creds, self.transaction, self.environment)
        else:
            return self.client.pingRecord(records, self.creds, self.transaction, self.environment)

    def remove(self, key, value, records, record=None):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
        records = records if records else record
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
        keys = keys if keys else key
        records = records if records else record
        if not timestamp:
            raise ValueError
        elif isinstance(keys, list) and isinstance(records, list):
            self.client.revertKeysRecordsTime(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list):
            self.client.revertKeysRecordTime(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            self.client.revertKeyRecordsTime(keys, records, self.creds, self.transaction, self.environment)
        else:
            self.client.revertKeyRecordsTime(keys, records, self.creds, self.transaction, self.environment)

    def search(self, key, query):
        """

        :param key:
        :param query:
        :return:
        """
        return self.client.search(key, query, self.creds, self.transaction, self.environment)

    def select(self, keys=None, key=None, criteria=None, records=None, record=None, timestamp=None):
        """

        :param keys:
        :param criteria:
        :param records:
        :param timestamp:
        :return:
        """
        keys = keys if keys else key
        records = records if records else record
        if isinstance(records, list) and not keys and not timestamp:
            data = self.client.selectRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and not keys:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and not timestamp:
            data = self.client.selectKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp:
            data = self.client.selectKeysRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                     self.environment)
        elif isinstance(keys, list) and criteria and not timestamp:
            data = self.client.selectKeysCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp:
            data = self.client.selectKeysCclTime(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and not timestamp:
            data = self.client.selectKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and timestamp:
            data = self.client.selectKeysRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif criteria and not keys and not timestamp:
            data = self.client.selectCcl(criteria, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and not keys:
            data = self.client.selectCclTime(criteria, self.creds, self.transaction, self.environment)
        elif records and not keys and not timestamp:
            data = self.client.selectRecord(records, self.creds, self.transaction, self.environment)
        elif records and timestamp and not keys:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and criteria and not timestamp:
            data = self.client.selectKeyCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif keys and criteria and timestamp:
            data = self.client.selectKeyCclTime(keys, criteria, timestamp, self.creds, self.transaction,
                                                self.environment)
        elif keys and records and not timestamp:
            data = self.client.selectKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and timestamp:
            data = self.client.selectKeyRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                   self.environment)
        else:
            raise StandardError
        return pythonify(data)

    def set(self, key, value, records):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
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



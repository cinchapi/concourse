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
        Authenticate the username and password and locally store the AccessToken to use with
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
        value = Convert.python_to_thrift(value)
        if records is None:
            return self.client.addKeyValue(key, value, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            return self.client.addKeyValueRecords(key, value, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, (int, long)):
            return self.client.addKeyValueRecord(key, value, records, self.creds, self.transaction, self.environment)
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
                return self.client.browseKeys(keys, self.creds, self.transaction, self.environment)
        else:
            if timestamp:
                return self.client.browseKeyTime(keys, self.creds, self.transaction, self.environment)
            else:
                return self.client.browseKey(keys, self.creds, self.transaction, self.environment)



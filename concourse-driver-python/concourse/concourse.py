from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from rpc import ConcourseService
from rpc.ttypes import *
from rpc.shared.ttypes import *
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
        elif isinstance(records, int):
            return self.client.addKeyValueRecord(key, value, records, self.creds, self.transaction, self.environment)
        else:
            raise ValueError



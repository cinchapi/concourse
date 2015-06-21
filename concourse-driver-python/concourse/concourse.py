__author__ = "Jeff Nelson"
__copyright__ = "Copyright 2015, Cinchapi, Inc."
__license__ = "Apache, Version 2.0"

from thrift import Thrift
from thrift.transport import TSocket
from thriftapi import ConcourseService
from thriftapi.shared.ttypes import *
from utils import *
from collections import OrderedDict
import ujson
from configparser import ConfigParser
import itertools
import os


class Concourse(object):
    """ Concourse is a self-tuning database that makes it easier to quickly build reliable and scalable systems.
    Concourse dynamically adapts to any application and offers features like automatic indexing, version control, and
    distributed ACID transactions within a big data platform that manages itself, reduces costs and allows developers
    to focus on what really matters.

    Data Model:

    The Concourse data model is lightweight and flexible. Unlike other databases, Concourse is completely schemaless and
    does not hold data in tables or collections. Concourse is simply a distributed document-graph. All data is
    stored in records (similar to documents or rows in other databases). Each record has multiple keys. And each
    key has one or more distinct values. Like any graph, you can link records to one another. And the structure of one
    record does not affect the structure of another.

    - Record: A logical grouping of data about a single person, place or thing (i.e. an object). Each record is
    identified by a unique primary key.
    - Key: A attribute that maps to one or more distinct values.
    - Value: A dynamically typed quantity.

    Data Types:

    Concourse natively stores the following primitives: bool, double, integer, string (UTF-8) and Tag (a string that is
    not full text searchable). Any other data type will be stored as its __str__ representation.

    Links:

    Concourse allows linking a key in one record to another record using the link() function. Links are retrievable and
    queryable just like any other value.

    Transactions:

    By default, Concourse conducts every operation in autocommit mode where every change is immediately written.
    You can also stage a group of operations in an ACID transaction. Transactions are managed using the stage(),
    commit() and abort() commands.

    Version Control:

    Concourse automatically tracks every changes to data and the API exposes several methods to tap into this feature.
    1) You can get() and select() previous version of data by specifying a timestamp using natural language or a unix
       timestamp integer in microseconds.
    2) You can browse() and find() records that matched a criteria in the past by specifying a timestamp using natural
       language or a unix timestamp integer in microseconds.
    3) You can audit() and diff() changes over time, revert() to previous states and  chronologize() how data has
        evolved within a range of time.
    """

    @staticmethod
    def connect(host="localhost", port=1717, username="admin", password="admin", environment="", **kwargs):
        """ This is an alias for the constructor.
        """
        return Concourse(host, port, username, password, environment, **kwargs)

    def __init__(self, host="localhost", port=1717, username="admin", password="admin", environment="", **kwargs):
        """ Initialize a new client connection

        :param host: the server host (default: localhost)
        :param port: the listener post (default: 1717)
        :param username: the username with which to connect (default: admin)
        :param password: the password for the username (default: admin)
        :param environment: the environment to use, (default: the 'default_environment' in the server's
                            concourse.prefs file)

        You may specify the path to a preferences file using the 'prefs' keyword argument. If a prefs file
        is supplied, the values contained therewithin for any of arguments above become the default
        if the arguments are not explicitly given values.

        :return: the handle
        """
        username = username or find_in_kwargs_by_alias('username', kwargs)
        password = password or find_in_kwargs_by_alias('password', kwargs)
        prefs = find_in_kwargs_by_alias('prefs', kwargs)
        if prefs:
            # Hack to use ConfigParser with java style properties file
            with open(os.path.abspath(os.path.expanduser(prefs))) as stream:
                lines = itertools.chain(("[default]",), stream)
                prefs = ConfigParser()
                prefs.read_file(lines)
                prefs = dict(prefs._sections['default'])
        else:
            prefs = {}
        self.host = prefs.get('host', host)
        self.port = int(prefs.get('port', port))
        self.username = prefs.get('username', username)
        self.password = prefs.get('password', password)
        self.environment = prefs.get('environment', environment)
        try:
            transport = TSocket.TSocket(self.host, self.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self.client = ConcourseService.Client(protocol)
            transport.open()
            self.transport = transport
            self.__authenticate()
            self.transaction = None
        except Thrift.TException:
            raise RuntimeError("Could not connect to the Concourse Server at "+self.host+":"+str(self.port))

    def __authenticate(self):
        """ Internal method to login with the username/password and locally store the AccessToken for use with
        subsequent operations.
        """
        try:
            self.creds = self.client.login(self.username, self.password, self.environment)
        except Thrift.TException as e:
            raise e

    def abort(self):
        """ Abort the current transaction and discard any changes that were staged. After returning, the
        driver will return to autocommit mode and all subsequent changes will be committed immediately.
        """
        if self.transaction:
            token = self.transaction
            self.transaction = None
            self.client.abort(self.creds, token, self.environment)

    def add(self, key, value, records=None, **kwargs):
        """ Add a a value to a key within a record if it does not exist.

        :param key: string
        :param value: object
        :param record: int (optional) or records: list (optional)

        :return: 1) a boolean that indicates whether the value was added, if a record is supplied 2) a dict mapping
        record to a boolean that indicates whether the value was added, if a list of records is supplied 3) the id of
        the new record where the data was added, if not record is supplied as an argument
        """
        value = python_to_thrift(value)
        records = records or kwargs.get('record')
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
            require_kwarg('record or records')

    def audit(self, key=None, record=None, start=None, end=None, **kwargs):
        """ Return a log of revisions

        :param key:string (optional)
        :param record:int
        :param start:string|int (optional)
        :param end:string|int (optional)

        :return: a dict mapping a timestamp to a description of changes
        """
        start = start or find_in_kwargs_by_alias('timestamp', kwargs)
        startstr = isinstance(start, basestring)
        endstr = isinstance(end, basestring)
        if isinstance(key, int):
            record = key
            key = None
        if key and record and start and not startstr and end and not endstr:
            data = self.client.auditKeyRecordStartEnd(key, record, start, end, self.creds, self.transaction,
                                                      self.environment)
        elif key and record and start and startstr and end and endstr:
            data = self.client.auditKeyRecordStartstrEndstr(key, record, start, end, self.creds, self.transaction,
                                                            self.environment)
        elif key and record and start and not startstr:
            data = self.client.auditKeyRecordStart(key, record, start, self.creds, self.transaction, self.environment)
        elif key and record and start and startstr:
            data = self.client.auditKeyRecordStartstr(key, record, start, self.creds, self.transaction, self.environment)
        elif key and record:
            data = self.client.auditKeyRecord(key, record, self.creds, self.transaction, self.environment)
        elif record and start and not startstr and end and not endstr:
            data = self.client.auditRecordStartEnd(record, start, end, self.creds, self.transaction,
                                                   self.environment)
        elif record and start and startstr and end and endstr:
            data = self.client.auditRecordStartstrEndstr(record, start, end, self.creds, self.transaction,
                                                         self.environment)
        elif record and start and not startstr:
            data = self.client.auditRecordStart(record, start, self.creds, self.transaction, self.environment)
        elif record and start and startstr:
            data = self.client.auditRecordStartstr(record, start, self.creds, self.transaction, self.environment)
        elif record:
            data = self.client.auditRecord(record, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('record')
        data = OrderedDict(sorted(data.items()))
        return data

    def browse(self, keys=None, timestamp=None, **kwargs):
        """ Return a view of all the values indexed for a key or group of keys.

        :param key: string or keys: list
        :param timestamp:string (optional)

        :return: 1) a dict mapping a value to a set of records containing the value if a single key is specified or
        2) a dict mapping a key to a dict mapping a value to set of records containing that value of a list of keys
        is specified
        """
        keys = keys or kwargs.get('key')
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
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
        elif keys:
            data = self.client.browseKey(keys, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('key or keys')
        return pythonify(data)

    def chronologize(self, key, record, start=None, end=None, **kwargs):
        """

        :param key:
        :param record:
        :param start:
        :param end:
        :return:
        """
        start = start or find_in_kwargs_by_alias('timestamp', kwargs)
        startstr = isinstance(start, basestring)
        endstr = isinstance(end, basestring)
        if start and not startstr and end and not endstr:
            data = self.client.chronologizeKeyRecordStartEnd(key, record, start, end, self.creds, self.transaction,
                                                             self.environment)
        elif start and startstr and end and endstr:
            data = self.client.chronologizeKeyRecordStartstrEndstr(key, record, start, end, self.creds, self.transaction,
                                                                   self.environment)
        elif start and not startstr:
            data = self.client.chronologizeKeyRecordStart(key, record, start, self.creds, self.transaction,
                                                          self.environment)
        elif start and startstr:
            data = self.client.chronologizeKeyRecordStartstr(key, record, start, self.creds, self.transaction,
                                                             self.environment)
        else:
            data = self.client.chronologizeKeyRecord(key, record, self.creds, self.transaction, self.environment)
        data = OrderedDict(sorted(data.items()))
        return pythonify(data)

    def clear(self, keys=None, records=None, **kwargs):
        """

        :param keys:
        :param key:
        :param records:
        :param record:
        :return:
        """
        keys = keys or kwargs.get('key')
        records = records or kwargs.get('record')
        if isinstance(keys, list) and isinstance(records, list):
            return self.client.clearKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and not keys:
            return self.client.clearRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records:
            return self.client.clearKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and keys:
            return self.client.clearKeyRecords(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records:
            return self.client.clearKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif records:
            return self.client.clearRecord(records, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('record or records')

    def commit(self):
        """

        :return:
        """
        token = self.transaction
        self.transaction = None
        return self.client.commit(self.creds, token, self.environment)

    def describe(self, records=None, timestamp=None, **kwargs):
        """

        :param records:
        :param record:
        :param timestamp:
        :return:
        """
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, basestring)
        records = records or kwargs.get('record')
        if isinstance(records, list) and timestamp and not timestr:
            return self.client.describeRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and timestr:
            return self.client.describeRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            return self.client.describeRecords(records, self.creds, self.transaction, self.environment)
        elif timestamp and not timestr:
            return self.client.describeRecordTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif timestamp and timestr:
            return self.client.describeRecordTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        else:
            return self.client.describeRecord(records, self.creds, self.transaction, self.environment)

    def diff(self, key=None, record=None, start=None, end=None, **kwargs):
        """

        :param key:
        :param record:
        :param start:
        :param end:
        :param kwargs:
        :return:
        """
        start = start or find_in_kwargs_by_alias('timestamp', kwargs)
        startstr = isinstance(start, basestring)
        endstr = isinstance(end, basestring)
        if key and record and start and not startstr and end and not endstr:
            data = self.client.diffKeyRecordStartEnd(key, record, start, end, self.creds, self.transaction,
                                                     self.environment)
        elif key and record and start and startstr and end and endstr:
            data = self.client.diffKeyRecordStartstrEndstr(key, record, start, end, self.creds, self.transaction,
                                                           self.environment)
        elif key and record and start and not startstr:
            data = self.client.diffKeyRecordStart(key, record, start, self.creds, self.transaction, self.environment)
        elif key and record and start and startstr:
            data = self.client.diffKeyRecordStartstr(key, record, start, self.creds, self.transaction, self.environment)
        elif key and start and not startstr and end and not endstr:
            data = self.client.diffKeyStartEnd(key, start, end, self.creds, self.transaction, self.environment)
        elif key and start and startstr and end and endstr:
            data = self.client.diffKeyStartstrEndstr(key, start, end, self.creds, self.transaction, self.environment)
        elif key and start and not startstr:
            data = self.client.diffKeyStart(key, start, self.creds, self.transaction, self.environment)
        elif key and start and startstr:
            data = self.client.diffKeyStartstr(key, start, self.creds, self.transaction, self.environment)
        elif record and start and not startstr and end and not endstr:
            data = self.client.diffRecordStartEnd(record, start, end, self.creds, self.transaction, self.environment)
        elif record and start and startstr and end and endstr:
            data = self.client.diffRecordStartstrEndstr(record, start, end, self.creds, self.transaction,
                                                        self.environment)
        elif record and start and not startstr:
            data = self.client.diffRecordStart(record, start, self.creds, self.transaction, self.environment)
        elif record and start and startstr:
            data = self.client.diffRecordStartstr(record, start, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('start and (record or key)')
        return pythonify(data)

    def close(self):
        """ Close the connection.
        """
        self.exit()

    def exit(self):
        """ Close the connection.
        """
        self.client.logout(self.creds, self.environment)
        self.transport.close()

    def find(self, criteria=None, **kwargs):
        """

        :param criteria:
        :return:
        """
        criteria = criteria or find_in_kwargs_by_alias('criteria', kwargs)
        key = kwargs.get('key')
        operator = kwargs.get('operator')
        operatorstr = isinstance(operator, basestring)
        values = kwargs.get('value') or kwargs.get('values')
        values = [values] if not isinstance(values, list) else values
        values = thriftify(values)
        timestamp = find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, basestring)
        if criteria:
            return self.client.findCcl(criteria, self.creds, self.transaction, self.environment)
        elif key and operator and not operatorstr and values and not timestamp:
            return self.client.findKeyOperatorValues(key, operator, values, self.creds, self.transaction,
                                                     self.environment)
        elif key and operator and operatorstr and values and not timestamp:
            return self.client.findKeyOperatorstrValues(key, operator, values, self.creds, self.transaction,
                                                        self.environment)
        elif key and operator and not operatorstr and values and timestamp and not timestr:
            return self.client.findKeyOperatorValuesTime(key, operator, values, timestamp, self.creds, self.transaction,
                                                         self.environment)
        elif key and operator and operatorstr and values and timestamp and not timestr:
            return self.client.findKeyOperatorstrValuesTime(key, operator, values, timestamp, self.creds,
                                                            self.transaction, self.environment)
        elif key and operator and not operatorstr and values and timestamp and timestr:
            return self.client.findKeyOperatorValuesTimestr(key, operator, values, timestamp, self.creds,
                                                            self.transaction, self.environment)
        elif key and operator and operatorstr and values and timestamp and operatorstr:
            return self.client.findKeyOperatorstrValuesTimestr(key, operator, values, timestamp, self.creds,
                                                               self.transaction, self.environment)
        else:
            require_kwarg('criteria or all of (key, operator and value/s)')

    def get(self, keys=None, criteria=None, records=None, timestamp=None, **kwargs):
        """

        :param keys:
        :param criteria:
        :param records:
        :param timestamp:
        :return:
        """
        criteria = criteria or find_in_kwargs_by_alias('criteria', kwargs)
        keys = keys or kwargs.get('key')
        records = records or kwargs.get('record')
        timestr = isinstance(timestamp, basestring)
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
        elif keys and isinstance(records, list) and not timestamp:
            data = self.client.getKeyRecords(keys, records, self.creds, self.transaction, self.environment)
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

    def insert(self, data, records=None, **kwargs):
        """

        :param data:
        :param records:
        :param record:
        :return:
        """
        data = data or kwargs.get('json')
        records = records or kwargs.get('record')
        if isinstance(data, dict):
            data = ujson.dumps(data)

        if isinstance(records, list):
            return self.client.insertJsonRecords(data, records, self.creds, self.transaction, self.environment)
        elif records:
            return self.client.insertJsonRecord(data, records, self.creds, self.transaction, self.environment)
        else:
            return self.client.insertJson(data, self.creds, self.transaction, self.environment)

    def link(self, key, source, destinations=None, **kwargs):
        """

        :param key:
        :param source:
        :param destinations:
        :param destination:
        :return:
        """
        destinations = destinations or kwargs.get('destination')
        if isinstance(destinations, list):
            return self.add(key, Link.to(destinations), source)
        else:
            data = dict()
            for dest in destinations:
                data[dest] = self.add(key, Link.to(dest), source)
            return data

    def logout(self):
        self.client.logout(self.creds, self.environment)

    def ping(self, records, **kwargs):
        """

        :param records:
        :return:
        """
        records = records or kwargs.get('record')
        if isinstance(records, list):
            return self.client.pingRecords(records, self.creds, self.transaction, self.environment)
        else:
            return self.client.pingRecord(records, self.creds, self.transaction, self.environment)

    def remove(self, key, value, records=None, **kwargs):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
        value = python_to_thrift(value)
        records = records or kwargs.get('record')
        if isinstance(records, list):
            return self.client.removeKeyValueRecords(key, value, records, self.creds, self.transaction,
                                                     self.environment)
        elif isinstance(records, (int, long)):
            return self.client.removeKeyValueRecord(key, value, records, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('record or records')

    def revert(self, keys=None, records=None, timestamp=None, **kwargs):
        """

        :param keys:
        :param records:
        :param timestamp:
        :return:
        """
        keys = keys or kwargs.get('key')
        records = records or kwargs.get('record')
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, basestring)
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

    def select(self, keys=None, criteria=None, records=None, timestamp=None, **kwargs):
        """

        :param keys:
        :param criteria:
        :param records:
        :param timestamp:
        :return:
        """
        keys = keys or kwargs.get('key')
        records = records or kwargs.get('record')
        criteria = criteria or find_in_kwargs_by_alias('criteria', kwargs)
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
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
        elif keys and isinstance(records, list) and not timestamp:
            data = self.client.selectKeyRecords(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and not timestamp:
            data = self.client.selectKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and timestamp and not timestamp_is_string:
            data = self.client.selectKeyRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                   self.environment)
        elif keys and records and timestamp and timestamp_is_string:
            data = self.client.selectKeyRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                      self.environment)
        else:
            require_kwarg('record or records')
        return pythonify(data)

    def set(self, key, value, records=None, **kwargs):
        """

        :param key:
        :param value:
        :param records:
        :return:
        """
        records = records or kwargs.get('record')
        value = python_to_thrift(value)
        if not records:
            return self.client.setKeyValue(key, value, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            self.client.setKeyValueRecords(key, value, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, (int, long)):
            self.client.setKeyValueRecord(key, value, records, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('record or records')

    def stage(self):
        """

        :return:
        """
        self.transaction = self.client.stage(self.creds, self.environment)

    def time(self, phrase=None):
        """

        :param phrase:
        :return:
        """
        if phrase:
            return self.client.timePhrase(phrase, self.creds, self.transaction, self.environment)
        else:
            return self.client.time(self.creds, self.transaction, self.environment)

    def unlink(self, key, source, destination):
        """

        :param key:
        :param source:
        :param destination:
        :return:
        """
        return self.client.unlink(key, source, destination, self.creds, self.transaction, self.environment)

    def verify(self, key, value, record, timestamp=None, **kwargs):
        value = python_to_thrift(value)
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timesttr = isinstance(timestamp, basestring)
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
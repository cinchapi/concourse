__author__ = "Jeff Nelson"
__copyright__ = "Copyright 2015, Cinchapi Inc."
__license__ = "Apache, Version 2.0"

from thrift import Thrift
from thrift.transport import TSocket
from .thriftapi import ConcourseService
from .thriftapi.shared.ttypes import *
from .utils import *
from collections import OrderedDict
from io import BytesIO
from configparser import ConfigParser
import ujson
import itertools
import os
import types


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

            # Edit the buffer attributes in the transport to use BytesIO
            setattr(transport, '_TBufferedTransport__wbuf', BytesIO())
            setattr(transport, '_TBufferedTransport__rbuf', BytesIO(b""))

            # Edit the write method of the transport to encode data
            def write(slf, buf):
                try:
                    slf._TBufferedTransport__wbuf.write(buf)
                except TypeError:
                    buf = bytes(buf, 'utf-8')
                    slf.write(buf)
                except Exception as e:
                    # on exception reset wbuf so it doesn't contain a partial function call
                    self._TBufferedTransport__wbuf = BytesIO
                    raise e
            transport.write = types.MethodType(write, transport)

            # Edit the flush method of the transport to use BytesIO
            def flush(slf):
                out = slf._TBufferedTransport__wbuf.getvalue()
                # reset wbuf before write/flush to preserve state on underlying failure
                slf._TBufferedTransport__wbuf = BytesIO()
                slf._TBufferedTransport__trans.write(out)
                slf._TBufferedTransport__trans.flush()
            transport.flush = types.MethodType(flush, transport)

            # Edit the read method of the transport to use BytesIO
            def read(slf, sz):
                ret = slf._TBufferedTransport__rbuf.read(sz)
                if len(ret) != 0:
                    return ret

                slf._TBufferedTransport__rbuf = BytesIO(slf._TBufferedTransport__trans.read(max(sz, slf._TBufferedTransport__rbuf_size)))
                return slf._TBufferedTransport__rbuf.read(sz)
            transport.read = types.MethodType(read, transport)

            # Edit the readAll method of the transport to use a bytearray
            def readAll(slf, sz):
                buff = b''
                have = 0
                while have < sz:
                    chunk = slf.read(sz - have)
                    have += len(chunk)
                    buff += chunk
                    if len(chunk) == 0:
                        raise EOFError()
                return buff
            transport.readAll = types.MethodType(readAll, transport)

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
        """ Append **key** as **value** in one or more records.

        Options:
        -------
        * `add(key, value)` - Append *key* as *value* in a new record.
            * :param key: [string] the field name
            * :param value: [object] the value to add
            * :returns: the new record id
        * `add(key, value, record)` - Append *key* as *value* in *record* if and only if it does not exist.
            * :param key: [string] the field name
            * :param value: [object] the value to add
            * :param record: [integer] the record id where an attempt is made to add the data
            * :returns: a bool that indicates if the data was added
        * `add(key, value, records)` - Append *key* as *value* in each of the *records* where it doesn't exist.
            * :param key: [string] the field name
            * :param value: [object] the value to add
            * :param records: [list] a list of record ids where an attempt is made to add the data
            * :returns: a dict mapping each record id to a boolean that indicates if the data was added in that record
        """
        value = python_to_thrift(value)
        records = records or kwargs.get('record')
        if records is None:
            return self.client.addKeyValue(key, value, self.creds,
                                           self.transaction, self.environment)
        elif isinstance(records, list):
            return self.client.addKeyValueRecords(key, value, records,
                                                  self.creds, self.transaction, self.environment)
        elif isinstance(records, int):
            return self.client.addKeyValueRecord(key, value, records,
                                                 self.creds, self.transaction, self.environment)
        else:
            require_kwarg('key and value')

    def audit(self, key=None, record=None, start=None, end=None, **kwargs):
        """ Return a log of revisions.

        :param key:string (optional)
        :param record:int
        :param start:string|int (optional)
        :param end:string|int (optional)

        :return: a dict mapping a timestamp to a description of changes
        """
        start = start or find_in_kwargs_by_alias('timestamp', kwargs)
        startstr = isinstance(start, str)
        endstr = isinstance(end, str)
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
        data = pythonify(data)
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
        timestamp_is_string = isinstance(timestamp, str)
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
        """ Return a chronological view that shows the state of a field (key/record) over a range of time.

        :param key: string
        :param record: int
        :param start: string|int (optional)
        :param end: string|int (optional)
        :return: the chronological view of the field over the specified (or entire) range of time
        """
        start = start or find_in_kwargs_by_alias('timestamp', kwargs)
        startstr = isinstance(start, str)
        endstr = isinstance(end, str)
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
        """ Atomically remove all the data from a field or an entire record.

        :param key: string or keys: list
        :param record: int or records list
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
        """ Commit the currently running transaction.

        :return: True if the transaction commits. Otherwise, False.
        """
        token = self.transaction
        self.transaction = None
        if token:
            return self.client.commit(self.creds, token, self.environment)
        else:
            return False

    def describe(self, records=None, timestamp=None, **kwargs):
        """ Return all keys in a record at the present or the specified timestamp.

        :param record (int) or records (list)
        :param timestamp: string|int (optional)
        :return: a set of keys if a single record if provided, if multiple records are provided, a mapping from the
        record to a set of keys
        """
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, str)
        records = records or kwargs.get('record')
        if isinstance(records, list) and timestamp and not timestr:
            data = self.client.describeRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and timestr:
            data = self.client.describeRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list):
            data = self.client.describeRecords(records, self.creds, self.transaction, self.environment)
        elif timestamp and not timestr:
            data = self.client.describeRecordTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif timestamp and timestr:
            data = self.client.describeRecordTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        else:
            data = self.client.describeRecord(records, self.creds, self.transaction, self.environment)
        return pythonify(data)

    def diff(self, key=None, record=None, start=None, end=None, **kwargs):
        """ Return the differences in a field, record of index from a start timestamp to an end timestamp.

        :param key:
        :param record:
        :param start:
        :param end:
        :return:
        """
        start = start or find_in_kwargs_by_alias('timestamp', kwargs)
        startstr = isinstance(start, str)
        endstr = isinstance(end, str)
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
        operatorstr = isinstance(operator, str)
        values = kwargs.get('value') or kwargs.get('values')
        values = [values] if not isinstance(values, list) else values
        values = thriftify(values)
        timestamp = find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, str)
        if criteria:
            data = self.client.findCcl(criteria, self.creds, self.transaction, self.environment)
        elif key and operator and not operatorstr and values and not timestamp:
            data = self.client.findKeyOperatorValues(key, operator, values, self.creds, self.transaction,
                                                     self.environment)
        elif key and operator and operatorstr and values and not timestamp:
            data = self.client.findKeyOperatorstrValues(key, operator, values, self.creds, self.transaction,
                                                        self.environment)
        elif key and operator and not operatorstr and values and timestamp and not timestr:
            data = self.client.findKeyOperatorValuesTime(key, operator, values, timestamp, self.creds, self.transaction,
                                                         self.environment)
        elif key and operator and operatorstr and values and timestamp and not timestr:
            data = self.client.findKeyOperatorstrValuesTime(key, operator, values, timestamp, self.creds,
                                                            self.transaction, self.environment)
        elif key and operator and not operatorstr and values and timestamp and timestr:
            data = self.client.findKeyOperatorValuesTimestr(key, operator, values, timestamp, self.creds,
                                                            self.transaction, self.environment)
        elif key and operator and operatorstr and values and timestamp and timestr:
            data = self.client.findKeyOperatorstrValuesTimestr(key, operator, values, timestamp, self.creds,
                                                               self.transaction, self.environment)
        else:
            require_kwarg('criteria or all of (key, operator and value/s)')
        data = list(data) if isinstance(data, set) else data
        return data

    def find_or_add(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        value = python_to_thrift(value)
        return self.client.findOrAddKeyValue(key, value, self.creds, self.transaction, self.environment)

    def find_or_insert(self, criteria=None, data=None, **kwargs):
        """

        :param criteria:
        :param data:
        :param kwargs:
        :return:
        """
        data = data or kwargs.get('json')
        if isinstance(data, dict) or isinstance(data, list):
            data = ujson.dumps(data)
        criteria = criteria or find_in_kwargs_by_alias('criteria', kwargs)
        return self.client.findOrInsertCclJson(criteria, data, self.creds, self.transaction, self.environment)

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
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, str)
        # Handle case when kwargs are not used and the second parameter is a the record
        # (e.g. trying to get key/record(s))
        if (isinstance(criteria, int) or isinstance(criteria, list)) and not records:
            records = criteria
            criteria = None
        if isinstance(records, list) and not keys and not timestamp:
            data = self.client.getRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and not timestr and not keys:
            data = self.client.getRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and timestr and not keys:
            data = self.client.getRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and not timestamp:
            data = self.client.getKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp and not timestr:
            data = self.client.getKeysRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                  self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp and timestr:
            data = self.client.getKeysRecordsTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                     self.environment)
        elif isinstance(keys, list) and criteria and not timestamp:
            data = self.client.getKeysCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp and not timestr:
            data = self.client.getKeysCclTime(keys, criteria, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp and timestr:
            data = self.client.getKeysCclTimestr(keys, criteria, timestamp, self.creds, self.transaction,
                                                 self.environment)
        elif isinstance(keys, list) and records and not timestamp:
            data = self.client.getKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and timestamp and not timestr:
            data = self.client.getKeysRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                 self.environment)
        elif isinstance(keys, list) and records and timestamp and timestr:
            data = self.client.getKeysRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif criteria and not keys and not timestamp:
            data = self.client.getCcl(criteria, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and not timestr and not keys:
            data = self.client.getCclTime(criteria, timestamp, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and timestr and not keys:
            data = self.client.getCclTimestr(criteria, timestamp, self.creds, self.transaction, self.environment)
        elif records and not keys and not timestamp:
            data = self.client.getRecord(records, self.creds, self.transaction, self.environment)
        elif records and timestamp and not timestr and not keys:
            data = self.client.getRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and timestamp and timestr and not keys:
            data = self.client.getRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and criteria and not timestamp:
            data = self.client.getKeyCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif keys and criteria and timestamp and not timestr:
            data = self.client.getKeyCclTime(keys, criteria, timestamp, self.creds, self.transaction,
                                             self.environment)
        elif keys and criteria and timestamp and timestr:
            data = self.client.getKeyCclTimestr(keys, criteria, timestamp, self.creds, self.transaction,
                                                self.environment)
        elif keys and isinstance(records, list) and not timestamp:
            data = self.client.getKeyRecords(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and not timestamp:
            data = self.client.getKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif keys and isinstance(records, list) and timestamp and not timestr:
            data = self.client.getKeyRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                 self.environment)
        elif keys and isinstance(records, list) and timestamp and timestr:
            data = self.client.getKeyRecordsTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif keys and records and timestamp and not timestr:
            data = self.client.getKeyRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                self.environment)
        elif keys and records and timestamp and timestr:
            data = self.client.getKeyRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                   self.environment)
        else:
            require_kwarg('criteria or (key and record)')
        return pythonify(data)

    def get_server_environment(self):
        """ Return the environment to which the client is connected.

        :return: the environment
        """
        return self.client.getServerEnvironment(self.creds, self.transaction, self.environment)

    def get_server_version(self):
        """ Return the version of Concourse Server to which the client is connected. Generally speaking, a client cannot
        talk to a newer version of Concourse Server.

        :return: the server version
        """
        return self.client.getServerVersion().decode('utf-8')
        return self.client.getServerVersion().decode('utf-8')

    def insert(self, data, records=None, **kwargs):
        """ Bulk insert data from a dict, a list of dicts or a JSON string. This operation is atomic, within each record.
        An insert can only succeed within a record if all the data can be successfully added, which means the insert
        will fail if any aspect of the data already exists in the record.

        If no record or records are specified, the following behaviour occurs
        - data is a dict or a JSON object:
        The data is inserted into a new record

        - data is a list of dicts or a JSON array of objects:
        Each dict/object is inserted into a new record

        If a record or records are specified, the data must be a JSON object or a dict. In this case, the object/dict is
        inserted into every record specified as an argument to the function.

        :param data (dict | list | string):
        :param record (int) or records(list):
        :return: the list of records into which data was inserted, if no records are specified as method arguments.
        Otherwise, a bool indicating whether the insert was successful if a single record is specified as an argument
        or a dict mapping each specified record to a bool that indicates whether the insert was successful for that
        record
        """
        data = data or kwargs.get('json')
        records = records or kwargs.get('record')
        if isinstance(data, dict) or isinstance(data, list):
            data = ujson.dumps(data)

        if isinstance(records, list):
            result = self.client.insertJsonRecords(data, records, self.creds, self.transaction, self.environment)
        elif records:
            result = self.client.insertJsonRecord(data, records, self.creds, self.transaction, self.environment)
        else:
            result = self.client.insertJson(data, self.creds, self.transaction, self.environment)
        result = list(result) if isinstance(result, set) else result
        return result

    def inventory(self):
        """ Return a list of all the records that have any data.

        :return: the inventory
        """
        data = self.client.inventory(self.creds, self.transaction, self.environment)
        return list(data) if isinstance(data, set) else data

    def jsonify(self, records=None, include_id=False, timestamp=None, **kwargs):
        """

        :param records:
        :param include_id:
        :param timestamp:
        :param kwargs:
        :return:
        """
        records = records or kwargs.get('record')
        records = list(records) if not isinstance(records, list) else records
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        include_id = include_id or kwargs.get('id', False)
        timestr = isinstance(timestamp, str)
        if not timestamp:
            return self.client.jsonifyRecords(records, include_id, self.creds, self.transaction, self.environment)
        elif timestamp and not timestr:
            return self.client.jsonifyRecordsTime(records, timestamp, include_id, self.creds, self.transaction,
                                                  self.environment)
        elif timestamp and timestr:
            return self.client.jsonifyRecordsTimestr(records, timestamp, include_id, self.creds, self.transaction,
                                                     self.environment)
        else:
            require_kwarg('record or records')

    def link(self, key, source, destinations=None, **kwargs):
        """

        :param key:
        :param source:
        :param destinations:
        :param destination:
        :return:
        """
        destinations = destinations or kwargs.get('destination')
        if not isinstance(destinations, list):
            return self.add(key, Link.to(destinations), source)
        else:
            data = dict()
            for dest in destinations:
                data[dest] = self.add(key, Link.to(dest), source)
            return data

    def logout(self):
        """

        :return:
        """
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
        elif isinstance(records, int):
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
        timestr = isinstance(timestamp, str)
        if isinstance(keys, list) and isinstance(records, list) and timestamp and not timestr:
            self.client.revertKeysRecordsTime(keys, records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and isinstance(records, list) and timestamp and timestr:
            self.client.revertKeysRecordsTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                 self.environment)
        elif isinstance(keys, list) and records and timestamp and not timestr:
            self.client.revertKeysRecordTime(keys, records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and timestamp and timestr:
            self.client.revertKeysRecordTimestr(keys, records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and keys and timestamp and not timestr:
            self.client.revertKeyRecordsTime(keys, records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and keys and timestamp and timestr:
            self.client.revertKeyRecordsTimestr(keys, records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and records and timestamp and not timestr:
            self.client.revertKeyRecordTime(keys, records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and records and timestamp and timestr:
            self.client.revertKeyRecordTimestr(keys, records, timestamp, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('keys, record and timestamp')

    def search(self, key, query):
        """

        :param key:
        :param query:
        :return:
        """
        data = self.client.search(key, query, self.creds, self.transaction, self.environment)
        data = list(data) if isinstance(data, set) else data
        return data

    def select(self, keys=None, criteria=None, records=None, timestamp=None, **kwargs):
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
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, str)
        # Handle case when kwargs are not used and the second parameter is a the record
        # (e.g. trying to get key/record(s))
        if (isinstance(criteria, int) or isinstance(criteria, list)) and not records:
            records = criteria
            criteria = None
        if isinstance(records, list) and not keys and not timestamp:
            data = self.client.selectRecords(records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and not timestr and not keys:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and timestr and not keys:
            data = self.client.selectRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and not timestamp:
            data = self.client.selectKeysRecords(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp and not timestr:
            data = self.client.selectKeysRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                     self.environment)
        elif isinstance(records, list) and isinstance(keys, list) and timestamp and timestr:
            data = self.client.selectKeysRecordsTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                        self.environment)
        elif isinstance(keys, list) and criteria and not timestamp:
            data = self.client.selectKeysCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and criteria and timestamp and not timestr:
            data = self.client.selectKeysCclTime(keys, criteria, timestamp, self.creds, self.transaction,
                                                 self.environment)
        elif isinstance(keys, list) and criteria and timestamp and timestr:
            data = self.client.selectKeysCclTimestr(keys, criteria, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif isinstance(keys, list) and records and not timestamp:
            data = self.client.selectKeysRecord(keys, records, self.creds, self.transaction, self.environment)
        elif isinstance(keys, list) and records and timestamp and not timestr:
            data = self.client.selectKeysRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif isinstance(keys, list) and records and timestamp and timestr:
            data = self.client.selectKeysRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                       self.environment)
        elif criteria and not keys and not timestamp:
            data = self.client.selectCcl(criteria, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and not timestr and not keys:
            data = self.client.selectCclTime(criteria, timestamp, self.creds, self.transaction, self.environment)
        elif criteria and timestamp and timestr and not keys:
            data = self.client.selectCclTimestr(criteria, timestamp, self.creds, self.transaction, self.environment)
        elif records and not keys and not timestamp:
            data = self.client.selectRecord(records, self.creds, self.transaction, self.environment)
        elif records and not isinstance(records, list) and timestamp and not timestr and not keys:
            data = self.client.selectRecordTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and timestamp and not timestr and not keys:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and not isinstance(records, list) and timestamp and timestr and not keys:
            data = self.client.selectRecordTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif isinstance(records, list) and timestamp and timestr and not keys:
            data = self.client.selectRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif keys and criteria and not timestamp:
            data = self.client.selectKeyCcl(keys, criteria, self.creds, self.transaction, self.environment)
        elif keys and criteria and timestamp and not timestr:
            data = self.client.selectKeyCclTime(keys, criteria, timestamp, self.creds, self.transaction,
                                                self.environment)
        elif keys and criteria and timestamp and timestr:
            data = self.client.selectKeyCclTimestr(keys, criteria, timestamp, self.creds, self.transaction,
                                                   self.environment)
        elif keys and isinstance(records, list) and not timestamp:
            data = self.client.selectKeyRecords(keys, records, self.creds, self.transaction, self.environment)
        elif keys and records and not timestamp:
            data = self.client.selectKeyRecord(keys, records, self.creds, self.transaction, self.environment)
        elif keys and isinstance(records, list) and timestamp and not timestr:
            data = self.client.selectKeyRecordsTime(keys, records, timestamp, self.creds, self.transaction,
                                                    self.environment)
        elif keys and isinstance(records, list) and timestamp and timestr:
            data = self.client.selectKeyRecordsTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                       self.environment)
        elif keys and records and timestamp and not timestr:
            data = self.client.selectKeyRecordTime(keys, records, timestamp, self.creds, self.transaction,
                                                   self.environment)
        elif keys and records and timestamp and timestr:
            data = self.client.selectKeyRecordTimestr(keys, records, timestamp, self.creds, self.transaction,
                                                      self.environment)
        elif records and not isinstance(records, list) and not timestamp:
            data = self.client.selectRecord(records, self.creds, self.transaction, self.environment)
        elif records and not isinstance(records, list) and timestamp and not timestr:
            data = self.client.selectRecordTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and not isinstance(records, list) and timestamp and timestr:
            data = self.client.selectRecordTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and not timestamp:
            data = self.client.selectRecords(records, self.creds, self.transaction, self.environment)
        elif records and timestamp and not timestr:
            data = self.client.selectRecordsTime(records, timestamp, self.creds, self.transaction, self.environment)
        elif records and timestamp and timestr:
            data = self.client.selectRecordsTimestr(records, timestamp, self.creds, self.transaction, self.environment)
        else:
            require_kwarg('criteria or record')
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
        elif isinstance(records, int):
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

    def unlink(self, key, source, destinations=None, **kwargs):
        """

        :param key:
        :param source:
        :param destination:
        :return:
        """
        destinations = destinations or kwargs.get('destination')
        if not isinstance(destinations, list):
            return self.remove(key=key, value=Link.to(destinations), record=source)
        else:
            data = dict()
            for dest in destinations:
                data[dest] = self.remove(key, Link.to(dest), source)
            return data

    def verify(self, key, value, record, timestamp=None, **kwargs):
        value = python_to_thrift(value)
        timestamp = timestamp or find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = isinstance(timestamp, str)
        if not timestamp:
            return self.client.verifyKeyValueRecord(key, value, record, self.creds, self.transaction, self.environment)
        elif timestamp and not timestr:
            return self.client.verifyKeyValueRecordTime(key, value, record, timestamp, self.creds, self.transaction,
                                                        self.environment)
        elif timestamp and timestr:
            return self.client.verifyKeyValueRecordTimestr(key, value, record, timestamp, self.creds, self.transaction,
                                                           self.environment)

    def verify_and_swap(self, key=None, expected=None, record=None, replacement=None, **kwargs):
        """

        :param key:
        :param expected:
        :param record:
        :param replacement:
        :return:
        """
        expected = expected or find_in_kwargs_by_alias('expected', **kwargs)
        replacement = replacement or find_in_kwargs_by_alias('replacement', **kwargs)
        expected = python_to_thrift(expected)
        replacement = python_to_thrift(replacement)
        if key and expected and record and replacement:
            return self.client.verifyAndSwap(key, expected, record, replacement, self.creds,  self.transaction,
                                         self.environment)
        else:
            require_kwarg('key, expected, record and replacement')

    def verify_or_set(self, key, value, record):
        """

        :param key:
        :param value:
        :param record:
        :return:
        """
        value = python_to_thrift(value)
        return self.client.verifyOrSet(key, value, record, self.creds, self.transaction, self.environment)

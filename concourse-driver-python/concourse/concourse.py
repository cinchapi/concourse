# Copyright (c) 2013-2017 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "Jeff Nelson"
__copyright__ = "Copyright 2015, Cinchapi Inc."
__license__ = "Apache, Version 2.0"

from thrift import Thrift
from thrift.transport import TSocket
from .thriftapi import ConcourseService
from .thriftapi.shared.ttypes import *
from .utils import *
from .json import json_encode
from collections import OrderedDict
from io import BytesIO
from configparser import ConfigParser
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

    def abort(self):
        """ Abort the current transaction and discard any changes that are currently staged.

        After returning, the driver will return to `autocommit` mode and all subsequent changes
        will be committed immediately.

        Calling this method when the driver is not in `staging` mode is a no-op.
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
            * :param key: [str] the field name
            * :param value: [object] the value to add
            * :returns: the new record id
        * `add(key, value, record)` - Append *key* as *value* in *record* if and only if it does not exist.
            * :param key: [str] the field name
            * :param value: [object] the value to add
            * :param record: [integer] the record id where an attempt is made to add the data
            * :returns: a bool that indicates if the data was added
        * `add(key, value, records)` - Append *key* as *value* in each of the *records* where it doesn't exist.
            * :param key: [str] the field name
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
        """ List changes made to to a **field** or **record** over time.

        Options:
        -------
        * `audit(key, record)` - List all the changes ever made to the *key* field in *record*.
            * :param key: [str] the field name
            * :param record: [long] the record id
            * :returns a dict containing, for each change, a mapping from timestamp to a description of the change that
                       occurred
        * `audit(key, record, start)` - List all the changes made to the *key* field in *record* since *start*
            (inclusive).
            * :param key: [str] the field name
            * :param record: [str] the record id
            * :param start: [int|str] an inclusive timestamp for the oldest change that should possibly be included in
                            the audit
            * :returns a dict containing, for each change, a mapping from timestamp to a description of the change that
                       occurred
        * `audit(key, record, start, end)` - List all the changes made to the *key* field in *record* between *start*
            (inclusive) and *end* (non-inclusive).
            * :param key: [str] the field name
            * :param record: [str] the record id
            * :param start: [int|str] an inclusive timestamp for the oldest change that should possibly be included in
                            the audit
            * :param end: [int|str] a non-inclusive timestamp for the most recent change that should possibly be
                          included in the audit
            * :returns a dict containing, for each change, a mapping from timestamp to a description of the change that
                       occurred
        * `audit(record)` - List all the changes ever made to *record*.
            * :param record: [long] the record id
            * :returns a dict containing, for each change, a mapping from timestamp to a description of the change that
                       occurred
        * `audit(record, start)` - List all the changes made to *record* since *start* (inclusive).
            * :param record: [str] the record id
            * :param start: [int|str] an inclusive timestamp for the oldest change that should possibly be included in
                            the audit
            * :returns a dict containing, for each change, a mapping from timestamp to a description of the change that
                       occurred
        * `audit(record, start, end)` - List all the changes made to *record* between *start* (inclusive) and *end*
            (non-inclusive).
            * :param record: [str] the record id
            * :param start: [int|str] an inclusive timestamp for the oldest change that should possibly be included in
                            the audit
            * :param end: [int|str] a non-inclusive timestamp for the most recent change that should possibly be
                          included in the audit
            * :returns a dict containing, for each change, a mapping from timestamp to a description of the change that
                       occurred

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
        """ For one or more **fields**, view the values from all records currently or previously stored.

        Options:
        -------
        * `browse(key)` - View the values from all records that are currently stored for *key*.
            * :param key: [str] - the field name
            * :returns a dict associating each value to the list of records that contain that value in the *key* field
        * `browse(key, timestamp)` - View the values from all records that were stored for *key* at _timestamp_.
            * :param key: [str] - the field name
            * :param timestamp: [int|str] - the historical timestamp to use in the lookup
            * :returns a dict associating each value to the list of records that contained that value in the *key*
                       field at *timestamp*
        * `browse(keys)` - View the values from all records that are currently stored for each of the *keys*.
            * :param keys: [list] - a list of field names
            * :returns a dict associating each key to a dict associating each value to the list of records that contain
                       that value in the *key* field
        * `browse(keys, timestamp)` - View the values from all records that were stored for each of the *keys* at
                                     *timestamp*
            * :param keys: [list] - a list of field names
            * :param timestamp: [int|str] - the historical timestamp to use in the lookup
            * :returns a dict associating each key to a dict associating each value to the list of records that
                       contained that value in the *key* field at *timestamp*
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
        """ View a time series with snapshots of a _field_ after every change.

        Options:
        -------
        * `chronologize(key, record)` - View a time series that associates the timestamp of each modification for _key_
            in _record_ to a snapshot containing the values that were stored in the field after the change.
            * :param key: [str] - the field name
            * :param record: [int] - the record id
            * :returns a dict associating each modification timestamp to the list of values that were stored in the
            field after the change
        * `chronologize(key, record, start)` - View a time series between _start_ (inclusive) and the present that
            associates the timestamp of each modification for _key_ in _record_ to a snapshot containing the values that
            were stored in the field after the change.
            * :param key: [str] - the field name
            * :param record: [int] - the record id
            * :param start [int|str] - the first possible timestamp to include in the time series
            * :returns a dict associating each modification timestamp to the list of values that were stored in the
            field after the change.
        * `chronologize(key, record, start, end)` - View a time series between _start_ (inclusive) and _end_
            (non-inclusive) that associates the timestamp of each modification for _key_ in _record_ to a snapshot
            containing the values that were stored in the field after the change.
            * :param key: [str] - the field name
            * :param record: [int] - the record id
            * :param start [int|str] - the first possible timestamp to include in the time series
            * :param end [int|str] - the timestamp that should be greater than every timestamp in the time series
            * :returns a dict associating each modification timestamp to the list of values that were stored in the
            field after the change.
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
        """ Atomically remove all the values from one or more _fields_.

        Options:
        -------
        * `clear(record)` - Atomically remove all the values stored for every key in _record_.
            * :param record: [int] - the record id
        * `clear(records)` - Atomically remove all the values stored for every key in each of the _records_.
            * :param records: [list] - the list of record ids
        * `clear(key, record)` - Atomically remove all the values stored for _key_ in _record_.
            * :param key: [str] - the field name
            * :param record: [int] - the record id
        * `clear(keys, record)` - Atomically remove all the values stored for each of the _keys_ in _record_.
            * :param keys: [list] - the list of field names
            * :param record: [int] - the record id
        * `clear(key, records)` - Atomically remove all the values stored for _key_ in each of the _records_.
            * :param key: [str] - the field name
            * :param records: [list] - the list of record ids
        * `clear(keys, records)` - Atomically remove all the values stored for each of the _keys_ in each of the
                _records_.
            * :param keys: [list] - the list of field names
            * :param records: [list] - the list of record ids
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

    def close(self):
        """ An alias for the exit method.
        """
        self.exit()

    def commit(self):
        """ Attempt to permanently commit any changes that are staged in a transaction and return _True_ if and only if
        all the changes can be applied. Otherwise, returns _False_ and all the changes are discarded.

        After returning, the driver will return to _autocommit_ mode and all subsequent changes will be committed
        immediately.

        This method will return _false_ if it is called when the driver is not in _staging_ mode.

        :return: _True_ if all staged changes are committed, otherwise _False_
        """
        token = self.transaction
        self.transaction = None
        if token:
            return self.client.commit(self.creds, token, self.environment)
        else:
            return False

    def describe(self, records=None, timestamp=None, **kwargs):
        """ For one or more _records_, list all the _keys_ that have at least one value.

        Options:
        -------
        * `describe(record)` - List all the keys in _record_ that have at least one value.
            * :param record: [int] - the record id
            * :returns the list of keys in _record_
        * `describe(record, timestamp)` - List all the keys in _record_ that had at least one value at _timestamp_.
            * :param record: [int] - the record id
            * :param timestamp [int|str] - the historical timestamp to use in the lookup
            * :returns the list of keys that were in _record_ at _timestamp_
        * `describe(records)` - For each of the _records_, list all of the keys that have at least one value.
            * :param records: [list] - a list of record ids
            * :returns a dict associating each record id to the list of keys in that record
        * `describe(records, timestamp)` - For each of the _records_, list all the keys that had at least one value at
            _timestamp_.
            * :param records: [list] - a list of record ids
            * :param timestamp [int|str] - the historical timestamp to use in the lookup
            * :returns a dict associating each record id to the list of keys that were in that record at _timestamp_
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
        """ List the net changes made to a _field_, _record_ or _index_ from one timestamp to another.

        Options:
        -------
        * `diff(record, start)` - List the net changes made to _record_ since _start_. If you begin with
            the state of the _record_ at _start_ and re-apply all the changes in the diff, you'll re-create
            the state of the same _record_ at the present.
            * :param record: [int] - the record id
            * :param start [int|str] - the base timestamp from which the diff is calculated
            * :returns a dict that associates each key in the _record_ to another dict that associates a change
                description (see Diff) to the list of values that fit the description
                (i.e. `{"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}`)
        * `diff(record, start, end)` - List the net changes made to _record_ from _start_ to _end_. If you begin
            with the state of the same _record_ at _start_ and re-apply all the changes in the diff, you'll re-create
            the state of the _record_ at _end_.
            * :param record: [int] - the record id
            * :param start [int|str] - the base timestamp from which the diff is calculated
            * :param end [int|str] - the comparison timestamp to which the diff is calculated
            * :returns a dict that associates each key in the _record_ to another dict that associates a change
                description (see Diff) to the list of values that fit the description
                (i.e. `{"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}`)
        * `diff(key, record, start)` - List the net changes made to _key_ in _record_ since _start_. If you begin
            with the state of the field at _start_ and re-apply all the changes in the diff, you'll re-create the state
            of the same field at the present.
            * :param key: [str] - the field name
            * :param record: [int] - the record id
            * :param start [int|str] - the base timestamp from which the diff is calculated
            * :returns dict that associates a change description (see Diff) to the list of values that fit
                the description (i.e. `{ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}`)
        * `diff(key, record, start, end)` - List the next changes made to _key_ in _record_ from _start_ to _end_.
            If you begin with the sate of the field at _start_ and re-apply all the changes in the diff, you'll
            re-create the state of the same field at _end_.
            * :param key: [str] - the field name
            * :param record: [int] - the record id
            * :param start [int|str] - the base timestamp from which the diff is calculated
            * :param end [int|str] - the comparison timestamp to which the diff is calculated
            * :returns dict that associates a change description (see Diff) to the list of values that fit
                the description (i.e. `{ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}`)
        * `diff(key, start)` - List the net changes made to the _key_ field across all records since _start_. If you
            begin with the state of the inverted index for _key_ at _start_ and re-apply all the changes in the diff,
            you'll re-create the state of the same index at the present.
            * :param key: [str] - the field name
            * :param start [int|str] - the base timestamp from which the diff is calculated
            * :returns a dict that associates each value stored for _key_ across all records to another dict that
                associates a change description (see Diff) to the list of records where the description applies to
                that value in the _key_ field (i.e. `{"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}`)
        * `diff(key, start, end)` - List the net changes made to the _key_ field across all records from _start_ to
            _end_. If you begin with the state of the inverted index for _key_ at _start_ and re-apply all the changes
            in the diff, you'll re-create the state of the same index at _end_.
            * :param key: [str] - the field name
            * :param start [int|str] - the base timestamp from which the diff is calculated
            * :param end [int|str] - the comparison timestamp to which the diff is calculated
            * :returns a dict that associates each value stored for _key_ across all records to another dict that
                associates a change description (see Diff) to the list of records where the description applies to
                that value in the _key_ field (i.e. `{"value1": {ADDED: [1, 2], REMOVED: [3, 4]}}`)
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

    def exit(self):
        """ Terminate the client's session and close this connection.
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
            data = json_encode(data)
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
            data = json_encode(data)

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
        :param destinations:
        :param destination:
        :param source:
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

    def reconcile(self, key, record, values, **kwargs):
        """

        :param key:
        :param record:
        :param values:
        :return:
        """
        key = key or kwargs.get('key')
        record = record or kwargs.get('record')
        values = values or kwargs.get('values') or kwargs.get('value') or []
        values = thriftify(values);
        self.client.reconcileKeyRecordValues(key, record, values, self.creds, self.transaction, self.environment)

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
        :param destination:
        :param source:
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

    def __authenticate(self):
        """ Internal method to login with the username/password and locally store the AccessToken for use with
        subsequent operations.
        """
        try:
            self.creds = self.client.login(self.username, self.password, self.environment)
        except Thrift.TException as e:
            raise e

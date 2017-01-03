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

__author__ = 'jnelson'
from .thriftapi.shared.ttypes import Type
from .thriftapi.data.ttypes import TObject
from .types import *
import struct
import inspect


def python_to_thrift(value):
    """
    Serialize a value to its thrift representation.
    :param value:
    :return: the TObject representation
    """
    if isinstance(value, bool):
        ttype = Type.BOOLEAN
        data = struct.pack(">?", value)
    elif isinstance(value, int):
        if value > 2147483647 or value < -2147483648:
            ttype = Type.LONG
            data = struct.pack(">q", value)
        else:
            ttype = Type.INTEGER
            data = struct.pack(">i", value)
    elif isinstance(value, float):
            ttype = Type.DOUBLE
            data = struct.pack(">d", value)
    elif isinstance(value, Link):
        ttype = Type.LINK
        data = struct.pack(">q", value.record)
    elif isinstance(value, Tag):
        ttype = Type.TAG
        data = bytes(value.__str__(), 'utf-8')
    else:
        ttype = Type.STRING
        data = bytes(value.__str__(), 'utf-8')
    return TObject(data, ttype)


def thrift_to_python(tobject):
    """
    Convert a TObject to the appropriate python representation
    :return: the pythonic value
    """
    data = tobject.data
    if tobject.type == Type.BOOLEAN:
        py = struct.unpack_from('>?', data)[0]
    elif tobject.type == Type.INTEGER:
        py = struct.unpack_from(">i", data)[0]
    elif tobject.type == Type.LONG:
        py = struct.unpack_from(">q", data)[0]
    elif tobject.type == Type.DOUBLE:
        py = struct.unpack_from(">d", data)[0]
    elif tobject.type == Type.FLOAT:
        py = struct.unpack_from(">f", data)[0]
    elif tobject.type == Type.LINK:
        record = struct.unpack_from(">q", data)[0]
        py = Link.to(record)
    elif tobject.type == Type.TAG:
        s = data.decode('utf-8')
        py = Tag.create(s)
    elif tobject.type == Type.NULL:
        py = None
    else:
        py = data.decode('utf-8')
    return py


def thriftify(obj):
    """Recursively convert and nested Python objects to TObjects
    :param obj
    :return a TObject or collection of TObjects
    """
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            obj.pop(k)
            k = python_to_thrift(k) if (not isinstance(k, dict) and not isinstance(k, list) and not isinstance(k, set)) else thriftify(k)
            v = python_to_thrift(v) if (not isinstance(k, dict) and not isinstance(k, list) and not isinstance(k, set)) else thriftify(v)
            obj[k] = v
        return obj
    elif isinstance(obj, list) or isinstance(obj, set):
        return [thriftify(n) for n in obj]
    elif not isinstance(obj, TObject):
        return python_to_thrift(obj)
    else:
        return obj


def pythonify(obj):
    """
    Recursively convert any nested TObjects to python objects
    :param obj:
    :return: a purely pythonic object
    """
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            obj.pop(k)
            if isinstance(k, bytes):
                k = k.decode()
            k = thrift_to_python(k) if isinstance(k, TObject) else pythonify(k)
            v = thrift_to_python(v) if isinstance(v, TObject) else pythonify(v)
            obj[k] = v
        return obj
    elif isinstance(obj, list) or isinstance(obj, set):
        return [pythonify(n) for n in obj]
    elif isinstance(obj, TObject):
        return thrift_to_python(obj)
    else:
        if isinstance(obj, bytes):
            obj = obj.decode()
        return obj


def require_kwarg(arg):
    """
    Raise a value error that explains that the arg is required
    :param arg:
    """
    func = inspect.stack()[1][3] + '()'
    raise ValueError(func + ' requires the ' + arg + ' keyword argument(s)')


# The aliases for the find_in_kwargs function lookup
kwarg_aliases = {
    'criteria': lambda x: x.get('ccl') or x.get('where') or x.get('query'),
    'timestamp': lambda x: x.get('time') or x.get('ts'),
    'username': lambda x: x.get('user') or x.get('uname'),
    'password': lambda x: x.get('pass') or x.get('pword'),
    'prefs': lambda x: x.get('file') or x.get('filename') or x.get('config') or x.get('path'),
    'expected': lambda x: x.get('value') or x.get('current') or x.get('old'),
    'replacement': lambda x: x.get('new') or x.get('other') or x.get('value2'),
    'json': lambda x: x.get('data'),
    'record': lambda x: x.get('id')
}


def find_in_kwargs_by_alias(key, kwargs0):
    """
    Attempt to find a value for a key in the kwargs by looking up certain aliases.
    :param key:
    :param thekwargs:
    :return: the value that corresponds to key after looking up all the aliases or None if a value is not found
    """
    return kwargs0.get(key) or kwarg_aliases.get(key, lambda x: None)(kwargs0)

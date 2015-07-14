__author__ = 'jnelson'
from thriftapi.shared.ttypes import Type
from thriftapi.data.ttypes import TObject
from types import *
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
        data = buffer(value.__str__())
    else:
        ttype = Type.STRING
        data = buffer(value.__str__())
    return TObject(data, ttype)


def thrift_to_python(tobject):
    """
    Convert a TObject to the appropriate python representation
    :return: the pythonic value
    """
    if tobject.type == Type.BOOLEAN:
        py = struct.unpack_from('>?', tobject.data)[0]
    elif tobject.type == Type.INTEGER:
        py = struct.unpack_from(">i", tobject.data)[0]
    elif tobject.type == Type.LONG:
        py = struct.unpack_from(">q", tobject.data)[0]
    elif tobject.type == Type.DOUBLE:
        py = struct.unpack_from(">d", tobject.data)[0]
    elif tobject.type == Type.FLOAT:
        py = struct.unpack_from(">f", tobject.data)[0]
    elif tobject.type == Type.LINK:
        record = struct.unpack_from(">q", tobject.data)[0]
        py = Link.to(record)
    elif tobject.type == Type.TAG:
        s = tobject.data.__str__()
        py = Tag.create(s)
    elif tobject.type == Type.NULL:
        py = None
    else:
        py = tobject.data.__str__()
    return py


def thriftify(obj):
    """Recursively convert and nested Python objects to TObjects
    :param obj
    :return a TObject or collection of TObjects
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
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
        for k, v in obj.items():
            obj.pop(k)
            k = thrift_to_python(k) if isinstance(k, TObject) else pythonify(k)
            v = thrift_to_python(v) if isinstance(v, TObject) else pythonify(v)
            obj[k] = v
        return obj
    elif isinstance(obj, list) or isinstance(obj, set):
        return [pythonify(n) for n in obj]
    elif isinstance(obj, TObject):
        return thrift_to_python(obj)
    else:
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
    'replacement': lambda x: x.get('new') or x.get('other') or x.get('value2')
}


def find_in_kwargs_by_alias(key, kwargs0):
    """
    Attempt to find a value for a key in the kwargs by looking up certain aliases.
    :param key:
    :param thekwargs:
    :return: the value that corresponds to key after looking up all the aliases or None if a value is not found
    """
    return kwargs0.get(key) or kwarg_aliases.get(key, lambda x: None)(kwargs0)
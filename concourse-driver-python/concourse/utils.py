__author__ = 'jnelson'
from thriftapi.shared.ttypes import Type
from thriftapi.data.ttypes import TObject
from types import *
import struct


class Convert:

    @staticmethod
    def python_to_thrift(value):
        """

        :param value:
        :return:
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
            ttype = Type.FLOAT
            data = struct.pack(">f", value)
            # TODO account for floats
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

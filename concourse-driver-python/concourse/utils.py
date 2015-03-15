__author__ = 'jnelson'

from rpc.shared.ttypes import Type
from rpc.ttypes import TObject


class Convert:

    @staticmethod
    def python_to_thrift(value):
        """

        :param value:
        :return:
        """
        if isinstance(value, basestring):
            ttype = Type.STRING
            data = buffer(value)
        else:
            raise ValueError
        return TObject(data, ttype)

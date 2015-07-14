__author__ = 'Jeff Nelson'

from nose.plugins.skip import SkipTest


def ignore(func):
    """Ignore a nose test using the @ignore decorator
    """
    def x(caller):
        raise SkipTest
    x.__name__ = func.__name__
    return x
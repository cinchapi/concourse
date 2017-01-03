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

from abc import ABCMeta


class JsonPicklable(object):
    """
    A class that can be serialized to some JSON representation using the jsonpickle module.
    """

    __metaclass__ = ABCMeta

    def __getstate__(self):
        """
        Return the state that is used by jsonpickle for serialization. By default, the __str()__
        method is used.
        """
        return self.__str__()


class Tag(JsonPicklable):
    """
    A Tag is a String data type that does not get full-text indexed.

    Each Tag is equivalent to its String counterpart. Tags merely exist for the
    client to instruct Concourse not to full text index the data. Tags are stored
    as Strings within Concourse. And any value that is written as a Tag is always
    returned as a String when reading from Concourse.
    """
    @staticmethod
    def create(value):
        return Tag(value)

    def __init__(self, value):
        self.value = value.__str__()

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.value == other.value


class Link(JsonPicklable):
    """
    A Link is a pointer to a record.

    Links should never be written directly. They can be created using the `Concourse.link`
    method in the `Concourse` API.

    Links may be returned when reading data using the `Concourse.select()`, `Concourse.get()`
    and `Concourse.browse()` methods. When handling Link objects, you can retrieve the
    underlying record id by accessing the `Link.record` property.

    When performing a bulk insert (using the `Concourse.insert()` method) you can use this class
    to create Link objects that are added to the data/json blob. Links inserted in this manner
    will be written in the same way they would have been if they were written using the
    `Concourse.link()` method.

    To create a static link to a single record, use `Link.to()`.

    To create static link to each of the records that match a criteria, use the `Link.to_where`
    method.
    """

    @staticmethod
    def to(record):
        """ Return a _Link_ that points to _record_.

        :param record: [int] the record id
        :return: a _Link_ that points to _record_
        """
        return Link(record)

    @staticmethod
    def to_where(ccl):
        """ Return a string that instructs Concourse to create links that point to each of the records
        that match the _ccl_ string.

        NOTE: This method DOES NOT return a _Link_ object, so it should only be used when adding a
        resolvable link value to a data/json blob that will be passed to the `Concourse.insert()`
        method.

        :param ccl: [str] a CCL string that describes the records to which a Link should point
        :return: a resolvable link instruction
        """
        return "@{ccl}@".format(ccl=ccl)

    def __init__(self, record):
        if not isinstance(record, int):
            raise ValueError
        self.__record = record

    def __str__(self):
        return "@" + self.__record.__str__()

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return isinstance(other, Link) and self.record == other.record

    @property
    def record(self):
        return self.__record

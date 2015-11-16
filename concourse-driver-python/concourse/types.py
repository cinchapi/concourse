# Copyright (c) 2015 Cinchapi Inc.
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


class Tag:
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


class Link:
    """
    A Link is a wrapper around a {@link Long} that represents the primary
    key of a record and distinguishes from simple long values. A Link is
    returned from read methods in Concourse if data was added using one of
    the #link operations.
    """

    @staticmethod
    def to(record):
        return Link(record)

    def __init__(self, record):
        if not isinstance(record, int):
            raise ValueError
        self.record = record

    def __str__(self):
        return "@" + self.record.__str__()

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.record == other.record

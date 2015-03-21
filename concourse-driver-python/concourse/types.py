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
        return self.value == other


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
        return "@" + self.record.__str__() + "@"

    def __repr__(self):
        return self.__str__()

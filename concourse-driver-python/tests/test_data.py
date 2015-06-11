__author__ = 'jnelson'

import random
import string


def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def random_bool():
    return True if random.randint(0, 10) % 2 == 0 else False


def random_int():
    return random.randint(-2147483648, 2147483647)


def random_long():
    return random.randint(-9223372036854775808, 9223372036854775807)


def random_float():
    return random.uniform(1, 10)

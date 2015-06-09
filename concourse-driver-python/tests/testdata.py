__author__ = 'jnelson'

import random
import string


def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def random_bool():
    return True if random.randint(0, 10) % 2 == 0 else False

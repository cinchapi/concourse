# Copyright (c) 2013-2016 Cinchapi Inc.
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

import random
import string
import time

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

current_time_millis = lambda: int(round(time.time() * 1000))


def get_elapsed_millis_string(timestamp):
    """ Return a string that describes how many milliseconds have passed since the specified timestamp
    (i.e. 3 milliseconds ago)

    :param timestamp:
    :return: a string describing how long ago the timestamp occurred
    """
    now = current_time_millis()
    delta = now - timestamp
    return str(delta) + ' milliseconds ago'

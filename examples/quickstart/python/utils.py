__author__ = 'Jeff Nelson'
#
# The MIT License (MIT)
#
# Copyright (c) 2013-2017 Cinchapi Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import time
import pprint


def printd(content, delay=1.5):
    """
    Print with a delay.
    :param content:
    :param delay:
    """
    if isinstance(content, str) and content.startswith('>'):
        delay = 0.1
    print(content)
    time.sleep(delay)


def pprintd(content, delay=1.5):
    """
    Pretty print with a delay
    :param content:
    :param delay:
    """
    if isinstance(content, str) and content.startswith('>'):
        delay = 0.5
    pprint.pprint(content)
    time.sleep(delay)
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

__author__ = 'Jeff Nelson'

import jsonpickle

""" Python has so many JSON libraries, each with various configuration options. This module contains
 basic methods for JSON handling that abstract the underlying library from the rest of the codebase.
"""


def json_encode(obj):
    """ Encode _obj_ as a JSON formatted string.

    :param obj: [object] the object to encode
    :return: the JSON string
    """
    return jsonpickle.encode(obj, unpicklable=False)
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

from setuptools import setup, find_packages
import os
import subprocess


def get_version():
    """
    Return the correct form of the version that should be passed to the setup function
    :return: the version string
    """
    script = os.path.dirname(os.path.realpath(__file__)) + '/../version.sh'
    version = subprocess.getoutput('bash ' + script)
    version = version.replace('-SNAPSHOT', '.dev')  # PyPi prefers .dev suffix over -SNAPSHOT
    return version

setup(
    name='concourse-driver-python',
    description='The official Python driver for Concourse',
    long_description='The official Python driver for Concourse',
    url='https://github.com/cinchapi/concourse',
    version=get_version(),
    author='Cinchapi Inc',
    author_email='python-oss@cinchapi.org',
    license='Apache, Version 2.0',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=[
        'thrift == 0.9.3',
        'jsonpickle == 0.9.2',
    ],
    extra_require={
        'test': [
            'nose',
            'ujson==1.33'
        ]
    }
)

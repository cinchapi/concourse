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

from invoke import task, run


@task
def build():
    run('python setup.py clean sdist bdist_wheel')
    #run('twine upload dist/*')

@task
def clean():
    run('rm -rf build')

@task
def docs():
    run('pdoc concourse --html --overwrite --html-no-source --html-dir build/docs')


@task
def test():
    run('nosetests')


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

from invoke import task, run


@task
def build(ctx):
    run('python setup.py sdist bdist_wheel')
    run('mv dist build/')
    run('mv concourse_driver_python.egg-info build/')


@task(build)
def upload_pypi(ctx):
    import os
    file = '~/.pypi-password'
    try:
        with open(os.path.abspath(os.path.expanduser(file))) as f:
            password = f.read()
    except FileNotFoundError:
        password = None
    finally:
        if not password:
            raise Exception("Could not find the pypi password in "+file)
        else:
            run('twine upload --config .pypirc build/dist/* -p '+password)


@task
def clean(ctx):
    run('rm -rf build dist')


@task
def docs(ctx):
    run('pdoc concourse --html --overwrite --html-no-source --html-dir build/docs')


@task
def test(ctx):
    run('nosetests')

__author__ = 'Jeff Nelson'

from setuptools import setup, find_packages

setup(
    name='concourse-driver-python',
    description='The official Python driver for Concourse',
    long_description='The official Python driver for Concourse',
    url='https://github.com/cinchapi/concourse',
    version='0.5.0',
    author='Cinchapi, Inc',
    author_email='python-oss@cinchapi.org',
    license='Apache, Version 2.0',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=[
        'thrift == 0.9.2',
        'ujson == 1.33',
        'nose == 1.3.4'
    ]
)

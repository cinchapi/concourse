__author__ = 'Jeff Nelson'

from setuptools import setup

setup(
    name='concourse-driver-python',
    version='0.5.0',
    author='Cinchapi, Inc',
    license='Apache, Version 2.0',
    packages=['concourse'],
    install_requires=[
        'thrift==0.9.2',
        'ujson==1.33',
        'nose=1.3.4'
    ]
)

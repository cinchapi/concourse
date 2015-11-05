# Concourse Python Driver
The official Python driver for [Concourse](http://concoursedb.com).

## Requirements
* Python 2.7 or Python 3.3+
* *[Groovy] (http://www.groovy-lang.org/) (for running unit tests)*

## Quickstart
TODO

## Developer Setup
*All commands are run from the root of the concourse-driver-python project*

### The Build System
We use [pyinvoke](http://www.pyinvoke.org/) as the build system. The `pyinvoke` executable is 
located in the root of the project.

#### Running tests
Behind the scenes we use [nose](https://nose.readthedocs.org/en/latest/) for unit tests. 
Unit tests interact with an in-memory instance of Concourse (see [Mockcourse](../mockcourse)) that requires 
[Groovy] (http://www.groovy-lang.org/).
```bash
$ ./pyinvoke test
```

#### Generating Documentation
```bash
$ ./pyinvoke docs
```
The documentation will be available in the `build/docs` directory.

#### Building 
```bash
$ ./pyinvoke build
```
This will generate both an sdist tar.gz file and a bdist wheel for distribution. Both will be located in 
the `build/dist` directory.

#### Uploading to PyPi
```bash
$ ./pyinvoke upload_pypi
```
This will upload the sdist and bdist files to the PyPi repo defined in the local `.pypirc` file (note: the tasks 
will run the build task to create these files if necessary).

The tasks checks for the password needed for uploading in the executing user's home directory for a file named 
`.pypi-password`. If that file does not exist, the tasks will fail by throwing an Exception.

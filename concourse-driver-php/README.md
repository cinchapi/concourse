# Concourse PHP Driver

## Requirements
* PHP 5.4+

## Installation
```bash
composer require cinchapi/concourse-driver-php
```

## Developer Setup
*All commands are run from the root of the concourse-driver-php project.*

1. Use [Composer](https://getcomposer.org/) to get local copies of all necessary dependencies.
```bash
$ php composer.phar install
```

### The Build System
We use [phake](https://github.com/jaz303/phake) as the build system for the PHP Driver. The `phake` executable is located in the root of the project.

#### Running tests
```bash
$ ./phake test
```

#### Generating Documentation
```bash
$ ./phake docs
```

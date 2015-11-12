# Concourse PHP Driver
The offical PHP driver for [Concourse](http://concoursedb.com).

## Requirements
* PHP 5.4+

## Quickstart

### Installation
Install via composer
```bash
composer require cinchapi/concourse-driver-php
```

### Usage
```php
<?php
$concourse = Concourse::connect(); // connects to localhost:1717 by default
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

#### Publishing
The PHP driver is made available via [Packagist](https://packagist.org/packages/cinchapi/concourse-driver-php). You can _upload_ by doing the following:
```bash
$ ./phake upload-packagist
```

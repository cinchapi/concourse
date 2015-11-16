# concourse-bash-tests
This project contains unit tests to validate bash/command-line functionality for Concourse scripts. The unit tests make use of the [bats](https://github.com/sstephenson/bats) framework.

See https://github.com/sstephenson/bats for documentation.

## Running the tests
```bash
$ ./utils/run-bash-tests.sh
```

## Modules
The unit tests are organized in a tree of modules. Modules deeper within the tree are more specific.
### base
The `base` module provides checks to ensure that the environment is conducive to running the unit tests. The base also creates a temporary directory and copies a Concourse Server installer over for possible use in unit tests.
##### Global Variables
* `$CONCOURSE_TEMP_DIR` - The absolute path to a temporary directory that can be used to store files that will be cleaned up after each unit test.
* `$CONCOURSE_INSTALLER_FILENAME` - The absolute path to an installer contained in a temporary directory. This installer can be used to install a temporary Concourse Server instance.

### post_install
The `post_install` module contains unit tests that assume the Concourse Server instance has been installed (but not necessarily started).
##### Global Variables
* `$CONCOURSE_BIN_DIR` - The absolute path to the `bin` directory for the temporary Concourse Server instance.

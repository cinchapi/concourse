# Concourse Shell
The concourse [`shell`](../reference/cli/shell.md) is a built-in interface to interact with Concourse. The concourse [`shell`](../reference/cli/shell) is backed by the full power of the [Groovy](http://groovy-lang.org/syntax.html) scripting language, so you can use it to dynamically query and update data.

Most examples in the [Concourse Guide](/) use concourse [`shell`](../reference/cli/shell). Most client [drivers](/) also provide a similar interface to Concourse.

## Starting concourse `shell`
!!! warning
    Make sure that Concourse is running before attempting to start concourse [shell](/).
To start concourse [`shell`](../reference/cli/shell.md) and connect to the Concourse instance running on **localhost** with the **default port** into the **default environment**:

1. At a prompt in a terminal window:

        concourse shell


### Changing connection parameters
You can connect to a different environment or a Concourse instance running on a different server and/or a different port by specifying any combination of the the following command line options:
```
Usage: concourse-shell [options]
  Options:
    -e, --environment
       The environment of the Concourse Server to use
       Default: <empty string>
    -h, --host
       The hostname where the Concourse Server is located
       Default: localhost
    -p, --port
       The port on which the Concourse Server is listening
       Default: 1717
    -u, --username
       The username with which to connect
       Default: admin
```

## Working with the concourse `shell`
## Ad hoc commands
## Run Commands File
You can use a `run commands` file to seed the shell with a script that is run before the prompt is displayed for the first time. By default, concourse `shell` checks the user's [HOME](/) directory for a groovy file named `.cashrc`.

Alternatively, you can specify a different `run-commands` file by starting concourse shell using the `--run-commands` or `-rc` option:
```bash
concourse shell --run-commands /path/to/run-commands-file
```
### Usage
Using a `run commands` file allows you to automatically seed each concourse shell session with common configuration.

**NOTE**: The script can access all the standard variables and methods defined in the shell's environment.
#### Startup Commands
#### Custom Functions
You can define custom functions in the `run commands` file using standard groovy syntax. Custom functions are added to the shell's namespace so they can be invoked by the interpreter; however they are **not run** on startup.

* For example, create a function to display all the keys across all records in Concourse:

```groovy
def showKeys() {
  allkeys = describe(inventory()).values()
  result = []
  allkeys.each { keys ->
    keys.each { key ->
      result << key
    }
  }
  result.toSet()
}
```

* Launch concourse shell and invoke the function:

```bash
[default/cash]$ showKeys()
Returned '[manager, name, follows, age, friends]' in 0.086 sec
```
#### Custom Variables
#### Interacting with Concourse

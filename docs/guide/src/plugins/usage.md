# Using Plugins

Use the `concourse plugins` command to manage plugins.

## Installation

You can install plugins using a local plugin `.zip` package or by downloading from the the [marketplace](https://marketplace.cinchapi.com).

### Install a local plugin package
```bash
concourse plugin install </path/to/plugin>
```

### Install a plugin from the marketplace
```bash
concourse plugin install <plugin-name>
```

### Installing multiple plugins

#### Install all local plugin packages within a directory
If one of the arguments given to the `concourse plugin install` command is a directory, all the plugin packages in that directory will be installed.
```bash
concourse plugin install </path/to/directory>
```

#### Install multiple local plugin packages
You can specify multiple local plugin packages as arguments to the `concourse plugin install` command.
```bash
concourse plugin install </path/to/plugin1> </path/to/plugin2> ... </path/to/plguinN>
```

#### Install multiple plugins from the marketplace
You can specify multiple marketplace plugin names as arguments to the `concourse plugin install` command.
```bash
concourse plugin install <plugin-name1> <plugin-name2>
```

## Invoking plugin methods

Plugin methods are automatically added to the Concourse API and can be called from any driver using the `invokePlugin` method.
```
invokePlugin(pluginId, methodName, methodArgs...)
```

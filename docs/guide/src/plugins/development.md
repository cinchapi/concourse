# Developing Plugins

## Getting Started
Concourse plugins must be implemented as Java classes that extend `com.cinchapi.concourse.server.plugin.Plugin`. To get started, create a Java project that depends on the `concourse-plugin-core` framework.

#### Create a build.gradle file
```groovy
plugins {
    id "com.cinchapi.concourse-plugin" version "1.0.14"
    id 'java'
    id 'eclipse'
    id 'maven'
}

group = 'com.cinchapi'
version  = getVersion()

// Set the version for all Concourse dependencies
ext.concourseVersion = '0.7.0'

// Configure Concourse plugin
bundle {
    bundleName project.name
}

task wrapper(type: Wrapper) {
    gradleVersion = '3.0'
}

repositories {
    mavenCentral()
}

dependencies {
    compile group: 'com.cinchapi', name: 'concourse-plugin-core',
      version: concourseVersion

	testCompile 'junit:junit:4.11'
	testCompile group: 'com.cinchapi', name: 'concourse-ete-test-core',
      version: concourseVersion
}

```

#### Download dependencies and generate IDE metadata
```bash
./gradlew clean eclipse
```

#### Implement the plugin
```java
package com.company.concourse.plugin.sample;

import com.cinchapi.concourse.server.plugin.Plugin;

/**
 * Sample Plugin Boilerplate
 */
public class SamplePlugin extends Plugin {

    /**
     * Construct a new instance.
     *
     * @param fromServer
     * @param fromPlugin
     */
    public Sample(String fromServer, String fromPlugin) {
        super(fromServer, fromPlugin);
    }

}
```

Any instance methods that are added to the plugin class will be dynamically added to the Concourse API.

#### Create the plugin package
```bash
./gradlew bundleZip
```

Check the `build/distributions` directory to see the generated `.zip` plugin package.

## Accessing Concourse within a Plugin
Concourse plugins have privileged access to the database using a special API. To interact with Concourse, use the `runtime` method inside the plugin class.
```java
public void sampleWriteMethod(String key, Object value) {
  runtime.addKeyValue(key, value);
}

public Object sampleReadMethod(String key) {
  //TODO: fixme
}
```
If you need to access Concourse from a class that does not extend `Plugin` (i.e. a utility class) you can interact with the runtime by calling `PluginRuntime.getRuntime()`.

## Background Requests
Each Plugin has access to the host Concourse instance

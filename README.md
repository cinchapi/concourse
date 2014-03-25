# Concourse

[Concourse](http://cinchapi.org/concourse) is a schemaless and distributed version control database with optimistic availability, serializable transactions and full-text search. Concourse provides a more intuitive approach to data management that is easy to deploy, access and scale with minimal tuning while also maintaining the referential integrity and ACID characteristics of traditional database systems.

**This README is written for developers looking to contribute to the codebase. Detailed user documentation is available at [http://cinchapi.org/concourse](http://cinchapi.org/concourse)**

***You may also refer to the [API](concourse/README.md) documentation for details on using Concourse in a third party application.***

## System Requirements

### Memory
Concourse immediately writes all data to disk, but also keeps recently written and read content in memory. Since memory operations are faster, having more on hand is great, but you can operate Concourse safely with a minimum 256 MB heap.

### Operating System
Concourse is only supported on Linux and Mac OS X operating systems. Things _should_ also work on Windows, but we can't make any guarantees.

### Java
Concourse runs on Java 1.7.

## General Information

### Versioning

This is version 0.3.3 of Concourse.

Concourse will be maintained under the [Semantic Versioning](http://semver.org)
guidelines such that release versions will be formatted as `<major>.<minor>.<patch>`
where

* breaking backward compatibility bumps the major,
* new additions while maintaining backward compatibility bumps the minor, and
* bug fixes or miscellaneous changes bumps the patch.

### Modules
This repository contains several modules that form the concourse-core.

* The **concourse** project contains the core API, which is the foundation for everything in Concourse. This API is public and should be used in third-party applications to interact with Concourse.
* The **concourse-server** project contains all the server-side logic for data storage and retrieval. You should __*never*__ include this code in a third-party application, but should install the concourse-server distribution and interact with it using the concourse api.
* The **concourse-shell** project contains the code for the Concourse Action SHell (CaSH), which is shipped with concourse-server.
* The **concourse-testing** project contains long running end-to-end tests that should be run separately from the build process.

## Contributing
### Learn how things work
Look over the [wiki](https://cinchapi.atlassian.net/wiki/display/CON/Concourse) to learn about the internal architecture.

### Pick an issue
1. Choose an open [issue](https://cinchapi.atlassian.net/browse/CON) or implement a new feature.
2. Create an account in Jira.
3. If necessary, create a new ticket to track your work. Otherwise, assign an existing ticket. yourself.

### Write some code
1. Read the [coding standards](https://cinchapi.atlassian.net/wiki/display/CON/Coding+Standards).
2. [Fork](https://github.com/cinchapi/concourse/fork) the repo.
3. Clone your forked version of concourse.git.

		$ git clone git@github.com:<username>/concourse.git

4. Start writing code :)

### Run from Eclipse
1. Install the [gradle eclipse plugin](https://github.com/spring-projects/eclipse-integration-gradle/#installing-gradle-tooling-from-update-site).
2. Import all the concourse projects.
3. Import the launch configurations from `concourse-server/launch` and `concourse-shell/launch`.
4. Use **Start Server** to start a local server with the default configuration.
5. Use **Stop Server** to stop all locally running servers.
6. Use **Launch CaSH** to launch a cash terminal that is connected to a locally running server listening on the default port.


### Build your changes
Always check that Concourse builds properly after you've made changes. We use [Gradle](http://www.gradle.org/) as the build system.

	$ ./gradlew clean build installer

If all goes well, run the installer in `concourse-server/build/distributions`, launch CaSH and sanity check some [smoke tests](https://cinchapi.atlassian.net/wiki/display/CON/Testing+Zone).

### Submit your changes
1. Send a pull request.
2. Update the Jira ticket with the commit hashes for your change.

### Join the team
Shoot us an [email](mailto:jeff@cinchapi.org) if you want to become a regular contributor and help with strategic planning!


### Report Issues
If you run into any issues, have questions or want to request new features, please create a ticket in [Jira](https://cinchapi.atlassian.net/browse/CON).

### Ask Questions
Ping us at [concourse-devs@cinchapi.org](mailto:concourse-devs@cinchapi.org) if you ever have any questions. We're happy to help.


## Credits
### Author

* Jeff Nelson

### License

Copyright © 2013-2014 Jeff Nelson, Cinchapi Software Collective.

Concourse is released under the MIT License. For more information see LICENSE, which is included with this package.


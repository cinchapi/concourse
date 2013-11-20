# Concourse

[Concourse](http://cinchapi.org/concourse) is a schemaless and distributed version control database with optimistic availability, serializable transactions and full-text search. Concourse provides a more intuitive approach to data management that is easy to deploy, access and scale with minimal tuning while also maintaining the referential integrity and ACID characteristics of traditional database systems.

**This README is written for developers looking to contribute to the codebase. Detailed user documentation is available at [http://cinchapi.org/concourse](http://cinchapi.org/concourse)**

## General Information

### Versioning

This is version 0.2.0 of Concourse.

Concourse will be maintained under the [Semantic Versioning](http://semver.org)
guidelines such that release versions will be formatted as `<major>.<minor>.<patch>`
where

* breaking backward compatibility bumps the major,
* new additions while maintaining backward compatibility bumps the minor, and
* bug fixes or miscellaneous changes bumps the patch.

### Modules
This repository contains several modules that form the concourse-core.

* The **concourse-api** project contains the core API, which is the foundation for everything in Concourse. This API is public and should be used in third-party applications to interact with Concourse.
* The **concourse-server** project contains all the server-side logic for data storage and retrieval. You should __*never*__ include this code in a third-party application, but should install the concourse-server distribution and interact with it using the concourse-api.
* The **concourse-shell** project contains the code for the Concourse Action SHell (CaSH), which is shipped with concourse-server.
* The **concourse-testing** project contains long running end-to-end tests that should be run separately from the build process.

## Contributing
### Learn how things work
Look over the [wiki](https://cinchapi.atlassian.net/wiki/display/CON/Concourse) to learn about the internal architecture.

### Pick an issue
1. Fix an open [issue](https://cinchapi.atlassian.net/browse/CON) or implement a new feature.
2. Create an account in Jira
3. Create a ticket to track your work.
4. Assign the ticket (existing or newly created) to yourself.

### Write some code
1. Read the [coding standards](https://cinchapi.atlassian.net/wiki/display/CON/Coding+Standards).
2. [Fork](https://github.com/cinchapi/concourse/fork) the repo and get to work!

### Build your changes
Always check that Concourse builds properly after you've made changes. We use [Gradle](http://www.gradle.org/) as the build system.

	$ ./gradlew clean build distZip
	
If all goes well, unzip the distribution in `concourse-server/build/distribution`, launch CaSH and sanity check some [smoke tests](https://cinchapi.atlassian.net/wiki/display/CON/Testing+Zone).

### Submit your changes
1. Send a pull request
2. Update the Jira ticket with the commit hashes for your change

### Join the team
Shoot us an [email](mailto:jeff@cinchapi.org) if you want to become a regular contributor and help with strategic planning!


## Reporting Issues
If you run into any issues, have questions or want to request new features, please create a ticket in [Jira](https://cinchapi.atlassian.net/browse/CON).

## Asking Questions
Ping us at [concourse-devs@cinchapi.org](mailto:concourse-devs@cinchapi.org) if you ever have any questions. We're happy to help.


## Credits
### Author

* Jeff Nelson (jeff@cinchapi.org)

### License

Copyright © 2013 Cinchapi Software Collective.

Concourse is released under the MIT License. For more information see LICENSE,
which is included with this package.

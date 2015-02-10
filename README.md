# Concourse

[Concourse](http://cinchapi.org/concourse) is a schemaless and distributed version control database with optimistic availability, serializable transactions and full-text search. Concourse provides a more intuitive approach to data management that is easy to deploy, access and scale with minimal tuning while also maintaining the referential integrity and ACID characteristics of traditional database systems.

## Overview
* [End User Installation Guide](https://cinchapi.atlassian.net/wiki/display/CON/Getting+Started#GettingStarted-InstallConcourse)
* [End User Upgrade Guide](https://cinchapi.atlassian.net/wiki/display/CON/Upgrade+Guide)
* [Hello World Tutorial](https://cinchapi.atlassian.net/wiki/display/CON/Getting+Started)
* [API Documentation](concourse/README.md)
* [Developer Setup](https://cinchapi.atlassian.net/wiki/display/CON/Concourse+Dev+Setup)
* [Understanding the codebase](https://cinchapi.atlassian.net/wiki/display/CON/Understanding+the+codebase)
* [Data Model](https://cinchapi.atlassian.net/wiki/display/CON/Data+Model)
* [Storage Model](https://cinchapi.atlassian.net/wiki/display/CON/Storage+Model)

### System Requirements

#### Memory
Concourse immediately writes all data to disk, but also keeps recently written and read content in memory. Since memory operations are faster, having more on hand is great, but you can operate Concourse safely with a minimum 256 MB heap.

#### Operating System
Concourse is only supported on Linux and Mac OS X operating systems. Things _should_ also work on Windows, but we can't make any guarantees.

#### Java
Concourse runs on Java 1.7.

### General Information

#### Versioning

This is version 0.4.3 of Concourse.

Concourse will be maintained under the [Semantic Versioning](http://semver.org)
guidelines such that release versions will be formatted as `<major>.<minor>.<patch>`
where

* breaking backward compatibility bumps the major,
* new additions while maintaining backward compatibility bumps the minor, and
* bug fixes or miscellaneous changes bumps the patch.

#### Modules
This repository contains several modules that form the concourse-core.

* The **concourse** project contains the core API, which is the foundation for everything in Concourse. This API is public and should be used in third-party applications to interact with Concourse.
* The **concourse-server** project contains all the server-side logic for data storage and retrieval. You should __*never*__ include this code in a third-party application, but should install the concourse-server distribution and interact with it using the concourse api.
* The **concourse-shell** project contains the code for the Concourse Action SHell (CaSH), which is shipped with concourse-server.
* The **concourse-integration-tests** project contains long running end-to-end tests that should be run separately from the build process.

## Contributing
Read the [contributing guidelines](CONTRIBUTING.md) to learn how to get involved in the community. We value and welcome constructive contributions from anyone regardless of skill level :)

### Mailing Lists

* [concourse-devs](https://groups.google.com/forum/#!forum/concourse-devs)
* [concourse-users](https://groups.google.com/forum/#!forum/concourse-users)


## Credits
### Author

* Jeff Nelson

### License

Copyright © 2013-2014 Jeff Nelson, Cinchapi Software Collective.

Concourse is released under the MIT License. For more information see LICENSE, which is included with this package.


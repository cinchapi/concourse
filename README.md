# Concourse

[Concourse](http://concoursedb.com) is a schemaless and distributed version control database with [automatic indexing](http://concoursedb.com/blog/index-all-the-things/), acid transactions and full-text search. Concourse provides a more intuitive approach to data management that is easy to deploy, access and scale with minimal tuning while also maintaining the referential integrity and ACID characteristics of traditional database systems.

## Introduction
The Concourse data model is lightweight and flexible. Unlike other databases, Concourse is completely schemaless and does not hold data in tables or collections. Instead, Concourse is simply a distributed graph of records. Each record has multiple keys. And each key has one or more distinct values. Like any graph, you can link records to one another. And the structure of one record does not affect the structure of another.
### Writing Data
```java
import org.cinchapi.concourse

// Establish connection to Concourse Server
Concourse concourse = Concourse.connect();

// Insert a value for the "name" key in record 1
concourse.set("name", "Jeff Nelson", 1);

// Append an additional value for the "name" key in record 1
concourse.add("name", "John Doe", 1);

// Remove a value for the "name" key in record 1
concourse.remove("name", "Jeff Nelson", 1)
```

### Reading Data
```java
// Get the oldest value for the "name" key in record 1
concourse.get("name", 1);

// Fetch all the values for the "name" key in record 1
concourse.fetch("name", 1);

// Find all the records that have a value of "Jeff Nelson" for the "name" key
concourse.find("name", Operator.EQUALS, "Jeff Nelson");
```

### Transactions
```java
try {
  // Transfer $50 from acct1 to acct2
  concourse.stage(); //start transaction
  concourse.set("balance", concourse.get("balance", acct1) - 50), acct1);
  concourse.set("balance", concourse.get("balance", acct2) + 50), acct2);
  concourse.commit();
}
catch (TransactionException e) {
  concourse.abort();
}
```

For more usage information please review the [Concourse Guide](http://concoursedb.com/guide) and [API documentation](concourse/README.md).

## Overview
* [Installation](http://concoursedb.com/guide/installation)
* [Tutorial](http://concoursedb.com/guide/tutorial)
* [API](concourse/README.md)
* [Developer Setup](https://cinchapi.atlassian.net/wiki/display/CON/Concourse+Dev+Setup)
* [Codebase](http://concoursedb.com/guide/the-codebase)
* [Data Model](http://concoursedb.com/guide/data-model/)
* [Storage Model](http://concoursedb.com/guide/storage-model/)

### System Requirements

#### Memory
Concourse immediately writes all data to disk, but also keeps recently written and read content in memory. Since memory operations are faster, having more on hand is great, but you can operate Concourse safely with a minimum 256 MB heap.

#### Operating System
Concourse is only supported on Linux and Mac OS X operating systems. Things _should_ also work on Windows, but we can't make any guarantees.

#### Java
Concourse runs on Java 1.7.

### General Information

#### Versioning

<<<<<<< HEAD
This is version 0.4.3 of Concourse.
=======
This is version 0.5.0 of Concourse.
>>>>>>> de8748264fd8f0370664c027005cdaf90ba95252

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


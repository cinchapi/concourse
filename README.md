# Concourse

[Concourse](http://concoursedb.com) is a schemaless and distributed version control database with [automatic indexing](http://concoursedb.com/blog/index-all-the-things/), acid transactions and full-text search. Concourse provides a more intuitive approach to data management that is easy to deploy, access and scale with minimal tuning while also maintaining the referential integrity and ACID characteristics of traditional database systems.

## Introduction
The Concourse data model is lightweight and flexible. Unlike other databases, Concourse is completely schemaless and does not hold data in tables or collections. Instead, Concourse is simply a distributed graph of records. Each record has multiple keys. And each key has one or more distinct values. Like any graph, you can link records to one another. And the structure of one record does not affect the structure of another.
### Connecting to Concourse
Each Concourse client connects to a Concourse Server environment on behalf of a user. Concourse Server can accomodate many concurrent connections. While there is a 1:1 mapping between each client connection and the environment to which it is connected, a user can have multiple concurrent client connections to the same environment or different environments. You connect to Concourse using one of the `connect` methods in the `Concourse` class by specifying some combination of a `host`, `port`, `username`, `password` and `environment`. 

The easiest way to connect is to use the default parameters. This attempts to connect to the default environment of the local server listening on port 17171.
```java
Concourse concourse = Concourse.connect();
```

Of course, you can always specify an environment other than the default one when connecting to Concourse Server. For instance, you can connect to the "staging" environment of the default local server.
```java
Concourse concourse = Concourse.connect("staging");
```
*You can always connect to an existing environment. If you try to create a client connection with an environment that doesn't exist, it will be dynamically created. There is never a need to explicitly define environments in Concourse*

Finally, you can specify all the connection parameters to override the default values. For instance, you can connect to the production environment on remote server as a non-admin user.
```java
Concourse concourse = Concourse.connect("http://remote-server.com", 11345, "myusername", 
  "mycomplexpassword", "production");
```

### Writing to Concourse
Concourse allows you to write data immediately without specifying a schema or creating any explicit structure.
```java
// Insert a value for the "name" key in record 1
concourse.set("name", "Jeff Nelson", 1);

// Append an additional value for the "name" key in record 1
concourse.add("name", "John Doe", 1);

// Remove a value for the "name" key in record 1
concourse.remove("name", "Jeff Nelson", 1)
```

### Reading from Concourse
Concourse automatically creates primary, secondary and fulltext indexes for all of your data so you can perform efficient predicate, range, and search queries on anything at anytime.
```java
// Get the oldest value for the "name" key in record 1
concourse.get("name", 1);

// Fetch all the values for the "name" key in record 1
concourse.fetch("name", 1);

// Find all the records that have a value of "Jeff Nelson" for the "name" key
concourse.find("name", Operator.EQUALS, "Jeff Nelson");
```

### Transactions
Concourse provides cross-record transactions that are fully ACID compliant: all operations succeed or fail together; writes are visible to all readers only after being successfully committed; serializable isolation with [just-in-time locking ](http://concoursedb.com/blog/just-in-time-locking/) prevents all read or write phenomena and committed transactions are immediately stored to disk so they persist in the event of power loss, crash or error.
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

### Version Control
Concourse automatically and efficiently tracks revisions to your data. This means that you can easily audit changes and rever to previous states without downtime.
```java
// return all the revisions to the record
concourse.audit(1);

// return all the revisions to just the "name" key in the record
concourse.audit("name", 1);

// Return a timeseries for all the changes to the "name" key in 
// the record between last month and last week
concourse.chronologize("name", 1, Timestamp.parse("last month"), Timestamp.parse("last week"));
```

### Reading from the past
Version control in Concourse also  means that you have the power to query and fetch data from any point in the past, which makes it  possible to build applications that know what was known when and can analyze real-time changes over time.
```java
// Find data matching criteria in the past
concourse.find("age", Operator.LESS_THAN, 50, Timestamp.parse("last year"));

// Fetch data in a previous state from a record
concourse.get("name", 1, Timestamp.parse("yesterday"));
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

This is version 0.5.0 of Concourse.

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


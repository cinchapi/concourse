# Concourse

[ConcourseDB](http://concoursedb.com) is a self-tuning database that practically runs itself. Concourse offers features like automatic indexing, version control and distributed ACID transactions to provide a more efficient approach to data management that is easy to deploy, access and scale while maintaining the strong consistency of traditional database systems.

This is version 0.5.0 of Concourse.

## Quickstart
Let's assume we have the following array of JSON objects:
```json
[
  {
    "name": "Lebron James",
    "age": 30,
    "team": "Cleveland Cavaliers"
  },
  {
    "name": "Kevin Durant",
    "age": 26,
    "team": "OKC Thunder"
  },
  {
    "name": "Kobe Bryant",
    "age": 36,
    "team": "LA Lakers"
  }
]
```
We can use Concourse to quickly insert the data and do some quick analysis. Notice that we don't have to declare a schema, create any structure or configure any indexes.
```java
package org.cinchapi.concourse.examples;

import java.util.Set;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.TransactionException;
import org.cinchapi.concourse.thrift.Operator;

import com.google.common.collect.Iterables;

public static void main(String... args) {
    Concourse concourse = Concourse.connect();
    
    Set<Long> records = concourse.insert(json); // Each object is added to a distinct record 
    long lebron = Iterables.get(records, 0);
    long durant = Iterables.get(records, 1);
    long kobe = Iterables.get(records, 2);

    // I can get or modify individual attributes for each record without
    // loading the entire data set
    concourse.get("age", kobe);
    concourse.add("name", "KD", durant);
    concourse.remove("jersey_number", 6, lebron);

    // Since data is automatically indexed, I can easily find records that
    // match a criteria
    concourse.find("team", "=", "Chicago Bulls");
    concourse.find("age", Operator.BETWEEN, 22, 29);

    // If I'm curious about how the data looked in the past, I can perform
    // historical reads
    concourse.get("age", durant, Timestamp.parse("04/2009"));
    concourse.find("team", Operator.EQUALS, "Chicago Bulls",
            Timestamp.parse("2011"));
    concourse.find("age", Operator.BETWEEN, 22, 29,
            Timestamp.parse("2 years ago"));

    // I can also analyze how data has changed over time and restore
    // previous states.
    concourse.audit("team", lebron);
    concourse.revert("jersey_number", kobe, Timestamp.parse("10 years ago"));

    // I can also rely on transactions if any changes across records need
    // ACID guarantees.
    concourse.stage();
    try {
        concourse.set("current_team", "OKC Thunder", lebron);
        concourse.set("current_team", "Cleveland Cavs", durant);
        concourse.commit();
    }
    catch (TransactionException e) {
        concourse.abort();
    }
}
```
You can find more examples in the [examples](examples) directory. More information is also available in the [Concourse Guide](http://concoursedb.com/guide) and [API documentation](concourse/README.md).

## Motivation
Whether you use SQL or NoSQL, building data driven software forces you to spend too much time managing the database.

###### Planning
You have an awesome idea for a product? Great! But before you start building, you've got to figure out how to translate that idea into a rigid database structure. And if you want to change a feature down the line, you've got to migrate your existing data. This slows down your ability to prototype, iterate and pivot.

###### Profiling
Got performance problems? Well, now you have to spend time finding the culprit. Did you index the data correctly? Are your queries properly structured? Do you have an external cache in place? Meanwhile, your competitors are busy gaining traction.

###### Scaling
You have more users and more data. Now the database needs more resources. Sure, the popular NoSQL solutions make this easier, but eventual consistency forces you to figure out what to do when the database can't be trusted.

###### Optimizing
Your database is mission critical, but as you can see, it needs a babysitter. This forces you to allocate an abundance of resources just to figure out which settings to tweak each time your app grows. Time and money wasted.

## The Solution
Concourse is an entirely new kind of database that is designed to automatically adapt to any workload or data scale.

###### Automatic Indexing
You no longer need to plan queries in advance because Concourse automatically **indexes all of your data** while guaranteeing **constant time writes** that are super fast. Concourse fully supports ad-hoc range and predicate queries and automatically caches frequently requested data for optimal performance.

###### Version Control
Concourse automatically **tracks changes to your data**, just like Git does for source code. Of course this means that you can easily audit changes and revert to previous states without downtime; but it also means that you have the power to **query data from the past**. Version control in Concourse makes it possible to build applications that know what was known when and can analyze real-time changes over time.

###### ACID Transactions
Concourse supports truly distributed ACID transactions without restriction. And we use dynamic resource management with **just-in-time locking** to ensure that they deliver both the highest performance and strongest consistency. So no need to guess when your data will eventually become consistent. When distributed Concourse responds to a query, you can **trust the results immediately**.

###### Simple Data Model
Concourse's **document-graph** structure is lightweight and flexible–it supports any kind of data and very large scales. Data about each person, place or thing is stored in a record, which is simply a collection of key/value pairs. And you can create links among records to easily **model all the relationships within your data**.

###### Schemaless
Since Concourse makes very few assumptions about data, it integrates with your application seamlessly and never needs a translator (goodbye object-relational impedance mismatch)! You **never have to declare structure up front**–no schema, tables, or indexes–or specify value types because Concourse is smart enough to figure it out. Concourse **dynamically adapts to your application** so that you can focus on building value without having to drag the database along.

###### Search
Concourse supports rich full text search right out the box, so you don't need to deploy an external search server. Data is automatically indexed and **searchable in real-time** without ever diminishing write performance. In Concourse, you can always perform **as-you-type searches** that match full or partial terms.

## Overview
##### System Requirements
* At least 256 MB of available memory
* Linux or OS X
* Java 1.7+

##### Versioning
Concourse will be maintained under the [Semantic Versioning](http://semver.org)
guidelines such that release versions will be formatted as `<major>.<minor>.<patch>`
where

* breaking backward compatibility bumps the major,
* new additions while maintaining backward compatibility bumps the minor, and
* bug fixes or miscellaneous changes bumps the patch.

##### Additional Resources
* [Installation](http://concoursedb.com/guide/installation)
* [Tutorial](http://concoursedb.com/guide/tutorial)
* [API](concourse/README.md)
* [Developer Setup](https://cinchapi.atlassian.net/wiki/display/CON/Concourse+Dev+Setup)
* [Codebase](http://concoursedb.com/guide/the-codebase)
* [Data Model](http://concoursedb.com/guide/data-model/)
* [Storage Model](http://concoursedb.com/guide/storage-model/)

## Contributing
Read the [contributing guidelines](CONTRIBUTING.md) to learn how to get involved in the community. We value and welcome constructive contributions from anyone regardless of skill level :)

##### Mailing Lists

* [concourse-devs](https://groups.google.com/forum/#!forum/concourse-devs)
* [concourse-users](https://groups.google.com/forum/#!forum/concourse-users)

## Credits
##### Author

* Jeff Nelson

##### License

Copyright © 2013-2015 Cinchapi, Inc.

Concourse is released under the Apache License, Version 2.0. For more information see LICENSE, which is included with this package.

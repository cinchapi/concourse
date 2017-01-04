# Concourse

 ![](https://img.shields.io/badge/version-0.6.0-green.svg)
 ![](https://img.shields.io/badge/status-alpha-orange.svg) ![](https://img.shields.io/badge/license-Apache%202-blue.svg)
 [![Join the chat at https://gitter.im/cinchapi/concourse](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cinchapi/concourse?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
 [![](https://circleci.com/gh/cinchapi/concourse.svg?style=shield&circle-token=954a20e6114d649b1b6a046d95b953e7d05d2e2f)](https://circleci.com/gh/cinchapi/concourse)

> [Concourse](http://concoursedb.com) is a self-tuning database for both ACID transactions and ad-hoc analytics across time. Developers prefer Concourse because it makes it easy to build operational software with on-demand data intelligence. Furthermore, Concourse greatly simplifies end-to-end data management by requiring no extra infrastructure, no prior configuration and no continuous tuning–all of which greatly reduce costs, and allow developers to focus on core business problems.

This is version 0.6.0 of Concourse.

## Quickstart
[![Deploy to Heroku](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/cinchapi/concourse/tree/develop)

Let's assume we have the an array of JSON objects that describe NBA players.
```python
from concourse import *

data = [
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
###### You can use Concourse to quickly insert the data and do some quick analysis. Notice that we don't have to declare a schema, create any structure or configure any indexes.
```python
concourse = Concourse.connect()
records = concourse.insert(data) # each object is added to a distinct record
lebron = records[0]
durant = records[1]
kobe = records[2]
```
###### You can read and modify individual attributes without loading the entire record.
```python
concourse.get(key="age", record=kobe)
concourse.add(key="name", value="KD", record=durant)
concourse.remove(key="jersey_number", value=23, record=lebron)
```
###### You can easily find records that match a criteria and select the desired since everything is automatically indexed.
```python
concourse.select(criteria="team = Chicago Bulls")
concourse.select(keys=["name", "team"], criteria="age bw 22 29")
```
###### You can even query data from the past without doing any extra work.
```python
concourse.get(key="age", record=durant, time="04/2009")
concourse.find("team = Chicago Bulls at 2011")
concourse.select(criteria="age > 25 and team != Chicago Bulls", time="two years ago")
```
###### It is very easy to analyze how data has changed over time and revert to previous states.
```python
# Analyze how data has changed over time and revert to previous states
concourse.audit(key="team", record=lebron)
concourse.revert(key="jersey_number", record=kobe, time="two years ago")
```
###### And ACID transactions are available for important, cross record changes.
```python
concourse.stage()
try:
    concourse.set(key="current_team", value="OKC Thunder", record=lebron)
    concourse.set(key="current_team", value="Cleveland Cavs", record=durant)
    concourse.commit()
except TransactionException:
    concourse.abort()
```
You can find more examples in the [examples](examples) directory. More information is also available in the [Concourse Guide](http://concoursedb.com/guide) and [API documentation](concourse/README.md).

## Motivation
Whether you use SQL or NoSQL, it's hard to get real-time insight into your mission critical data because most systems are only optimized for either transactions or analytics, not both. As a result, end-to-end data management requires complex data pipelining, which slows down development, complicates infrastructure and increases costs.

## The Solution
Concourse is an integrated and self-managing transactional database that enables real time ad-hoc analytics without any configuration.

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
* Java 1.8+

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
* [API](concourse-driver-java/README.md)
* [Developer Setup](http://wiki.cinchapi.com/display/OSS/Concourse+Developer+Setup)
* [Codebase](http://concoursedb.com/guide/the-codebase)
* [Data Model](http://concoursedb.com/guide/data-model/)
* [Storage Model](http://concoursedb.com/guide/storage-model/)

## Contributing
Read the [contributing guidelines](CONTRIBUTING.md) to learn how to get involved in the community. We value and welcome constructive contributions from anyone, regardless of skill level :)

##### Mailing Lists

* [concourse-devs](https://groups.google.com/forum/#!forum/concourse-devs)
* [concourse-users](https://groups.google.com/forum/#!forum/concourse-users)

## Credits
##### Author

* [Jeff Nelson](https://www.linkedin.com/in/jtnelson1)

##### License

Copyright © 2013-2017 Cinchapi Inc.

Concourse is released under the Apache License, Version 2.0. For more information see LICENSE, which is included with this package.

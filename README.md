# Concourse

 ![](https://img.shields.io/badge/version-1.0.0-green.svg)
 ![](https://img.shields.io/badge/status-alpha-orange.svg) ![](https://img.shields.io/badge/license-Apache%202-blue.svg)
 [![Join the chat at https://gitter.im/cinchapi/concourse](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cinchapi/concourse?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
 [![](https://circleci.com/gh/cinchapi/concourse.svg?style=shield&circle-token=954a20e6114d649b1b6a046d95b953e7d05d2e2f)](https://circleci.com/gh/cinchapi/concourse)

> [Concourse](http://concoursedb.com) is a distributed database warehouse for transactions search and analytics across time. Developers prefer Concourse because it simplifies building misssion-critical systems with on-demand data intelligence. Furthermore, Concourse makes end-to-end data management trivial by requiring no extra infrastructure, no prior configuration and no continuous tuning–all of which greatly reduce costs, and allow developers to focus on core business problems.

This is version 1.0.0 of Concourse.

## Quickstart
### Docker
Using Concourse via Docker is the quickest way to get started.

#### Run `concourse`
```bash
docker run -p 1717:1717 --name concourse cinchapi/concourse
```
NOTE: This will run Concourse in the foreground. To run in the background, add a `-d` flag to the `docker run` command.

#### Run `concourse` with a peristent/shared directory
```bash
docker run -p 1717:1717 -v </path/to/local/data>:/data --name concourse cinchapi/concourse
```

#### Run `concourse` with a custom HEAP_SIZE
```bash
docker run -p 1717:1717 -e CONCOURSE_HEAP_SIZE=<HEAP_SIZE> --name concourse cinchapi/concourse
```

#### Run `concourse shell` and connect to the running `concourse` docker container
```bash
docker run -it --rm --link concourse:concourse cinchapi/concourse shell --host concourse --password admin
```

#### Run `concourse shell` and connect to a running `concourse` container spun up using `docker-compose`
```bash
docker-compose run concourse shell --host concourse
```

#### Use the `concourse import` to perform an [interactive import](https://docs.cinchapi.com/concourse/imports/) that reads input from the command line
```bash
docker run -it --rm --link concourse:concourse cinchapi/concourse import --host concourse --password admin
```

#### Use the `concourse import` to import a **file** from the host machine into the `concourse` docker container
```bash
xargs -I % docker run -i --rm --link concourse:concourse --mount type=bind,source=%,target=/data/% cinchapi/concourse import --host concourse --password admin -d /data/% <<< /absolute/path/to/file
```

#### Run server-side management commands (e.g. `concourse debug`) within the running container
```bash
docker exec -it concourse concourse <command> <args>
```
For example, you can call the `concourse users sessions` command
```bash
docker exec -it concourse concourse users sessions --password admin
```

For more information, visit [https://docs.cinchapi.com/concourse/quickstart](https://docs.cinchapi.com/concourse/quickstart).

### Usage
Let's assume we have the an array of JSON objects corresponding to NBA players.
*NOTE: These examples assume you're using Concourse Shell, but are easily adaptable to any of the Concourse client drivers or REST API.*
```groovy
data = '[
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
]'
```
###### You can use Concourse to quickly insert the data and do some quick analysis. Notice that we don't have to declare a schema, create any structure or configure any indexes.
```groovy
ids = insert(data) // each object is added to a distinct record
lebron = ids[0]
durant = ids[1]
kobe = ids[2]
```
###### You can read and modify individual attributes without loading the entire record.
```groovy
get(key="age", record=kobe)
add(key="name", value="KD", record=durant)
remove(key="jersey_number", value=23, record=lebron)
```
###### You can easily find records that match a criteria and select the desired since everything is automatically indexed.
```groovy
select(criteria="team = Chicago Bulls")
select(keys=["name", "team"], criteria="age bw 22 29")
```
###### You can even query data from the past without doing any extra work.
```groovy
// Return data from 04/2009 from records that match now
get(key="age", record=durant, time=time("04/2009")) 

// Return records that matched in 2011
find("team = Chicago Bulls at 2011") 

// Return data from two years ago from records that match now
select(criteria="age > 25 and team != Chicago Bulls", time=time("two years ago")) 
```
###### It is very easy to analyze how data has changed over time and revert to previous states.
```groovy
// Analyze how data has changed over time and revert to previous states
audit(key="team", record=lebron)
revert(key="jersey_number", record=kobe, time=time("two years ago"))
```
###### And ACID transactions are available for important, cross record changes.
```groovy
stage
set(key="current_team", value="OKC Thunder", record=lebron)
set(key="current_team", value="Cleveland Cavs", record=durant)
commit
```
...or using shorthand syntax
```groovy
stage({
    set(key="current_team", value="OKC Thunder", record=lebron);
    set(key="current_team", value="Cleveland Cavs", record=durant);
})
```
You can find more examples in the [examples](examples) directory. More information is also available in the [Concourse Guide](https://docs.cinchapi.com/concourse).

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
* [Installation](https://docs.cinchapi.com/concourse/guide/installation/)
* [Introduction](https://docs.cinchapi.com/concourse/guide/introduction/)
* [API](concourse-driver-java/README.md)
* [Developer Setup](http://wiki.cinchapi.com/display/OSS/Concourse+Developer+Setup)

## Contributing
Read the [contributing guidelines](CONTRIBUTING.md) to learn how to get involved in the community. We value and welcome constructive contributions from anyone, regardless of skill level :)

##### Mailing Lists

* [concourse-devs](https://groups.google.com/forum/#!forum/concourse-devs)
* [concourse-users](https://groups.google.com/forum/#!forum/concourse-users)

## Credits
##### Author

* [Jeff Nelson](https://www.linkedin.com/in/jtnelson1)

##### License

Copyright © 2013-2019 Cinchapi Inc.

Concourse is released under the Apache License, Version 2.0. For more information see LICENSE, which is included with this package.

# Concourse
[Concourse](http://cinchapi.org/concourse) is a schemaless and distributed version control database with optimistic availability, serializable transactions and full-text search. Concourse provides a more intuitive approach to data management that is easy to deploy, access and scale with minimal tuning while also maintaining the referential integrity and ACID characteristics of traditional database systems.

## Introduction
---
### Data Model
The Concourse data model is lightweight and flexible which enables it to support any kind of data at very large scales. Concourse trades unnecessary structural notions of schemas, tables and indexes for a more natural modeling of data based solely on the following concepts:

* **Record** - A logical grouping of data about a single person, place or thing (i.e. an object). Each *record* is a collection of key/value pairs that are together identified by a unique primary key.
*  **Key** - An attribute that maps to a set of *one or more* distinct values. A *record* can have many different keys and the keys in one record do not affect those in another *record*.
*  **Value** - A dynamically typed quanity that is associated with a *key* in a *record*.

#### Data Types
Concourse natively stores most of the Java primitives: boolean, double, float, integer, long, and string (UTF-8). Otherwise, the value of the `#toString()` method for the Object is stored.

#### Links
Concourse supports linking a *key* in one *record* to another *record* using the `#link(String, long, long)` method. Links are one directional, but it is possible to add two inverse links so simulate bi-directionality.

### Transactions
By default, Concourse conducts every operation in  `autocommit` mode where every change is immediately written. Concourse also supports the ability to stage a group of operations in transactions that are atomic, consistent, isolated, and durable using the `#stage()`, `#commit()` and `#abort()` methods.

## Documentation

---
##### Concourse connect()
Create a new client connection using the details provided in *concourse_client.prefs*. If the prefs file does not exist or does not contain connection information, then the default connection details (*admin@localhost:1717*) will be used.
###### Returns
the database handler
###### Example
	Concourse concourse = Concourse.connect();
---
##### Concourse connect(String host, int port, String username, String password)
Create a new client connection for *username@host:port* using *password*.
###### Parameters
* host
* port
* username
* password

###### Returns
the database handler
###### Example
	Concourse concourse = Concourse.connect("localhost", 1717, "admin", "admin");
---
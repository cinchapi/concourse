cash(1) -- Concourse Action SHell
=================================

## DESCRIPTION
**Cash** is an interactive interpreter for Concourse that is backed by the full power of the Groovy scripting language.

## USAGE
You can use Java or Groovy syntax (e.g. no paranthesis) to call methods in standalone statements:

**method** arg1, arg2, arg3 *OR* **method(**arg1, arg2, arg3**)**

However, you MUST use Java syntax with nested method calls:

**method(**arg1, arg2, **method1(**arg3**)****)**

## METHODS
### Basic WRITE Operations

Concourse allows you to write data immediately without specifying a schema or creating any explict strcuture. The Concourse data model is simple. Data about each person, place or thing is held in a *record*, which is simply a collection of *key/value* pairs that are together identified by a unique *primary key*. A key can map to multiple distinct values, even if those values have different types.

* `add`(key, value, record) -> *boolean*:
    Add *key* (String) AS *value* (Object) TO *record* (long) if it is not already contained.

* `remove`(key, value, record) -> *boolean*:
	Remove *key* (String) as *value* (Object) FROM *record* (long) if it is contained.

* `set`(key, value, record):
	Atomically remove all the values currently mapped from *key* (String) in *record* (long) and add *key* AS *value* (Object) TO *record*.

### Basic READ Operations

Concourse automatically creates primary, secondary and full text indexes for all of your data so you can perform efficient predicate, range and search queries on anything at anytime. Concourse automatically caches frequently request data in memory for optimal performance.

* `browse`(record) -> *Map[String, Set[Object]]*:
	Return a map of every key in *record* (long) to its mapped set of values. This method is the atomic equivalent of calling `fetch(describe(record), record)`.

* `browse`(key) -> *Map[Object, Set[Long]]*:
	Return a map of every value mapped from *key* (String) to the set of records that contain the key/value mapping.

* `describe`(record) -> *Set[String]*:
	Return the keys that currently exist (e.g. have at least one mapped value) in *record* (long).

* `fetch`(key, record) -> *Set[Object]*:
	Return all the values mapped from *key* (String) in *record* (long).

* `find`(criteria) -> *Set[Long]*:
	Return all the records that satisfy the *criteria* (Criteria).

	READ THE **CRITERIA** SECTION FOR MORE INFORMATION

* `find`(key, operator, value[, value1]) -> *Set[Long]*:
	Return all the records that satisfy *key* (String) *operator* (Operator) *value* (Object).

	READ THE **OPERATORS** SECTION FOR MORE INFORMATION

* `get`(key, record) -> *Object*:
	Return the oldest value currently mapped from *key* (String) in *record* (long).

* `search`(key, query) -> *Set[Long]*:
	Perform a full text search for *query* (String) against *key* (String) and return the records that contain matching values.

* `verify`(key, value, record) -> *boolean*:
	Return *true* if *value* (Object) is currently mapped from *key* (String) in *record* (long).

### Transaction Operations

Concourse provides cross-record transactions that are fully ACID compliant: all operations succeed or fail together; writes are visible to all readers only after being successfully committed; serializable isolation is used to prevent dirty reads, non-repeatable reads and phantom reads; and committed transactions are immediately stored to disk so they persist in the event of power loss, crash or error

* `stage`():
	Enable *staging* mode so that all subsequent changes are collected in a staging area before possibly being committed. Staged operations are guranteed to be reliable, all or nothing unit of works that allow correct recovery from failures and provide isolation between clients so that Concourse is always consistent (e.g. a Transaction). After this method returns, all subsequent operations will be done in *staging* mode until either `abort()` or `commit()` is called.

* `commit`() -> *boolean*:
	Attempt to permanently commit all the currently staged changes. This method returns *true* if and only if all the changes in the staging area can be successfully applied while keeping Concourse in a consistent state. Otherwise, this method returns *false* and all the changes are aborted. After this method returns, Concourse will return to *autocommit* mode and all subsequent changes will be committed immediately until `stage()` is called again.

* `abort`():
	Discard any changes that are currently staged for commit. After this function returns, Concourse will be set to *autocommit* mode and all subsequent changes will be written immediately.

### Atomic Operations

Concourse provides low-level atomic operations that combine other low-level operations into a single unit of work that all succeed or fail together. These methods can be used to perform some common multi-step tasks without creating a Transaction.

* `clear`(record):
	Atomically remove each key in *record* (long) and their mapped values.

* `clear`(key, record):
	Atomically remove each value mapped from *key* (String) in *record* (long).

* `insert`(json) -> *long*:
	Atomically add the key/value mappings described in the *json* (String) into a new record. The JSON formatted string must describe a JSON object that contains one or more keys, each of which maps to a JSON primitive or an array of JSON primitives (basically no embedded JSON objects).

* `insert`(json, record) -> *boolean*:
	Atomically add the key/value mappings described in the *json* (String) into *record* (long). The JSON formatted string must describe a JSON object that contains one or more keys, each of which maps to a JSON primitive or an array of JSON primitives (basically no embedded JSON objects).

* `verifyAndSwap`(key, expected, record, replacement) -> *boolean*:
	Atomically verify that *key* (String) equals the *expected* (Object) value in *record* (long) and, if so, swap it with the *replacement* (Object) value.

### Version Control Operations

Concourse automatically and efficiently tracks revisions to your data. This means that you can easily audit changes and revert to previous states without downtime.

* `audit`(record) -> *Map[Timestamp, String]*:
	Return a map of every modification timestamp in *record* (long) to a description of the revision that occured.

* `audit`(key, record) -> *Map[Timestamp, String]*:
	Return a map of every modification timestamp for *key* (String) in *record* (long) to a description of the revision that occured.

* `chronologize`(key, record) -> *Map[Timestamp, Set{Object]]*:
	Return a chronological mapping from each timestamp when a changed occurred to the set of values that were contained for *key* (String) in *record* (long) at the time.

* `chronologize`(key, record, start) -> *Map[Timestamp, Set[Object]]*:
	Return a chronological mapping from each timestamp between *start* (Timestamp) and now when a changed occurred to the set of values that were contained for *key* (String) in *record* (long) at the time.

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `chronologize`(key, record, start, end) -> *Map[Timestamp, Set[Object]]*:
	Return a chronological mapping from each timestamp between *start* (Timestamp) and  *end* (Timestamp) when a changed occurred to the set of values that were contained for *key* (String) in *record* (long) at the time.

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `revert`(key, record, timestamp):
	Atomically create new revisions that undo the relevant changes to have occured to *key* (String) in *record* (long) since *timestamp* (Timestamp)so that the values that are mapped from *key* in *record* are the same ones that were mapped at *timestamp*.

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

### Historical READ Operations

Version control in Concourse also means that you have the power to query and fetch data from any point in the past, which makes it possible to build applications that know what was known when and can analyze real-time changes over time.

* `browse`(record, timestamp) -> *Map[String, Set[Object]]*:
	Return a map of every key in *record* (long) to its mapped set of values at *timestamp* (Timestamp). This method is the atomic equivalent of calling `fetch(describe(record, timestamp), record, timestamp)`.

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `browse`(key, timestamp) -> *Map[Object, Set[Long]]*:
	Return a map of every value mapped from *key* (String) to the set of records that contained the mapping at *timestamp* (Timestamp).

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `describe`(record, timestamp) -> *Set[String]*:
	Return the keys that existed (e.g. had at least one mapped value) in *record* (long) at *timestamp* (Timestamp).

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `fetch`(key, record, timestamp) -> *Set[Object]*:
	Return all the values mapped from *key* (String) in *record* (long) at *timestamp* (Timestamp).

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `find`(key, operator, value, [value1, ] timestamp) -> *Set[Long]*:
	Return all the records that satisfied *key* (String) *operator* (Operator) *value* (Object) at *timestamp* (Timestamp).

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

	READ THE **OPERATORS** SECTION FOR MORE INFORMATION

* `get`(key, record, timestamp) -> *Object*:
	Return the oldest value mapped from *key* (String) in *record* (long) at *timestamp* (Timestamp).

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

* `verify`(key, value, record, timestamp) -> *boolean*:
	Return *true* if *value* (Object) was mapped from *key* (String) in *record* (long) at *timestamp* (Timestamp).

	READ THE **TIMESTAMP** SECTION FOR MORE INFORMATION

### Graph Operations

The people, places and things in your data are all connected. So Concourse allows you to model those relationships with enforced referential integrity and also provides a built-in interface to perform analytical graph queries.

* `link`(key, source, destination) -> *boolean*:
	Add a link from *key* (String) in the *source* (long) record to the *destination* (long) record if one does not exist.

* `unlink`(key, source, destination) -> *boolean*:
	Remove the link from *key* in the *source* (long) record that points to the *destination* (long) record if it exists.

### Miscellaneous Operations

Concourse has a few utility operations that provide useful metadata.

* `getServerEnvironment()` -> *String*:
	Return the environment of the server that is currently in use by the client.

* `getServerVersion()` -> *String*:
	Return the version of the server to which the client is currently connected.

* `ping`(record) -> *boolean*:
	Return *true* if the *record* (long) currently has at least one populated key.


### Compound Operations

For many of the operations above, Concourse provides convenience methods to combine two or more operations in non-atomic manner. In general, these methods operate on collections (i.e. get the value mapped from a key in a collection of records). You can embed the results of other methods or specify a list manually: `get`("name", `find`("age", gt, 30)) *OR* `get`("name", [1, 2, 3, 100]).

  * `add`(key, value, record*s*) -> *Map[Long, Boolean]*

  * `browse`(record*s*) -> *Map[Long, Map[String, Set[Object]]]*

  * `browse`(record*s*, timestamp) -> *Map[Long, Map[String, Set[Object]]]*

  * `clear`(key, record*s*)

  * `clear`(record*s*)

  * `clear`(key*s*, record)

  * `clear`(key*s*, record*s*)

  * `describe`(record*s*) -> *Map[Long, Set[String]]*

  * `describe`(record*s*, timestamp) -> *Map[Long, Set[String]]*

  * `fetch`(key*s*, record) -> *Map[String, Set[Object]]*

  * `fetch`(key*s*, record, timestamp) -> *Map[String, Set[Object]]*

  * `fetch`(key, record*s*) -> *Map[Long, Set[Object]]*

  * `fetch`(key, record*s*, timestamp) -> *Map[Long, Set[Object]]*

  * `fetch`(key*s*, record*s*) -> *Map[Long, Map[String, Set[Object]]]*

  * `fetch`(key*s*, record*s*, timestamp) -> *Map[Long, Map[String, Set[Object]]]*

  * `get`(key*s*, record) -> *Map[String, Object]*

  * `get`(key*s*, record, timestamp) -> *Map[String, Object]*

  * `get`(key, records) -> *Map[Long, Object]*

  * `get`(key, record*s*, timestamp) -> *Map[Long, Object]*

  * `get`(key*s*, record*s*) -> *Map[Long, Map[String, Object]]*

  * `get`(key*s*, record*s*, timestamp) -> *Map[Long, Map[String, Object]]*

  * `ping`(record*s*) -> *Map[Long, Boolean]*

  * `remove`(key, value, record*s*) -> *Map[Long, Boolean]*

  * `revert`(key*s*, record, timestamp)

  * `revert`(key, record*s*, timestamp)

  * `revert`(key*s*, record*s*, timestamp)

  * `set`(key, value, record*s*)

## CRITERIA

Concourse allows you to programatically build complex criteria to use in a `find` query. You can start to build a criteria statement by invoking the `where()` function.

### Examples

* `Simple`:
	where().key("name").operator(eq).value("Jeff Nelson")

* `Multiple Values`:
	where().key("age").operator(bw).value(20).value(30)

* `Conjunctive`:
	where().key("name").operator(eq).value("Jeff Nelson").and().key("age").operator(neq).value(30)

* `Disjunctive`:
	where().key("name").operator(eq).value("Jeff Nelson").or().key("age").operator(neq).value(30)

## OPERATORS

Operators are used in `find` queries and `Criteria` statements to filter the result set. CaSH provides built-in shortcut variables to access the members of the `Operator` class.

   * `eq`:
     EQUALS

   * `neq`:
     NOT_EQUALS

   * `gt`:
     GREATER_THAN

   * `lt`:
     LESS_THAN

   * `gte`:
     GREATER_THAN_OR_EQUALS

   * `lte`:
     LESS_THAN_OR_EQUALS

   * `bw`:
     BETWEEN

   * `regex`:
     REGEX

   * `nregex`:
     NOT_REGEX

   * `lnk2`:
     LINKS_TO

## TIMESTAMP
Use the `date` or `time` function to convert a valid English expression or unix timestamp with microsecond precision to a `Timestamp` object.

### Example
verify(key, value, record, time("expression"))

### Recognized Expressions
	* 4:49
	* 4:49:30
	* 4:49:30.2
	* yesterday
	* yesterday {time}
	* last week
	* last month
	* last year
	* October 26, 1981 or Oct 26, 1981
	* October 26 or Oct 26
	* 26 October 1981
	* 26 Oct 1981
	* 26 Oct 81
	* 10/26/1981 or 10-26-1981
	* 10/26/81 or 10-26-81
	* 1981/10/26 or 1981-10-26
	* 10/26 or 10-26
	* {any explicit date} {time}

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/

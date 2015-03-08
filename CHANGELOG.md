## Changelog

#### Version 0.5.0 (TBD)

#### Inventory
* Added an inventory to keep track of which records have been created.
* Added logic to the `verify` methods to first check if a record exists and fail fast if possible.

##### CaSH
* Fixed a bug in CaSH where pressing `CTRL + C` at the command prompt would unexpectedly exit the shell instead of returning a new prompt.
* Added a feature to automatically preserve CaSH command history across sessions.
* Changed CaSH error exit status from `127` to `1`.
* Added `-r` and `--run` options to the `cash` CLI that allow the execution of commands inline without launching the entire CaSH application.
* Added a `whoami` variable to CaSH that displays the current Concourse user.
* Added support for multi-line input in CaSH. For example, you can now write complex routines that span several lines like:

		[default/cash]$ for(long record : find("attending", eq, "Y")){
		> count+= fetch("name", record).size();
		> println fetch("name", record);
		> }

##### Miscellaneous
* Added a `verifyOrSet` method to the API that atomically ensures that a value is the only one that exists for a key in a record without creating more revisions than necessary.
* Improved the performance of the `set` operation by over 25 percent.
* Added functionality to client and management CLIs to automatically use connnection information specified in a `concourse_client.prefs` file located in the user's home directory. This gives users the option to invoke CLIs without having to specify any connection based arguments.
* Added a `find(key, value)` method that is a shortcut to query for records where `key` equals `value`.
* Changed the method signature of the `close()` method so that it does not throw a checked `Exception`.
* Added percent sign (%) wild card functionality that matches one or more characters for REGEX and NOT_REGEX operators in find operations. The (%) wildcard is an alias for the traditional regex (\*) wildcard. For example `find("name", REGEX, "%Jeff%")` returns the same result as `find("name", REGEX, "*Jeff*")` which is all the records where the name key contains the substring "Jeff".
* Added support for specifying *operators* to `find` methods and the `Criteria` builder using string symbols. For example, the following method invocations are now identical:

		concourse.find("foo", Operator.EQUALS, "bar");
		concourse.find("foo", "=", "bar");
		concourse.find("foo", "eq", "bar");
		find("foo", eq, "bar"); // in CaSH

* Added methods to limit the `audit` of a record or a key/record to a specified range of time.

#### Version 0.4.4 (March 2, 2015)
* Fixed an issue where transactions and atomic operations unnecessarily performed pre-commit locking during read operations, which negatively impacted performance and violated the just-in-time locking protocol ([CON-198/CON-199](https://cinchapi.atlassian.net/browse/CON-199)).
* Added logic to prevent the Buffer from attempting a scan for historical data that is older than any data that is currently within the Buffer ([CON-197](https://cinchapi.atlassian.net/browse/CON-197)).
* Added *group sync*: an optimization that improves Transaction performance by durably fsyncing committed writes to the Buffer in bulk. Transactions still honor the durability guarantee by taking a full backup prior to acknowledging a successful commit ([CON-125](https://cinchapi.atlassian.net/browse/CON-125)).
* Improved the performance of releasing locks by moving garbage collection of unused locks to a background thread.
* Improved the performance for upgrading range locks and checking for range conflicts by using collections that shard and sort range tokens.
* Improved Transaction write performance by using local bloom filters to speed up `verifies`.
* Fixed a bug where storage engine methods that touched an entire record (e.g. `browse(record)` and `audit(record)`) or an entire key (`browse(key)`) were not properly locked which potentially made reads inconsistent ([CON-239](https://cinchapi.atlassian.net/browse/CON-239)).
* Fixed an issue where transactions unnecessarily performed double write validation which hurt performance ([CON-246](https://cinchapi.atlassian.net/browse/CON-246)).
* Fixed a major memory leak that occurred when transactions were aborted or failed prior to committing ([CON-248](https://cinchapi.atlassian.net/browse/CON-248)).
* Added logging to indicate if the background indexing job terminates because of an uncaught error ([CON-238](https://cinchapi.atlassian.net/browse/CON-238)).
* Fixed an issue where the background indexing job could be wrongfully terminated because it appeared to be stalled when doing a large amount of work.
* Fixed a memory-leak issue where Concourse Server did not release resources for abandoned transactions if the client started a transaction and eventually started another one without explicitly committing or aborting the previous one ([CON-217](https://cinchapi.atlassian.net/browse/CON-217)).
* Fixed various issues and performance bottlenecks with syncing storage blocks to disk.
* Improved the names of several Concourse Server threads.

#### Version 0.4.3 (February 1, 2015)
*In this release we made lots of internal optimizations to further build on the performance improvements in versions 0.4.1 and 0.4.2. Many of them are small, but a few of the larger ones are highlighted below. In total, our efforts have produced additional speed improvements of 53 percent for queries, 80 percent for range queries, 65 percent for writes and 83 perecent for background indexing.*

* Added auto adjustable rate indexing where the throughput of the background indexing job will increase or decrease inversely with query load to prevent contention.
* Lowered the threshold for Java to start compiling server methods to native code.
* Implemented priority locks that ensure readers and writers always take precedence over the background indexing job when there is contention.
* Increased internal caching of some frequently used objects to reduce the overhead for initialization and garbage collection.
* Switched to using StampedLocks with optimistic reads in some places to reduce the overhead of accessing certain resources with little or no contention.
* Eliminated unnecessary intermediate copies of data in memory when serializing to disk.
* Switched to a faster hash function to generate lock tokens.
* Switched from using the default `ConcurrentHashMap` implementation to one backported from Java 8 for better performance.
* Improved the efficiency of the background indexing job by re-using worker threads.
* Improved heuristics to determine bloom filter sizing.
* Where appropriate, added some bloom filters that are less precise but have faster lookup times.
* Switched to using soft references for revisions in recently synced data blocks so that they avoid disk i/o unless absolutely necessary due to memory pressure.
* Added a more compact representation for revisions in memory to reduce bloat.
* Made miscellaneous optimizations for sensible performance gains.
* Upgraded the Tanuki wrapper to version 3.5.26 to fix an issue where Concourse Server on OS X Yosemite (10.10) systems mistakenly tried to start using 32-bit native libraries.
* Added an `envtool` CLI that can be used to manage environments in Concourse Server.
* Added a `--list-sessions` action to the `useradmin` CLI to list all the currently active user session in Concourse Server.
* Removed unnecessary locking that occurred when performing writes in a transaction or atomic operation.

#### Version 0.4.2 (October 4, 2014)
* Improved the way that the storage engine processes `find` queries, resulting in a further speed improvement of over 35 percent.
* Fixed a bug with real-time transaction failure detection that made it possible for [phantom reads](http://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads) to occur.
* Fixed an issue that caused Concourse Server to drop transaction tokens when under increased concurrency.
* Fixed a bug in the just-in-time locking protocol that prematurely removed references to active locks.
* Fixed a bug where transactions that started to commit but failed before completing did not release locks, resulting in deadlocks.
* Fixed an issue where transactions unnecessarily grabbed locks twice while committing.
* Fixed an issues that made it possible for deadlocks to occur with many concurrent Transactions performing atomic operations (i.e. `set`).
* Improved the javadoc for the `Tag` datatype.
* Fixed a bug where the `Tag#toString` method threw a `NullPointerException` if the Tag was created using a `null` value.
* Add a `min` method to the `Numbers` utility class.
* Fixed a bug that caused the `insert` methods to incorrectly store values encoded as *resolvable link specifications* as strings instead of links to resolved records.
* Added a `heap_size` preference in `concourse.prefs` that configures the initial and max heap for the Concourse Server JVM.

#### Version 0.3.8 (October 4, 2014)
* Fixed a bug where database records and indexes were not properly cached. Now, reads are over 87 percent faster.
* Removed a potential race-condition between real-time failure detection and just-in-time locking that made it possible for an failed transaction to errneously commit and violate ACID consistency.
* Fixed a bug where the `Numbers#max` method actually returned the minimum value.

#### Version 0.4.1 (September 13, 2014)
* Reduced the number of primary record lookups required to perform a `find` query which yields up to an order of magnitude in increased speed.
* Fixed a bug that accidentally stripped underscore *_* characters from environment names.
* Further improved the CPU efficiency of the background indexing processes.
* Fixed a bug that made it possible for Concourse Server to experience thread leaks.
* Fixed a bug that prevented backticks from being stripped in JSON encoded Tag values.
* Added cached and fixed `ConnecitionPool` factory methods that use the default connection info when creating new instances.
* Fixed a bug that caused some management CLIs to unnecssarily prompt for authentication instead of immediately displaying the `usage` message when an insufficent number of arguments were presented.
* Fixed a bug that caused the Criteria builder to improperly handle values with leading and trailing backticks.
* Made `Concourse` implement the `java.lang.AutoCloseable` interface.
* Fixed an issue where upgrades failed because the system version was not set for new installations.
* Fixed bugs that made it possible for atomic operations started from a Transaction to spin in an infinite loop if the Transaction failed prior to being committed.
* Added a `TransactionException` with a clear error message that is thrown when (staged) operations in a Transaction fail prior to being committed because of a data change.

#### Version 0.3.7 (September 13, 2014)
* Fixed an issue that caused Concourse Server to unnecessarily keep file descriptors open after data was indexed and synced to disk.
* Fixed an issue that made it possible for Concourse to lose some storage metadata in the event of a premature server crash or power loss.
* Improved CaSH by removing the display of meaningless performance logging when a user merely presses the `enter` key at the prompt.

#### Version 0.4.0 (June 30, 2014)

##### Environments
* Added support for multiple environments, which allows users to store data for different purposes (i.e. staging vs production) separately while managing them with the same Concourse Server. Users are automatically connected to a configurable `default_environment` (concourse.prefs) if none is specified at login. Alternatively, users can connect to or dynamically create a new environment by
	* using the new `Concourse#connect(host, port, username, password, environment)` or `Concourse#connect(environment)` login methods,
	* adding `environment = <name>` to the `concourse_client.prefs` file and using the `Concourse#connect()` or ``Concourse#connect(host, port, username, password)` login methods, or
	* specifying an environment name using the `-e` flag when launching CaSH like:

			$ ./cash -e production

* Added support for specifying environments using the `-e` flag to applicable server-side management CLIs (i.e. `dumptool`) and the `import` CLI.
* Added support for specifying environments with the `ConnectionPool` API.
* Improved the CaSH prompt to display the current environment like:

		production/cash$

##### API

* Added a `Criteria` building feature that allows users to programatically create complex queries with multiple clauses and groups. This is particularly helpful when programming in an IDE that offers code completion.
* Added a method to the `Convert` utility class to transform a JSON formatted string into a multimapping of keys to appropriate Java primitives.
* Added new core API methods:
	* `browse` returns a complete view of all the data presently or historically associated with a either a *record* or a *key*.
	* `chronologize` returns a chronological series of all the values for a *key* in a *record* over time.
	* `clear` now has an option to atomically remove all the data contained in an entire record.
	* `find` now has an option to process a complex `Criteria` using a single network call.
	* `insert` writes serveral key/value mappings from a JSON encoded string into one or more records with a single network call.
* Added `LINKS_TO` Operator (aliased as `lnk2` in CaSH) to make it easy to include links in find criteria. For example, the following statements are equivalent:

		concourse.find(\"foo\", Operator.LINKS_TO, 1);
		concourse.find(\"foo\", Operator.EQUALS, Links.to(1));
* Added a new `Tag` datatype for the purpose of storing a string value without performing full text search indexing. A `Tag` can be created programatically using the `Tag#create` method and in CaSH using the `tag()` alias.

##### Usability
* Improved the usability of the `useradmin` CLI and deprecated the `--grant` and `--revoke` options.
* Added requirement that new passwords be 3 or more characters long.
* Improved the `dumptool` CLI to list dumpable storage units by default if no `-i` or `--id` argument is specified. As a result the `--list` flag is now deprecated since it is unnecessary.
* Added logic to terminate a CaSH session if a relevant security change occurs.
* Improved readability of log files by removing redundant information from log messages.
* Added the optional installation of the `concourse` and `cash` scripts to the `$PATH` via `/usr/local/bin` during installation or upgrade so that they can be invoked from any location.
* Added the optional symlinking of the server log files to `/var/log/concourse` during installation or upgrade.

##### Bug Fixes
* Fixed an issue that prevented strings from being sorted in a case insensitive manner.
* Fixed a bug that causes some historical queries to return incorrect results.

##### Miscellaneous
* Added a framework to securely migrate stored data to new formats when upgrading Concourse Server.
* Improved the CPU efficiency of the background indexing process.
* Changed the startup script to use `.concourse.conf` instead of `concourse.conf` for configuration.
* Updated CaSH documentation.

#### Version 0.3.6 (June 30, 2014)
* Fixed a bug that caused string values to be sorted inconsitently.
* Fixed an infinite loop that caused Concourse Server to stack overflow when used with JRE 8.
* Fixed an issue where the stock `concourse.prefs` documentation referred to the default `buffer_page_size` as 8MB when its actually 8KB.
* Changed the daemon Concourse Server process name to display as `ConcourseServer` instead of `WrapperSimpleApp`.
* Updated the `concourse-config` dependency to version 1.0.5 which fixes and issue that caused passwords to be incorrecctly read from `concourse_client.prefs` files.

#### Version 0.3.5 (May 26, 2014)
* Added support for using short syntax in nested commands in CaSH. For example, the following commands are equivalanet and can now be used interchanably:

		cash$ get(describe(1), find(\"name\", eq, 1))
		cash$ concourse.get(concourse.describe(1), concourse.find(\"name\", eq, 1))

* Fixed a bug that caused a deadlock when committing a transaction that wrote a value to a key and then subsequently performed a query against the key that included the value directly.
* Fixed a bug that made it possible for the server to hang after reaching an inconsistent state caused by the Buffer expanding to accommodate new data written by one client while simultaneously servicing a read request for another client.
* Fixed a bug that prvented the server from starting after an unexpected shutdown corrupted an uncommited transaction.
* Fixed a bug that caused the database to appear to lose data if the `database_directory` preference was specified using a relative path.
* Fixed a bug that made it possible for the server to accidentally reindex data when starting up after an unexpected shutdown.
* Added checks to detect and warn about the existence of duplicate data that can safely be deleted without affecting data consistency.
* Improved memory management by using soft references and just-in-time metadata retrieval.
* Added logic to detect and repair stalled background index jobs.
* Fixed an issue that caused the server to unreliably lock resources under load.
* Fixed an bug that failed to prevent the addition of circular links.
* Improved CLI usability by displaying the username alongside the interactive password prompt and making it possible to display the help/usage text without authenticating.
* Added a CLI to import CSV files.
* Added logic to rollover and archive log files once they reach 10MB in size.

#### Version 0.3.4 (April 13, 2014)
* Added support for issuing commands in CaSH using short syntax. Short syntax allows the user to make Concourse API calls by invoking the desired method directly by name instead of prepending the invocation with `concourse.`. For example, the following commands are all equivalent and can now be used interchangably in stand-alone statements:

		cash$ add(\"name\", \"jeff\", 1)
		cash$ concourse.add(\"name\", \"jeff\", 1)
		cash$ add \"name\", \"jeff\", 1
		cash$ concourse.add \"name\", \"jeff\", 1

* Improved the `toString()` output of `Timestamp` objects so that they match the following format: `Thu Apr 03, 2014 @ 1:32:42:54 PM PDT`.
* Fixed an issue that caused the server to incorrectly lock resources when processing lots of concurrent reads/writes to a record or key in record.
* Fixed an issue that caused the server to deadlock if an error occured while indexing data in the background.
* Fixed an issue where the installer would launch a separate X11 window when configuring the `concourse-server` directory in some environments.

#### Version 0.3.3 (March 25, 2014)
* Upgraded Tanuki service wrapper to version 3.5.24 which fixes an issue that prevented the server from starting in OSX Mavericks.
* Consolidated service wrapper native libraries in `wrapper` directory within the root of the concourse-server installation.
* Added support for 32-bit Linux and OSX systems.
* Added `--list` and `-l` flags to the `dumptool` CLI to display a list of dumpable storage units.
* Fixed a bug that caused some searches to return false-positive results.
* Fixed a bug that caused mishandling of data containing leading or trailing whitespaces.
* Fixed a bug that made it possible to see inconsistent search results if a query was issued while the engine was indexing relavent data in the background.
* Fixed a bug that caused a deadlock when committing a transaction that performed a range query against a key and then subsequently added that key to a record as a value within the range.
* Made server-side `jmx_port` configurable in concourse.prefs.

#### Version 0.3.2 (March 16, 2014)
* Added support for creating a cached connection pool that continues to establish new connections on demand, but will use previously created ones when possible.
* Deprecated the `ConnectionPool#newConnectionPool` factory methods in favour of more descriptive ones.
* Added a method to the `Convert` utility class to transform a raw string value to the appropriate java primitive.
* Added a method to the `Convert` utility class to transform a raw string value to a resolvable link specification that instructs the receiver to add a link in a record to all the records that map a certain key to that value.
* Made server-side `client_port` and `shutdown_port` parameters configurable in concourse.prefs.
* Added check on server startup to ensure that the `buffer_directory` and `database_directory` parameters are not identical.

#### Version 0.3.1 (March 9, 2014)
* Added the ability to have multiple concurrent connections for a single user.
* Added support for connection pooling to the client API.
* Removed unused `transaction_directory` key from concourse.prefs.
* Fixed an issue that allowed the storage of blank string keys and values.
* Fixed an issue that prevented the client from properly processing compound `#get` operations that tried to retrieve data for a key in a record that did not contain any values.
* Improved the info logging for transactions by only using a unique id to refer to each transaction.
* Slighly increased full text indexing speed.
* Improved CaSH documentation.

#### Version 0.3.0 (February 1, 2014)
* Changed install and upgrade distributions from zip file to self-extracting binary.
* Added logic to upgrade from previous versions.
* Added server-side atomic operation and transaction protocols.
* Added Transaction support to the API.
* Added new `#verifyAndSwap()` atomic operation to the API
* Changed `#set()`, `#clear()`, and `#revert()` API methods to to be atomic.
* Added password based authentication and access token based session handling to server.
* Added `useradmin` CLI to add/edit/delete user access.
* Added several compound operations to API.
* Fixed bug that prevented server from starting on non-OSX systems.
* Made historical `#find()` methods consistent with other historical operations by specifying the timestamp last.
* Added Timestamp wrapper class that is interoperable with Joda DateTime, but has microsecond precision.
* Added requirement for authentication when using management CLIs.
* Fixed bug that allowed access to private variables in CaSH.
* Improved CLI error messages.
* Added API method to get server release version.
* Improved background data indexing protocol.
* Made artifact versioning more consistent.
* Added server side range locking protocol for #find() queries.
* Bug fixes.
* Improved documentation.
* Improved error messages.
* Improved build infrastructure.

#### Version 0.2.0 (December 28, 2013)
* Changed database storage model from one record per file to several revisions across blocks.
* Added CLI to dump buffer and block contents.
* Added Concourse Action SHeLL (CaSH)
* Added publishing of artifacts to maven central repo.
* Improved logging for thrift internal errors.
* Improved search performance.
* Removed two way link/unlink methods from API.
* Fixed bug where result set ordering did not persist from server to client.
* Decorated toString for return value of `#audit()` methods.
* Added shortcut `start` and `stop` server scripts.
* Added JMX support.
* Improved documentation.
* Bug fixes.

#### Version 0.1.0 (October 18, 2013)
* Hello World.

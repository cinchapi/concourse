## Changelog

#### Version 0.10.4 (TBD)
* Added support for using the `LIKE`, `NOT_LIKE` and `LINKS_TO` operators in the `TObject#is` methods. 
* Fixed a bug that made it possible for a `ConnectionPool` to refuse to accept the `release` of a previously issued `Concourse` connection due to a race condition.
* Fixed a bug that made it possible for Concourse to violate ACID consistency when performing a concurrent write to a key/record alongside a wide read in the same record.

#### Version 0.10.3 (November 12, 2019)
* Fixed an issue where the `Database` unnecessarily loaded data from disk when performing a read for a `key` in a `record` after a previous read for the entire `record` made the desired data available in memory.
* Fixed a minor bug that caused the Database to create unnecessary temporary directories when performing a reindex.
* The `Criteria` builder now creates a `NavigationKeySymbol` for navigation keys instead of a `KeySymbol`.
* Fixed a bug that caused `Convert#stringToJava` to throw an `NumberFormatException` when trying to convert numeric strings that appeared to be numbers outside of Java's representation range. As a result of this fix, those kinds of values will remain as strings.
* Added a `ForwardingConcourse` wrapper that can be extended by subclasses that provide additional functionality around a subset of Concourse methods.
* Fixed a bug that prevent a custom `ConnectionPool` using a custom `Concourse` instance (e.g. one that extends `ForwardingConcourse`) from returning a connection of the correct class. As a result of this change, the `ConnectionPool` constructors that accept explicit Concourse connection parameters have been deprecated in favor of one that takes a `Supplier` of Concourse connections.
* Fixed a bug that caused `TObject#compareTo` to return logically inconsistent results relative to `TObject#equals`. Previously, comparing `TObjects` with type `STRING` occurred in a case insensitive manner whereas the `equals` evaluation was case sensitive. Now, the `compareTo` method is case sensitive.
* Added the ability to compare `TObjects` in a case insensitive manner.
* Fixed a bug that made it possible for storage engine to return inaccurate results for `REGEX` and `NOT_REGEX` queries if matching values had different case formats.
* Fixed a bug that caused historical queries to incorrectly return logically different results compared to present state queries if matching values had different case formats.
* Fixed a bug that made it possible for reads within the `Buffer` to cause write lock starvation and resource exhaustion; preventing any further writes from occurring and generating a backlog of reads that never terminated.

#### Version 0.10.2 (August 24, 2019)
* Fixed a bug that caused an error to be thrown when creating a `Criteria` containing a navigation key using the `Criteria#parse` factory.
* Added an option to limit the length of substrings that are indexed for fulltext search. It is rare to add functionality in a patch release, but this improvement was needed to alleviate Concourse deployments that experience `OutOfMemory` exceptions because abnormally large String values are stuck in the Buffer waiting to be indexed. This option is **turned off by default** to maintain consistency with existing Concourse expectations for fulltext indexing. The option can be enabled by specifying a positive integer value for the newly added `max_search_substring_length` preference. When a value is supplied, the Storage Engine won't index any substrings longer than the provided value for any word in a String value.
* Made internal improvements to the search indexing algorithm to reduce the number of itermediary objects created which should decrease the number of garbage collection cycles that are triggered.
* Fixed a bug that caused the Engine to fail to accumulate metadata stats for fulltext search indices. This did not have any data correctness or consistency side-effects, but had the potential to make some search-related operations inefficient.
* Fixed a bug that made it possible for the Database to appear to lose data when starting up after a crash or unexpected shutdown. This happened because the Database ignored data in Block files that erroneously appeared to be duplicates. We've fixed this issue by improving the logic and workflow that the Database uses to test whether Block files contain duplicate data.
* Fixed a regression introduced in version `0.10.0` that made it possible for the Database to ignore errors that occurred when indexing writes. When indexing errors occur, the Database creates log entries and stops the indexing process, which is the behaviour that existed prior to version `0.10.0`.
* Fixed a bug that made it possible for fatal indexing errors to occur when at least two writes were written to the same `Buffer` page for the same key with values that have different types (i.e. `Float` vs `Integer`) but are *essentially* equal (i.e. `18.0` vs `18`). In accordance with Concourse's weak typing system, values that are *essentially* the same will be properly indexed and queryable across associated types. This change **requires a reindex of all block files which is automatically done when upgrading from a previous version**.
* Fixed a bug that causes the `ManagedConcourseServer` in the `concourse-ete-test-core` framework to take longer than necessary to allow connections in `ClientServerTest` cases.

#### Version 0.10.1 (August 6, 2019)
* Fixed a regression that caused an error when attempting an action with a CCL statement containing an unquoted string value with one or more periods.

#### Version 0.10.0 (August 3, 2019)

##### BREAKING CHANGES
There is only **PARTIAL COMPATIBILITY** between 
* an `0.10.0+` client and an older server, and 
* a `0.10.0+` server and an older client.

Due to changes in Concourse's internal APIs,
* Older client will receive an error when trying to invoke any navigate or calculation methods on a `0.10.0+` server.
* Older servers will throw an error message when any navigate or calculation methods are invoked from an `0.10.0+` client. 

##### New Features

###### Sorting
Concourse Server now (finally) has the ability to sort results!
* A result set that returns data from multiple records (across any number of keys) can be sorted.
* Concourse drivers now feature an `Order` framework that allows the client to specify how Concourse Server should sort a result set. An `Order` can be generated using an intuitive builder chain.
* An Order can be specified using values for any keys; regardless of whether those keys are explictly fetched or not.
* A timestamp can optionally be associated with each order component.
	* **NOTE**: If no timestamp is specified with an order component, Concourse Server uses the value that is explicitly fetched in the request OR it looks up the value using the selection timestamp of the request. If there is no selection timestamp, the lookup returns the current value(s).
* A direction (ascending or descending) can optionally be associated with each order component.
	* **NOTE**: The default direction is *ascending*.
* An Order can contain an unlimited number of components. If, after evaluating all the order components, two records still have the same sort order, Concourse Server automatically sorts based on the record id.
* If a `null` value is encountered when evaluating an order components, Concourse Server pushes the record containing that `null` value to the "end" of the result set regardless of the order component's direction.

###### Pagination
Concourse Server now (finally) has the ability to page through results!
* A result set that returns data from multiple records (across any number of keys) can be paginated.
* Concourse drviers now feature a `Page` object that allows the client to specify how Concourse Server should paginate a result set. A `Page` is an abstraction for offset/skip and limit parameters. The `Page` class contains various factory methods to offset and limit a result set using various intuitive notions of pagination.

###### Navigable Data Reads
* You can now traverse the document graph by specifying one ore more *navigation keys* in the following read methods:
	* browse
	* calculate
	* get
	* select
* Reading a navigation key using `get` or `select` is intended to repleace the `navigate` methods.
* When reading a navigation key in the context of one or more records, the root record (e.g the record from which the document-graph traversal starts) is mapped to the values that are retrieved from the destination records. In the `navgiate` methods, the destination record is associated with the destination value(s).
	* For example, assume record `1` is linked to record `2` on the `friends` key. Record `2` contains the value `Jeff` for the `name` key...
	* if you `select("friends.name", 1)`, the return value will map `1` to `[Jeff]` whereas the return value of `navigate("friends.name", 1)` maps `2` to `[Jeff]`. 
	
###### Navigable Criteria
* You can now use navigation keys in `Criteria` objects or `ccl` statements that are passed to the `find`, `get` and `select` methods.

###### ETL 
* Added the `com.cinchapi.concourse.etl` package that contains data processing utilities:
	*  A `Strainer` can be used to process a `Map<String, Object>` using Concourse's data model rules. In particular, the `Strainer` encapsulates logic to break down top-level sequence values and process their elements individually.
	* The `Transform` class contains functions for common data transformations. 

###### Miscellaneous
* Added an iterative connection builder that is accessible using the `Concourse.at()` static factory method.
* Added the `com.cinchapi.concourse.valididate.Keys` utility class which contains the `#isWritable` method that determines if a proposed key can be written to Concourse.
* Added `Parsers#create` static factory methods that accept a `Criteria` object as a parameter. These new methods compliment existing ones which take a CCL `String` and `TCriteria` object respectively.
* Upgraded the `ccl` dependency to the latest version, which adds support for local criteria evaluation using the `Parser#evaluate` method. The parsers returned from the `Parsers#create` factories all support local evaluation using the function defined in the newly created `Operators#evaluate` utility.
* Added support for remote debugging of Concourse Server. Remote debugging can be enabled by specifying a `remote_debugger_port` in `concourse.prefs`.
* The `calculate` methods now throw an `UnsupportedOperationException` instead of a vague `RuntimeException` when trying to run calculations that aren't permitted (i.e. running an aggregation on a non-numeric index).

##### Improvements
* Refactored the `concourse-import` framework to take advantage of version `1.1.0+` of the `data-transform-api` which has a more flexible notion of data transformations. As a result of this change, the `Importables` utility class has been removed. Custom importers that extend `DelimitedLineImporter` can leverage the protected `parseObject` and `importLines` methods to hook into the extraction and import logic in a manner similar to what was possible using the `Importables` functions.
* Refactored the `Criteria` class into an interface that is implemented by any language symbols that can be immediately transformed to a well-built criteria (e.g. `ValueState` and `TimestampState`). The primary benefit of this change is that methods that took a generic Object parameter and checked whether that object could be built into a `Criteria` have now been removed from the `Concourse` driver since that logic is automatically captured within the new class hiearchy. Another positive side effect of this change is that it is no longer necessary to explicitly build a nested `Criteria` when using the `group` functionality of the `Criteria` builder.
* Improved the performance of querying certain Concourse Server methods from a plugin via `ConcourseRuntime` by eliminating unnecessary server-side calculations meant to facilitate analytics. These calculations will continue to be computed when the plugin receives the data from `ConcourseRuntime`.

##### Bug Fixes
* Fixed a bug that caused data imported from STDIN to not have a `__datasource` tag, even if the `--annotate-data-source` flag was included with the CLI invocation. 
* Fixed a bug that allowed Concourse Server to start an environment's storage engine in a partially or wholly unreadable state if the Engine partially completed a block sync while Concourse Server was going through its shutdown routine. In this scenario, the partially written block is malformed and should not be processed by the Engine since the data contained in the malformed block is still contained in the Buffer. While the malformed block files can be safely deleted, the implemented fix causes the Engine to simply ignore them if they are encountered upon initialization. 
* Added checks to ensure that a storage Engine cannot transport writes from the Buffer to the Database while Concourse Server is shutting down.
* Fixed a bug that allow methods annotated as `PluginRestricted` to be invoked if those methods were defined in an ancestor class or interface of the invokved plugin.
* Fixed a bug introduced by [JEF-245](https://openjdk.java.net/jeps/245) that caused Concourse Server to fail to start on recent versions of Java 8 and Java 9+ due to an exploitation of a JVM bug that was used to allow Concourse Server to specify native thread prioritization when started by a non-root user. As a result of this fix, Concourse Server will only try to specify native thread prioritization when started by a root user.

##### Deprecated and Removed Features
* Removed the `Strings` utility class in favor of `AnyStrings` from `accent4j`.
* Removed the `StringSplitter` framework in favor of the same from `accent4j`.
* Deprecated `Criteria#getCclString` in favor of `Criteria#ccl`.
* The `navigate` methods in the client drivers have been deprecated in favor of using `select`/`get` to traverse the document-graph.

#### Version 0.9.6 (February 16, 2019)
* Fixed a bug that caused a `ParseException` to be thrown when trying to use a `Criteria` object containing a string value wrapped in single or double quotes out of necessity (i.e. because the value contained a keyword). This bug happened because the wrapping quotes were dropped by Concourse Server when parsing the `Criteria`.
* Fixed a bug where the CCL parser failed to handle some Unicode quote characters.

#### Version 0.9.5 (December 30, 2018)
* Fixed a bug where some of the `ManagedConcourseServer#get` methods in the `concourse-ete-test-core` package called the wrong upstream method of the Concourse Server instance under management. This had the effect of causing a runtime `ClassCastException` when trying to use those methods.
* Fixed a bug that caused ambiguity and erroneous dispatching for query methods (e.g. #get, #navigate and #select) that have different signatures containing a `long` (record) or generic `Object` (criteria) parameter in the same position. This caused issues when a `Long` was provided to the method that expected a `long` because the dispatcher would route the request that expected an `Object` (criteria) parameter.
* Fixed a bug in the `concourse-ete-test-core` framework where `Timestamp` objects returned from the managed server were not translated to `Timestamp` type from the test application's classpath.

#### Version 0.9.4 (October 31, 2018)
* Context has been added exceptions thrown from the `v2` `ccl` parser which makes it easier to identify what statements are causing issues.

#### Version 0.9.3 (October 7, 2018)
* Fixed a bug that caused a `NullPointerException` to be thrown when trying to `set` configuration data in `.prefs` files.

#### Version 0.9.2 (September 3, 2018)
* Deprecated the `forGenericObject`, `forCollection`, `forMap` and `forTObject` TypeAdapter generators in the `TypeAdapters` utility class in favor of `primitiveTypesFactor`, `collectionFactory` and `tObjectFactory` in the same class, each of which return a `TypeAdapterFactory` instead of a `TypeAdapter`. Going forward, please register these type adapter factories when building a `Gson` instance for correct Concourse-style JSON serialization semantics.
* Upgraded to `ccl` version `2.4.1` to capture fix for an bug that caused both the v1 and v2 parsers to mishandle numeric String and Tag values. These values were treated as numbers instead of their actual type. This made it possible for queries containing those values to return inaccurate results.
* Added the associated key to the error message of the Exception that is thrown when attempting to store a blank value.
* Fixed a regression that caused programmatic configuration updates to no longer persist to the underlying file.
* The `Timestamp` data type now implements the `Comparable` interface.

#### Version 0.9.1 (August 12, 2018)

##### Enhancements
* Upgraded client and server configuration management and added support for incremental configuration overrides. Now
	* Server preferences defined in `conf/concourse.prefs` can be individually overriden in a `conf/concourse.prefs.dev` file (previously override preferences in the .dev file had to be done all or nothing). 
	* Both server and client preferences can be individually overriden using environment variables that are capitalized and prefixed with `CONCOURSE_`. For example, you can override the `heap_size` preference with an environment variable named `CONCOURSE_HEAP_SIZE`.
* Added Docker image [https://hub.docker.com/r/cinchapi/concourse](https://hub.docker.com/r/cinchapi/concourse). See [https://docs.cinchapi.com/concourse/quickstart](https://docs.cinchapi.com/concourse/quickstart) for more information.

##### Bug Fixes
* Fixed a bug that caused Concourse to improperly interpret values written in scientific notation as a `STRING` data type instead of the appropriate number data type.
* Fixed a bug in the logic that Concourse Server used to determine if the system's data directories were inconsistent. Because of the bug, it was possible to put Concourse in an inconsistent state from which it could not automatically recover by changing either the `buffer_directory` or `database_directory` and restarting Concourse Server. The logic has been fixed, so Concourse now accurately determines when the data directories are inconsistent and prevents the system from starting.
* Fixed a bug that caused an exception to be thrown when trying to `jsonify` a dataset that contained a `Timestamp`.

#### Version 0.9.0 (May 30, 2018)

##### Vulnerabilities
* Fixed a vulnerability that made it possible for a malicious plugin archive that contained entry names with path traversal elements to execute arbitrary code on the filesystem, if installed. This vulnerability, which was first disclosed by the [Snyk Security Research Team](https://snyk.io/docs/security), existed because Concourse did not verify that an entry, potentially extracted from a zipfile, would exist within the target directory if actually extracted. We've fixed this vulnerability by switching to the [zt-zip](https://github.com/zeroturnaround/zt-zip) library for internal zip handling. In addition to having protections against this vulnerability, `zt-zip` is battle-tested and well maintained by [ZeroTurnaround](https://zeroturnaround.com/). Thanks again to the Snyk Security Research Team for disclosing this vulnerability.

##### Security Model
* Added a notion of *user roles*. Each user account can either have the `ADMIN` or `USER` role. `ADMIN` users are permitted to invoke management functions whereas accounts with the `USER` role are not.
	* All previously existing users are assigned the `ADMIN` role on upgrade. You can change a user's role using the `users` CLI.
	* The `users create` command now requires a role to be provided interactively when prompted or non-interactively using the `--set-role` parameter.
* Added an `edit` option to the `users` CLI that allows for setting a user's role and/or changing the password. The password can also still be changed using the `password` option of the `users` CLI.
* Removed a constraint the prevented the default `admin` user account from being deleted.
* Added additional logging around the upgrade process.
* Fixed a bug that prevented upgrade tasks from being run when upgrading a Concourse Server instance that was never started prior to the upgrade.
* Upgraded some internal libraries to help make server startup time faster.
* Fixed a bug in `concourse-driver-java` that caused the `navigate` functions to report errors incorrectly.
* Added *user permissions*. Each non-admin user account can be granted permission to `READ` or `WRITE` data within a specific environment:
	* Permissions can be granted and revoked for a non-admin role user by a user who has the admin role.
	* Permissions are granted on a per environment basis.
	* A user with `READ` permission can read data from an environment but cannot write data.
	* A user with `WRITE` permission can read and write data in an environment.
	* Users with the admin role implicitly have `WRITE` permission to every environment.
	* If a user's role is downgraded from admin to user, she will have the permissions she has before being assigned the admin role.
	* If a user attempts to invoke a function for which she doesn't have permission, a `PermissionException` will be thrown, but the user's session will not terminate.
	* A user with the admin role cannot have any of her permissions revoked.
	* Plugins automatically inherit a user's access (based on role and permission).
	* Service users that operate on behalf of plugins have `WRITE` access to every environment.

##### Data Types
* Added a `Criteria#at(Timestamp)` method to transform any `Criteria` object into one that has all clauses pinned to a specific `Timestamp`.
* Added a static `Criteria#parse(String)` method to parse a CCL statement and produce an analogous `Criteria` object.
* Streamlined the logic for server-side atomic operations to unlock higher performance potential.
* Added [short-circuit evaluation](https://en.wikipedia.org/wiki/Short-circuit_evaluation) logic to the query parsing pipeline to improve performance.
* Added a `TIMESTAMP` data type which makes it possible to store temporal values in Concourse.
	* The `concourse-driver-java` API uses the [`Timestamp`](https://docs.cinchapi.com/concourse/api/java/com/cinchapi/concourse/Timestamp.html) class to represent `TIMESTAMP` values. Please note that hallow `Timestamps` (e.g. those created using the `Timestamp#fromString` method cannot be stored as values). An attempt to do so will throw an `UnsupportedOperationException`.
	* The `concourse-driver-php` uses the [`DateTime`](http://php.net/manual/en/class.datetime.php) class to represent `TIMESTAMP` values.
	* The `concourse-driver-python` uses the [`datetime`](https://docs.python.org/2/library/datetime.html) class to represent `TIMESTAMP` values.
	* The `concourse-driver-ruby` uses the [`DateTime`](https://ruby-doc.org/stdlib-2.3.1/libdoc/date/rdoc/DateTime.html) class to represent `TIMESTAMP` values.
	* The Concourse REST API allows specifying `TIMESTAMP` values as strings by prepending and appending a `|` to the value (e.g. `|December 30, 1987|`). It is also possible to specify a formatting pattern after the value like `|December 30, 1987|MMM dd, yyyy|`.
* Added a `Timestamp#isDateOnly` method that returns `true` if a `Timestamp` does not contain a relevant temporal component (e.g. the `Timestamp` was created from a date string instead of a datetime string or a timestring).

##### Performance
* Upgraded the CCL parser to a newer and more efficient version. This change will yield general performance improvements in methods that parse CCL statements during evaluation.

##### Developer Experience
* The test Concourse instance used in a `ClientServerTest` will no longer be automatically deleted when the test fails. This will allow for manual inspection of the instance when debugging the test failure.
* Added additional logging for plugin errors.
* Added a `manage` interface to the driver APIs. This interface exposes a limited number of management methods that can be invoked programatically.

##### Bug Fixes
* Fixed a bug that caused the server to fail to start if the `conf/stopwords.txt` configuration file did not exist.
* Fixed a bug that caused `PrettyLinkedHashMap#toString` to render improperly if data was added using the `putAll` method.
* Fixed a bug in the `ConcourseImportDryRun#dump` method that caused the method to return an invalid JSON string.
* Fixed a bug where a users whose access had been `disabled` was automatically re-enabled if her password was changed.

##### Miscellaneous
* Added the ability for the storage engine to track stats and metadata about database structures.

#### Version 0.8.2 (April 17, 2018)
* Fixed a bug in the `ManagedConcourseServer#install` method that caused the server installation to randomly fail due to race conditions. This caused unit tests that extended the `concourse-ete-test-core` framework to intermittently fail.

#### Version 0.8.1 (March 26, 2018)
* Fixed a bug that caused local CCL resolution to not work in the `findOrInsert` methods.
* Fixed an issue that caused conversion from string to `Operator` to be case sensitive.
* Fixed a bug that caused the `putAll` method in the map returned from `TrackingMultimap#invert` to store data inconsistently.
* Added better error handling for cases when an attempt is made to read with a value with a type that is not available in the client's version.
* Fixed a bug that caused Concourse Server to unreliably stream data when multiple real-time plugins were installed.
* Fixed a bug that caused Concourse Server to frequently cause high CPU usage when multiple real-time plugins were installed.
* Added an **isolation** feature to the `ImportDryRunConcourse` client (from the `concourse-import` framework). This feature allows the client to import data into an isolated store instead of one shared among all instances. This functionality is not exposed to the `import` CLI (because it isn't necessary), but can be benefical to applications that use the dry-run client to programmatically preview how data will be imported into Concourse.
* Added an implementation for the `ImportDryRunConcourse#describe` method.

#### Version 0.8.0 (December 14, 2017)
* Added a `count` aggregation function that returns the number of values stored
	* across a key,
	* for a key in a record, or
	* for a key in multiple records.
* Added a `max` aggregation function that returns the largest numeric value stored
	* across a key,
	* for a key in a record, or
	* for a key in multiple records.
* Added a `min` aggregation function that returns the smallest numeric value stored
	* across a key,
	* for a key in a record, or
	* for a key in multiple records.
* Moved the `ccl` parsing logic into a [separate library](https://github.com/cinchapi/ccl) to make the process portable to plugins and other applications.
* Fixed some bugs that could have caused incorrect evaluation of `select(criteria)`, `find(criteria)` and related methods in some cases.
* Added a `TObject#is(operator, values...)` method so plugins can perform local operator based comparisons for values returned from the server.

#### Version 0.7.3 (December 14, 2017)
* Fixed a bug that caused the temporal `average` and `sum` calculations to fail if the `timestamp` parameter was generated from a `String` instead of `long`.
* Fixed a couple of bugs that made it possible for Concourse Server to pass blank or unsanitized environment names to plugins during method invocations.
* Fixed a bug that caused `Criteria` objects to be improperly serialized/deserialized when passed to plugin methods as arguments or used as return values.

#### Version 0.7.2 (November 26, 2017)
* Added more detailed information to the server and plugin log files about plugin errors.
* Fixed a bug where `TrackingMultimap#percentKeyDataType` returned `NaN` instead of `0` when the map was empty.
* Added a `memoryStorage` option to the `PluginStateContainer` class.

#### Version 0.7.1 (November 22, 2017)
* Fixed a bug that caused an error in some cases of importing or inserting data that contained a value of `-`.
* Added better error message for TApplicationException in CaSH.

#### Version 0.7.0 (November 19, 2017)
* Added `navigate` methods that allow selecting data based on link traversal. For example, it is possible to select the names of the friends of record 1's friends by doing

		navigate "friends.friends.name", 1

* Re-implemented the `users` CLI to provide extensible commands. Now the `users` CLI will respond to:
	1. `create` - create a new user
	2. `delete` - delete an existing user
	3. `enable` - restore access to a suspended user
	4. `password` - change a user's password
	5. `sessions` - list the current user sessions
	6. `suspend` - revoke access for a user

* Changed the `envtool` CLI to the `environments` CLI with extensible commands. The `environments` CLI will respond:
	1. `list` - list the Concourse Server environments

* Changed the `dumptool` CLI to the `data` CLI with extensible commands. The `data` CLI will respond to:
	1. `dump` - dump the contents of a Concourse Server data file
	2. `list` - list the Concourse Server data files

* Added a `CompositeTransformer` to the `concourse-import` framework that invokes multiple transformers in declaration order.
* Added a `Transformers` utility class to the `concourse-import` framework API.
* Fixed a bug that caused the loss of order in plugin results that contained a sorted map.
* Added a `--dry-run` flag to the `import` CLI that will perform a test import of data in-memory and print a JSON dump of what data would be inserted into Concourse.
* Added support for installing multiple plugins in the same directory using the `concourse plugin install </path/to/directory>` command.
* Implemented `describe()` and `describe(time)` methods to return all the keys across all records in the database.
* Fixed a bug where the `browse(keys, timestamp)` functionality would return data from the present state instead of the historical snapshot.
* Fixed an issue that caused plugins to use excessive CPU resources when watching the liveliness of the host Concourse Server process.
* Added a bug fix that prevents service tokens from auto-expiring.
* Added a `ps` command to the `plugins` CLI to display information about the running plugins.
* Fixed a bug that caused the `average(key)` method to return the incorrect result.
* Fixed a bug that caused calculations that internally performed division to prematurely round and produce in-precise results.
* Fixed a bug that caused the editing and deleting an existing user with the `users` CLI to always fail.
* Added support for defining custom importers in `.jar` files.
* Detect when the service is installed in an invalid directory and fail appropriately.
* Fixed a security bug that allowed the `invokePlugin` method to not enforce access controls properly.
* Fixed a bug that caused management CLIs to appear to fail when they actually succeeded.
* Improved the performance of the `ResultDataSet#put` method.
* Fixed a bug in the implementation of `ObjectResultDataset#count`.
* Deprecated `Numbers#isEqual` and `Numbers#isEqualCastSafe` in favor of better names `Numbers#areEqual` and `Numbers#areEqualCastSafe`.
* Added support for getting the min and max keys from a `TrackingMultimap`.
* Added an `ImmutableTrackingMultimap` class.
* Fixed a bug in the `TrackingMultimap#delete` method.
* Fixed the CPU efficiency of the JavaApp host termination watcher.
* Fix bug that caused JavaApp processes to hang if they ended before the host was terminated.
* Added database-wide `describe` method.

#### Version 0.6.0 (March 5, 2017)
* Added `calculate` interface to the `java` driver to perform aggregations.
* Added a `sum` aggregation function.
* Added an `average` aggregation function.
* Switched to socket-based (instead of shared memory based) interprocess communication between Concourse Server and plugins.
* Assigned meaningful process names to plugins.
* Added a System-Id for each Concourse Server instance.
* Fixed bugs in the `ObjectResultDataset` implementation.
* Added an end-to-end testing framework for the plugin framework.
* Fixed a bug that caused some query results to be case-sensitive.
* Fixed a bug that caused some query results to have inconsistent ordering.
* Upgraded support for parsing natural language timestamps.
* Updated the usage method of the `concourse` init.d script.
* Fixed a bug that caused `PluginContext` and `PluginRuntime` to return different directories for a plugin's data store.
* Added a progress bar for the `plugin install` command.
* Fixed a bug that caused `ConcourseRuntime` to mishandle plugin results.
* Clarified the proper way to use plugin hooks.
* Refactored the `plugin` management CLI.
* Fixed a bug that allowed plugins to invoke server-side transaction methods ([CON-518](http://jira.cinchapi.com/browse/CON-518)).
* Refactored the implementation of the `version` CLI.
* Improved process forking framework.
* Enabled console logging for plugins ([CON-514](http://jira.cinchapi.com/browse/CON-514)).
* Made the `Transformer` interface in `concourse-import` framework a `FunctionalInterface`.
* Added logic to plugins to signal to Concourse Server when initialization has completed.
* Added functionality to get the host Concourse Server directory from the `import` CLI and server-side management CLIs.
* Added support for defining custom importers in an `importers` directory within the Concourse Server instance directory.
* Added a `--annotate-data-source` option to the `import` CLI that will cause imported records to have the name of the source file added to the `__datasource` key.
* Added support for specifying the id of the record into which data should be inserted within the JSON blob that is passed to the `insert` method.
* Added method to `TrackingMultimap` that measures the spread/dispersion of the contained data.
* Fixed a race condition bug in the `concourse-ete-test` framework.
* Fixed bug that caused a preference for using random ports outside the ephemeral range.
* Changed the plugin configuration to no longer require setting `remote_debugger = on` to enable remote debugging; now it is sufficient to just specify the `remote_debugger_port` preference.

#### Version 0.5.0 (November 3, 2016)

##### API Breaks
* The `insert(json)` method now returns a `Set<Long>` instead of a `long`.
* The `fetch` methods have been renamed `select`.
* The `get` methods now return the most recently added value that exists instead of the oldest existing value.
* Compound operations have been refactored as batch operations, which are now implemented server-side (meaning only 1 TCP round trip per operation) and have atomic guarantees.
* Changed package names from `org.cinchapi.concourse.*` to `com.cinchapi.concourse.*`

##### API Additions
* Added support for the Concourse Criteria Language (CCL) which allows you to specify complex find/select criteria using structured language.
* Added an `inventory()` method that returns all the records that have ever had data.
* Added `select` methods that return the values for a one or more keys in all the records that match a criteria.
* Added a `verifyOrSet` method to the API that atomically ensures that a value is the only one that exists for a key in a record without creating more revisions than necessary.
* Added `jsonify` methods that return data from records as a JSON string dump.
* Added `diff` methods that return the changes that have occurred in a record or key/record within a range of time.
* Added a `find(key, value)` method that is a shortcut to query for records where `key` equals `value`.
* Changed the method signature of the `close()` method so that it does not throw a checked `Exception`.
* Added percent sign (%) wild card functionality that matches one or more characters for REGEX and NOT_REGEX operators in find operations. The (%) wildcard is an alias for the traditional regex (\*) wildcard. For example `find("name", REGEX, "%Jeff%")` returns the same result as `find("name", REGEX, "*Jeff*")` which is all the records where the name key contains the substring "Jeff".
* Added support for specifying *operators* to `find` methods and the `Criteria` builder using string symbols. For example, the following method invocations are now identical:

		concourse.find("foo", Operator.EQUALS, "bar");
		concourse.find("foo", "=", "bar");
		concourse.find("foo", "eq", "bar");
		find("foo", eq, "bar"); // in CaSH

* Added methods to limit the `audit` of a record or a key/record to a specified range of time.
* Added atomic operations to add/insert data if there are no existing records that match the data or a specific criteria.
* Deprecated `Convert#stringToResolvableLinkSpecification(String, String)` in the Java Driver in favor of `Convert#stringToResolvableLinkInstruction(String)`.
* Added logic to handle using arbitrary CCL strings for resolvable links when inserting or importing data.

##### Client Drivers
* Added a native Python client driver
* Added a native PHP client driver
* Added a native Ruby client driver
* Added REST API functionality to Concourse Server that can be enabled by specifying the `http_port` in concourse.prefs.

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

* Added the ability to request help information about specific functions in CaSH using the `help <function>` command.
* Display performance logging using seconds instead of milliseconds.
* Added functionality to pre-seed the CaSH environment with an external **run commands** groovy script using the `--run-commands <script>` or `--rc <script>` flag. Any script that is located in `~/.cashrc` is automatically loaded. There is also an option to disable loading any run commands script using the `--no-run-commands` or `--no-rc` flags.
* Added the ability to exclude parenthesis when invoke methods that don't take any arguments. For example, the following method invocations are identical:

		[default/cash] getServerVersion
		[default/cash] getServerVersion()

* Added `show records` command which will display all the records in Concourse that have data.

##### CLIs
* Added support for invoking server-side scripts via the `concourse` CLI. So, if the `concourse` CLI is added to the $PATH, it is possible to access the server scripts from any location. For example, you can access the import CLI like:

		$ concourse import -d /path/to/data

* Added functionality to client and management CLIs to automatically use connnection information specified in a `concourse_client.prefs` file located in the user's home directory. This gives users the option to invoke CLIs without having to specify any connection based arguments.
* Added `--version` option to get information about the Concourse Server version using the `concourse` CLI.
* Added option to perform a heap dump of a running Concourse Server instance to the `concourse` CLI.
* Added an `uninstall` script/option to the `concourse` CLI that safely removes the application data for a Concourse Server instance, while preserving data and logs.
* Added support for performing interactive imports using the `import` CLI.
* Added support for importing input piped from the output of another command with the `import` CLI.
* Added an `upgrade` action that checks for newer versions of Concourse Server and automatically upgrades, if possible.

##### Performance
* Improved the performance of the `set` operation by over 25 percent.
* Added logic to the `verify` methods to first check if a record exists and fail fast if possible.
* Optimized the way in which reads that query the present state delegate to code paths that expect a historical timestamp ([CON-268](https://cinchapi.atlassian.net/browse/CON-268)).
* Removed unnecessary locking when adding or reading data from a block index ([CON-256](https://cinchapi.atlassian.net/browse/CON-256)).
* Improved efficiency of string splitting that occurs during indexing and searching.

##### Configuration
* Added functionality to automatically choose a `shutdown_port` based on the specified `client_port`.
* Added logic to automatically calculate the `heap_size` preference based on the amount of system memory if a value isn't explicitly given in `concourse.prefs`.
* Added option to skip system-wide integration when installing Concourse Server. The syntax is

	```bash
	$ sh concourse-server.bin -- skip-integration
	```

##### Miscellaneous
* Changed from the MIT License to the Apache License, Version 2.0.
* Replaced the StringToTime library with Natty.
* Replaced the Tanuki Java Service Wrapper library with a custom implementation.
* Added text coloring to the output of various CLIs.
* Added logic to check if Concourse Server is uninstalled incorrectly.

##### Bug Fixes
* Fixed a bug that caused transactions to prematurely fail if an embedded atomic operation didn't succeed ([CON-263](https://cinchapi.atlassian.net/browse/CON-263)).
* Java Driver: Fixed an issue in the where the client would throw an Exception if a call was made to the `commit()` method when a transaction was not in progress. Now the client will simply return `false` in this case.
* Fixed an issue that caused the `concourse` and `cash` scripts to fail when added to the $PATH on certain Debian systems that did not have `sh` installed.
* Fixed an issue where using a future timestamp during a "historical" read in an atomoc operation allowed the phantom read phenomenon to occur ([CON-259](https://cinchapi.atlassian.net/browse/CON-259)).
* Fixed an issue that caused client connections to crash when inserting invalid JSON ([CON-279](https://cinchapi.atlassian.net/browse/CON-279)).
* Fixed a bug that caused the storage engine to erroneously omit valid results for a query if the query contained a clause looking for a numerical value with a different type than that which was stored ([CON-326](http://jira.cinchapi.com/browse/CON-326)).
* Fixed an issue that prevented the `import` CLI from properly handling relative paths. ([CON-172](https://cinchapi.atlassian.net/browse/CON-172)).
* Fixed a bug where the upgrade framework initializer ran during the installation process which almost always resulted in a scenario where the framework set the system version in the wrong storage directories. ([GH-86](https://github.com/cinchapi/concourse/issues/86))

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

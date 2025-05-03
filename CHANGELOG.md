#### Version 0.11.10 (TBD)
* Fixed a bug that prevented Concourse Server from properly starting if a String configuration value was used for a variable that does not expect a String (e.g., `max_search_substring_length = "40"`). Now, Concouse Server will correctly parse all configuration values to the appropriate type or use the default value if there is an error when parsing.
* **Enhanced Memory Management for Large Term Indexing** - Fixed a critical issue where Concourse Server would crash when indexing large search terms on systems with disabled memory swapping (such as *Container-Optimized OS* on *Google Kubernetes Engine*). Previously, when encountering large search terms to index, Concourse Server would always attempt to use off-heap memory to preserve heap space for other operations. If memory pressure occurred during processing, Concourse would detect this signal from the OS and fall back to a file-based approach. However, on swap-disabled systems like Container-Optimized OS, instead of receiving a graceful memory pressure signal, Concourse would be immediately `OOMKilled` before any fallback mechanism could activate. With this update, Concourse Server now proactively estimates required memory before attempting off-heap processing. If sufficient memory is available, it proceeds with the original approach (complete with file-based fallback capability). But, if insufficient memory is detected upfront, Concourse immediately employs a more rudimentary processing mechanism that requires no additional memory, preventing OOMKill scenarios while maintaining indexing functionality in memory-constrained environments.

#### Version 0.11.9 (April 30, 2025)
* Improved the performance of select operations that specify selection keys and require both sorting and pagination. The improvements are achieved from smarter heuristics that determine the most efficient execution path. The system now intelligently decides whether to:
  * Sort all records first and then select only the paginated subset of data, or
  * Select all data first and then apply sorting and pagination, or
	* If the `Order` clause only uses a single key, use the index of that order key to lookup the sorted order of values and filtering out those that are not in the desired record set.
  
  This decision is based on a cost model that compares the number of lookups required for each approach, considering factors such as the number of keys being selected, the number of records being processed, the pagination limit, and whether the sort keys overlap with the selected keys. This optimization significantly reduces the number of database lookups required for queries that combine sorting and pagination, particularly when working with large datasets where only a small page of results is needed.
* Improved multi-key selection commands that occur during concurrent Buffer transport operations. Previously, when selecting multiple keys while data was being transported for indexing, each primitive select operation would acquire a lock that blocked transports, but transports could still occur between operations, slowing down the overall read. Now, these commands use an advisory lock that blocks all transports until the entire bulk read completes. This optimization significantly improves performance in real-world scenarios with simultaneous reads and writes. In the future, this advisory locking mechanism will be extended to other bulk and atomic operations.
* Improved the performance of multi-key selection commands that occur during concurrent Buffer transport operations. Previously, when selecting multiple keys while data was being transported for indexing, each primitive select operation would acquire a lock that blocked transports, but transports could still occur between operations, slowing down the overall read. Now, these commands use an advisory lock that blocks all transports until the entire bulk read completes. This optimization significantly improves performance in real-world scenarios with simultaneous reads and writes. In the future, this advisory locking mechanism will be extended to other bulk and atomic operations.
* Implemented a caching mechanism for parsed keys to eliminate repeated validation and processing throughout the system. Previously, keys (especially navigation keys) were repeatedly validated against the WRITABLE_KEY regex and tokenized for every operation, even when the same keys were used multiple times. This optimization significantly improves performance for all operations that involve key validation and navigation key processing, with particularly noticeable benefits in scenarios that use the same keys repeatedly across multiple records.
* Eliminated unnecessary re-sorting of Database data that's already in sorted order. Previously, both the Buffer and Atomic Operation/Transaction queues would force a re-sort on all initial read context that was pulled from the Database (even when that data was already sorted because the Database retrieved it from an index or intentionally sorted it before returning). Now, those stores always apply their Writes to the context under the assumption that the context is sorted and will maintain sorter order, which greatly cuts down on the overhead of intermediate processing for read operations.
* Improved the `concourse data repair` CLI to detect and fix "imbalanced" data--an invalid state caused by a bug or error in Concourse Server's enforcement that any write about a topic must be a net-new ADD for that topic or must offset a previously inserted write for that topic (e.g., a REMOVE can only exist if there was a prior ADD and vice versa). When an unoffset write bypasses this constraint, some read operations will break entirely. The enhanced CLI now goes beyond only fixing duplicate transaction applications and now properly balances data in place, preserving intended state and fully restoring read functionality without any downtime.
* Implemented auto-healing for `ConnectionPool`s where failed connections are detected and automatically replaced when they are released back to the pool.

 an issue where the Database is unable process reads because the data mistakenly became imbalanced. Data is balanced when all writes about a particular topic are properly offset. If an error or bug ever causes an unoffset write to be inserted, it can cause reads to break. Now, the `repair` CLI can detect and recover from this issue without downtime.

#### Version 0.11.8 (April 15, 2025)

##### Navigation Queries
* **Optimized Navigation Key Traversal**: To minimize the number of lookups required when querying, we've implemented a smarter traversal strategy for navigation keys used in a Criteria/Condition. The system now automatically chooses between:
  * **Forward Traversal**: Starting from the beginning of the path and following links forward (traditional approach)
  * **Reverse Traversal**: Starting from records matching the final condition and working backwards

  For example, a query like `identity.credential.email = foo@foo.com` likely returns few records, so reverse traversal is more efficient - first finding records where `email = foo@foo.com` and then tracing backwards through the document graph. Conversely, a query like `identity.credential.numLogins > 5` that likely matches many records is better handled with forward traversal, starting with records containing links from the `identity` key and following the path forward.
  
  This optimization significantly improves performance for navigation queries where the initial key in the path has high cardinality (many unique values across records), but the final condition is more selective (e.g., a specific email address).

##### Caching
* **Dynamic Memory-Aware Eviction**: Record caches in the database now evict entires based on overall memory consumption instead of evicting after exceeding a static number of entries. Caches can grow up to an internally defined proportion of the defined `heap_size` and will be purged once this limit is exceeded or when system memory pressure necessitates garbage collection.
* **Enhanced Diagnostic Logging**: For improved observability, DEBUG logging now emits messages whenever a cached record is evicted, allowing for more effective monitoring and troubleshooting of cache behavior.

#### Version 0.11.7 (April 7, 2025)
* Fixed a bug that made it possible to leak filesystem resources by opening duplicate file descriptors for the same Segment file. At scale, this could prematurely lead to "too many open files" errors.
* [GH-534](https://github.com/cinchapi/concourse/issues/534): Fixed a bug that caused the `CONCOURSE_HEAP_SIZE` environment variable, if set, not to be read on server startup.

#### Version 0.11.6 (July 6, 2024)
* Added new configuration options for initializing Concourse Server with custom admin credentials upon first run. These options enhance security by allowing a non-default usernames and passwords before starting the server.
	* The `init_root_username` option in `concourse.prefs` can be used to specify the username for the initial administrator account.
	* The `init_root_password` option in `concourse.prefs` can be used to specify the password for the initial administrator account
* Exposed the default JMX port, `9010`, in the `Dockerfile`.
* Fixed a bug that kept HELP documentation from being packaged with Concourse Shell and prevented it from being displayed.
* Added a fallback option to display Concourse Shell HELP documentation in contexts when the `less` command isn't available (e.g., IDEs).
* Fixed a bug that caused Concourse Server to unnecessarily add error logging whenever a client disconnected.
* Added the ability to create `ConnectionPool`s that copy the credentials and connection information from an existing handler These copying connection pools can be created by using the respective "cached" or "fixed" factory methods in the `ConnectionPool` class that take a `Concourse` parameter.

#### Version 0.11.5 (November 5, 2022)
* Fixed a bug that made it possible for a Transaction to silently fail and cause a deadlock when multiple distinct writes committed in other operations caused that Transaction to become preempted (e.g., unable to continue or successfully commit because of a version change).
* Fixed a bug that allowed a Transaction's atomic operations (e.g., `verifyAndSwap`) to ignore range conflicts stemming from writes committed in other operations. As a result, the atomic operation would successfully commit to its a Transaction, but the Transaction would inevitably fail due to the aforementioned conflict. The correct (and now current) behaviour is that the atomic operation fails (so it can be retried) without dooming the entire Transaction to failure.
* Fixed a bug that caused an innocuous Exception to be thrown when importing CSV data using the interactive input feature of `concourse import` CLI.
* Fixed a bug that caused an issue with using the `ConcourseServerDownloader` to download installers from Github.

#### Version 0.11.4 (July 4, 2022)
* Slightly improved the performance of result sorting by removing unnecessary intermediate data gathering.
* Improved random access performance for all result sets.

#### Version 0.11.3 (June 4, 2022)
* Improved the performance of commands that select multiple keys from a record by adding herustics to the storage engine to reduce the number of overall lookups required. As a result, commands that select multiple keys are **up to 96% faster**.
* Streamlined the logic for reads that have a combination of `time`, `order` and `page` parameters by adding more intelligent heuristics for determining the most efficient code path. For example, a read that only has `time` and `page` parameters (e.g., no `order`) does not need to be performed atomically. Previously, those reads converged into an atomic code path, but now a separate code path exists so those reads can be more performant. Additionally, the logic is more aware of when attempts to sort or paginate data don't actually have an effect and now avoids unnecessary data transformations of re-collection.
* Fixed a bug that caused Concourse Server to not use the `Strategy` framework to determine the most efficient lookup source (e.g., field, record, or index) for navigation keys.
* Added support for querying on the intrinsic `identifier` of Records, as both a selection and evaluation key. The record identifier can be refernced using the `$id$` key (NOTE: this must be properly escaped in `concourse shell` as `\$id\$`). 
	* It is useful to include the Record identifier as a selection key for some navigation reads (e.g., `select(["partner.name", partner.\$id\$], 1)`)).
	* It is useful to include the Record identifier as an evaluation key in cases where you want to explictly exclude a record from matching a `Condition` (e.g., `select(["partner.name", parner.\$id\$], "\$id\$ != 2")`))
* Fixed a bug that caused historical reads with sorting to not be performed atomically; potentially violating ACID semantics.
* Fixed a bug that caused commands to `find` data matching a `Condition` (e.g., `Criteria` or `CCL Statement`) to not be fully performed atomically; potentially violating ACID semantics.

#### Version 0.11.2 (March 18, 2022)
* Fixed a bug that caused Concourse Server to incorrectly detect when an attempt was made to atomically commit multiple Writes that toggle the state of a field (e.g. `ADD name as jeff in 1`, `REMOVE name as jeff in 1`, `ADD name as jeff in 1`) in user-defined `transactions`. As a result of this bug, all field toggling Writes were committed instead of the desired behaviour where there was a commit of at most one equal Write that was required to obtain the intended field state. Committing multiple writes that toggled the field state within the same transaction could cause failures, unexplained results or fatal inconsistencies when reconciling data.
* Added a fallback to automatically switch to reading data from Segment files using traditional IO in the event that Concourse Server ever exceedes the maximum number of open memory mapped files allowed (as specified by the `vm.max_map_count` property on some Linux systems).
* Removed the `DEBUG` logging (added in `0.11.1`) that provides details on the execution path chosen for each lookup because it is too noisy and drastically degrades performance.
* Fixed a bug in the way that Concourse Server determined if duplicate data existed in the v3 storage files, which caused the `concourse data repair` CLI to no longer work properly (compared to how it worked on the v2 storage files).
* Fixed a regression that caused a memory leak when data values were read from disk. The nature of the memory leak caused a degradation in performance because Concourse Server was forced to evict cached records from memory more frequently than in previous versions.

#### Version 0.11.1 (March 9, 2022)
* Upgraded to CCL version `3.1.2` to fix a regression that caused parenthetical expressions within a Condition containing `LIKE` `REGEX`, `NOT_LIKE` and `NOT_REGEX` operators to mistakenly throw a `SyntaxException` when being parsed.
* Added the `ConcourseCompiler#evaluate(ConditionTree, Multimap)` method that uses the `Operators#evaluate` static method to perform local evaluation.
* Fixed a bug that, in some cases, caused the wrong default environment to be used when invoking server-side data CLIs (e.g., `concourse data <action>`). When a data CLI was invoked without specifying the environment using the `-e <environment>` flag, the `default` environment was always used instead of the `default_environment` that was specified in the Concourse Server configuration.
* Fixed a bug that caused the `concourse data compact` CLI to inexplicably die when invoked while `enable_compaction` was set to `false` in the Concourse Server configuration.
* Fixed the usage message description of the `concourse export` and `concourse import` CLIs.
* Fixed a bug that caused Concourse Shell to fail to parse short syntax within statements containing an open parenthesis as described in [GH-463](https://github.com/cinchapi/concourse/issues/463) and [GH-139](https://github.com/cinchapi/concourse/issues/139).
* Fixed a bug that caused the `Strategy` framework to select the wrong execution path when looking up historical values for order keys. This caused a regression in the performance for relevant commands.
* Added `DEBUG` logging that provides details on the execution path chosen for each lookup.
* Fixed a bug that caused `Order`/`Sort` instructions that contain multiple clauses referencing the same key to drop all but the last clause for that key.
* Fixed a bug that caused the `concourse export` CLI to not process some combinations of command line arguments properly.
* Fixed a bug tha caused an error to be thrown when using the `max` or `min` function over an entire index as an operation value in a CCL statement.
* Fixed several corner case bugs with Concourse's arithmetic engine that caused the `calculate` functions to 1) return inaccurate results when aggregating numbers of different types and 2) inexplicably throw an error when a calculation was performed on data containing `null` values.

#### Version 0.11.0 (March 4, 2022)

##### BREAKING CHANGES
There is only **PARTIAL COMPATIBILITY** between 
* an `0.11.0+` client and an older server, and 
* a `0.11.0+` server and an older client.

Due to changes in Concourse's internal APIs,
* An older client will receive an error when trying to invoke any `audit` methods on a `0.11.0+` server.
* An older server will throw an error message when any `audit` or `review` methods are invoked from an `0.11.0+` client. 

##### Storage Format Version 3
* This version introduces a new, more concise storage format where Database files are now stored as **Segments** instead of Blocks. In a segment file (`.seg`), all views of indexed data (primary, secondary, and search) are stored in the same file whereas a separate block file (`.blk`) was used to store each view of data in the v2 storage format. The process of transporting writes from the `Buffer` to the `Database` remains unchanged. When a Buffer page is fully transported, its data is durably synced in a new Segment file on disk.
* The v3 storage format should reduce the number of data file corruptions because there are fewer moving parts.
* An upgrade task has been added to automatically copy data to the v3 storage format.
	* The upgrade task will not delete v2 data files, so be mindful that **you will need twice the amount of data space available on disk to upgrade**. You can safely manually delete the v2 files after the upgrade. If the v2 files remain, a future version of Concourse may automatically delete them for you.
* In addition to improved data integrity, the v3 storage format brings performance improvements to all operations because of more efficient memory management and smarter usage of asynchronous work queues.

##### Atomic Commit Timestamps
All the writes in a committed `atomic operation` (e.g. anything from primitive atomics to user-defined `transactions`) will now have the **same** version/timestamp. Previously, when an atomic operation was committed, each write was assigned a distinct version. But, because each atomic write was applied as a distinct state change, it was possible to violate ACID semantics after the fact by performing a partial undo or partial historical read. Now, the version associated with each write is known as the **commit version**. For non-atomic operations, autocommit is in effect, so each write continues to have a distinct commit version. For atomic operations, the commit version is assigned when the operation is committed and assigned to each atomic write. As a result, all historical reads will either see all or see none of the committed atomic state and undo operations (e.g. `clear`, `revert`) will either affect all or affect none of the commited atomic state.

##### Optimizations
* The storage engine has been optimized to use less memory when indexing by de-duplicating and reusing equal data components. This drastically reduces the amount of time that the JVM must dedicate to Garbage Collection. Previously, when indexing, the storage engine would allocate new objects to represent data even if equal objects were already buffered in memory.
* We switched to a more compact in-memory representation of the `Inventory`, resulting in a reduction of its heap space usage by up to **97.9%**. This has an indirect benefit to overall performance and throughput by reducing memory contention that could lead to frequence JVM garbage collection cycles.
* Improved user-defined `transactions` by detecting when an attempt is made to atomically commit multiple Writes that toggle the state of a field (e.g. `ADD name as jeff in 1`, `REMOVE name as jeff in 1`, `ADD name as jeff in 1`) and only committing at most one equal Write that is required to obtain the intended state. For example, in the previous example, only 1 write for `ADD name as jeff in 1` would be committed.

##### Performance
* We improved the performance of commands that sort data by an average of **38.7%**. These performance improvements are the result of an new `Strategy` framework that allows Concourse Server to dynamically choose the most opitmal path for data lookups depending upon the entire context of the command and the state of storage engine. For example, when sorting a result set on `key1`, Concourse Server will now intelligently decide to lookup the values across `key1` using the relevant secondary index if `key1` is also a condition key. Alternatively, Concourse Server will decide to lookup the values across `key1` using the primary key for each impacted record if `key1` is also a being explicitly selected as part of the operation.
* Search is drastically faster as a result of the improved memory management that comes wth the v3 storage format as well as some other changes to the way that search indexes are read from disk and represented in memory. As a result, search performance is up-to **95.52%** faster on real-world data.

##### New Functionality
* Added `trace` functionality to atomically locate and return all the incoming links to one or more records. The incoming links are represented as a mapping from `key` to a `set of records` where the key is stored as a `Link` to the record being traced.
* Added `consolidate` functionality to atomically combine data from one or more records into another record. The records from which data is merged are cleared and all references to those cleared records are replaced with the consolidated record on the document-graph.
* Added the `concourse-export` framework which provides the `Exporter` construct for building tools that print data to an OutputStream in accordance with Concourse's multi-valued data format (e.g. a key mapped to multiple values will have those values printed as a delimited list). The `Exporters` utility class contains built-in exporters for exporting within CSV and Microsoft Excel formats.
* Added an `export` CLI that uses the `concourse-export` framework to export data from Concourse in CSV format to STDOUT or a file.
* For `CrossVersionTest`s, as an alternative to using the `Versions` annotation., added the ability to define test versions in a no-arg static method called `versions` that returns a `String[]`. Using the static method makes it possible to centrally define the desired test versions in a static variable that is shared across test classes.
* The `server` variable in a `ClientServerTest` (from the `concourse-ete-test-core` framework) now exposes the server configuration from the `prefs()` method to facilitate programatic configuration management within tests.
* Added the ability to configure the location of the access credentials file using the new `access_credentials_file` preference in `concourse.prefs`. This makes it possible to store credentials in a more secure directory that is also protected againist instances when the `concourse-server` installation directory is deleted. Please note that changing the value of `access_credentials_file` does not migrate existing credentials. By default, credentials are still stored in the `.access` within the root of the `concourse-server` installation directory.
* Added a separate log file for upgrade tasks (`log/upgrade.log`).
* Added a mechanism for failed upgrade tasks to automatically perform a rollback that'll reset the system state to be consistent with the state before the task was attempted.
* Added `PrettyLinkedHashMap.of` and `PrettyLinkedTableMap.of` factory methods that accept an analogous Map as a parameter. The input Map is lazily converted into one with a pretty `toString` format on-demand. In cases where a Map is not expected to be rendered as a String, but should be pretty if it is, these factories return a Map that defers the overhead of prettification until it is necessary. 

##### CCL Support
* Added support for specifying a CCL Function Statement as a selection/operation key, evaluation key (within a `Condition` or evaluation value (wthin a `Conditon`). A function statement can be provided as either the appropriate string form (e.g. `function(key)`, `function(key, ccl)`, `key | function`, etc) or the appropriate Java Object (e.g. `IndexFunction`, `KeyConditionFunction`, `ImplicitKeyRecordFunction`, etc). The default behaviour when reading is to interpret any string that looks like a function statement as a function statement. To perform a literal read of a string that appears to be a function statement, simply wrap the string in quotes. Finally, a function statement can never be written as a value.

##### Experimental Features

###### Compaction
* Concourse Server can now be configured to compact data files in an effort to optimize storage and improve read performance. When enabled, compaction automatically runs continuously in the background without disrupting data consistency or normal operations (although the impact on operational throughput has yet to be determined). The initial rollout of compaction is intentionally conservative (e.g. the built-in strategy will likely only make changes to a few data files). While this feature is experimental, there is no ability to tune it, but we plan to offer additional preferences to tailor the behaviour in future releases.
* Additionally, if enabled, performing compaction can be **suggested** to Concourse Server on an adhoc basis using the new `concourse data compact` CLI.
  * Compaction can be enabled by setting the `enable_compaction` preference to `true`. If this setting is `false`, Concourse Server will not perform compaction automatically or when suggested to do so.

###### Search Caching 
* Concouse Server can now be configured to cache search indexes. This feature is currently experimental and turned off by default. Enabling the search cache will further improve the performance of repeated searches by up to **200%**, but there is additional overhead that can slightly decrease the throughput of overall data indexing. Decreased indexing throughput may also indirectly affect write performance.
  * The search cache can be enabled by setting the `enable_search_cache` preference to `true`.

###### Verify by Lookup
* Concourse Server can now be configured to use special **lookup records** when performing a `verify` within the `Database`. In theory, the Database can respond to verifies faster when generating a lookup record because fewer irrelevant revisions are read from disk and processed in memory. However, lookup records are not cached, so repeated attempts to verify data in the same field (e.g. a counter whose value is stored against a single locator/key) or record may be slower.
  * Verify by Lookup can be enabled by setting the `enable_verify_by_lookup` preference to `true`.

##### API Breaks and Deprecations
* Upgraded to CCL version `3.1.1`. Internally, the database engine has switched to using a `Compiler` instead of a `Parser`. As a result, the Concourse-specific `Parser` has been deprecated.
* **It it only possible to upgrade to this version from Concourse `0.10.6+`. Previously, it was possible to upgrade to a new version of Concourse from any prior version.**
* Deprecated the `ByteBuffers` utility class in favor of the same in the `accent4j` library.
* Deprecated `PrettyLinkedHashMap.newPrettyLinkedHashMap` factory methods in favor of `PrettyLinkedHashMap.create`.
* Deprecated `PrettyLinkedHashMap.setKeyName` in favor of `PrettyLinkedHashMap.setKeyColumnHeader`
* Deprecated `PrettyLinkedHashMap.setValueName` in favor of `PrettyLinkedHashMap.setValueColumnHeader`
* Deprecated `PrettyLinkedTableMap.setRowName` in favor of `PrettyLinkedHashMap.setIdentifierColumnHeader`
* Deprecated `PrettyLinkedTableMap.newPrettyLinkedTableMap` factory methods in favor of `PrettyLinkedTableMap.create`
* Deprecated the `Concourse#audit` methods in favor of `Concourse#review` ones that take similar parameters. A `review` returns a `Map<Timestamp, List<String>>` instead of a `Map<Timestamp, String>` (as is the case with an `audit`) to account for the fact that a single commit timestamp/version may contain multiple changes.

##### Bug Fixes
* Fixed a bug that caused the system version to be set incorrectly when a newly installed instance of Concourse Server (e.g. not upgraded) utilized data directories containing data from an older system version. This bug caused some upgrade tasks to be skipped, placing the system in an unstable state.
* Fixed a bug that made it possible for Database operations to unexpectedly fail in the rare cases due to a locator mismatch resulting from faulty indexing logic.
* Fixed a bug in the serialization/deserialization logic for datasets passed between Concourse Server and plugins. This bug caused plugins to fail when performing operations that included non-trivial datasets.
* Fixed a bug that caused datasets returned from Concourse Server to a plugin to have incorrect missing data when `invert`ed.

#### Version 0.10.6 (September 9, 2021)

##### Removed limit on Block file sizes
* Added support for storing data in `Block` files that are larger than `2147483647` bytes (e.g. ~2.147GB) and fixed bugs that existed because of the previous limitation:
  * If a mutable `Block` exceeded the previous limit in memory it was not synced to disk and the storage engine didn't provide an error or warning, so indexing continued as normal. As a result, there was the potential for permanent data loss. 
  * When a mutable Block failed to sync in the manner described above, the data held in the Block remained completely in memory, resulting in a memory leak.
* To accommodate the possibility of larger Block files, the `BlockIndex` now records position pointers using 8 bytes instead of 4. As a result, all Block files must be reindexed, which is automatically done when Concourse Server starts are new installation or upgrade.

##### Eliminated risks of data inconsistency caused by premature shutdown
* Fixed the logic that prevents duplicate data indexing when Concourse Server prematurely shuts down or the background indexing job terminates because of an unexpected error. The logic was previously implemented to address [CON-83](https://cinchapi.atlassian.net/browse/CON-83), but it relied on data values instead of data versions and was therefore not robust enough to handle corner cases descried in [GH-441](https://github.com/cinchapi/concourse/issues/441) and [GH-442](https://github.com/cinchapi/concourse/issues/442).
  * A `concourse data repair` CLI has been added to detect and remediate data files that are corrupted because of the abovementioned bugs. The CLI can be run at anytime. If no corrupt data files are detected, the CLI has no effect.
  * Upon upgrading to this version, as a precuation, the CLIs routine is run for each environment.  

##### Other
* Fixed a bug that caused the default `log_level` to be `DEBUG` instead of `INFO`.
 
#### Version 0.10.5 (August 22, 2020)
* Fixed a bug where sorting on a navigation key that isn't fetched (e.g. using a navigation key in a `find` operation or not specifying the navigation key as an operation key in a `get` or `select` operation), causes the results set to be returned in the incorrect order.
* Upgraded CCL version to `2.6.3` in order to fix a parsing bug that occurred when creating a `Criteria` containing a String or String-like value with a whitespace or equal sign (e.g. `=`) character.
* Fixed a bug that made it possible to store circular links (e.g. a link from a record to itself) when atomically adding or setting data in multiple records at once.
* Fixed a race condition that occurred when multiple client connections logged into a non-default environment at the same time. The proper concurrency controls weren't in place, so the simultaneous connection attempts, in many cases, caused the Engine for that environment to be initialized multiple times. This did not cause any data duplication issues (because only one of the duplicate Engines would be recognized at any given time), but it could cause an `OutOfMemoryException` if that corresponding environment had a lot of metadata to be loaded into memory during intialization.

#### Version 0.10.4 (December 15, 2019)
* Added support for using the `LIKE`, `NOT_LIKE` and `LINKS_TO` operators in the `TObject#is` methods. 
* Fixed a bug that made it possible for a `ConnectionPool` to refuse to accept the `release` of a previously issued `Concourse` connection due to a race condition.
* Fixed a bug that made it possible for Concourse to violate ACID consistency when performing a concurrent write to a key/record alongside a wide read in the same record.
* Fixed a bug that caused inconsistencies in the intrinsic order of the result set records from a `find` operation vs a `select` or `get` operation.

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
* Changed database storage format from one record per file to several revisions across blocks (Storage Format Version 2).
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
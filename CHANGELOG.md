## Changelog

#### Version 0.5.0 (TBD)

#### Version 0.4.1 (TBD)
* Reduced the number of primary record lookups required to perform a `find` query which yields up to an order of magnitude in increased speed.

#### Version 0.3.7 (TBD)

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

		concourse.find("foo", Operator.LINKS_TO, 1);
		concourse.find("foo", Operator.EQUALS, Links.to(1));
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

		cash$ get(describe(1), find("name", eq, 1))
		cash$ concourse.get(concourse.describe(1), concourse.find("name", eq, 1))

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

		cash$ add("name", "jeff", 1)
		cash$ concourse.add("name", "jeff", 1)
		cash$ add "name", "jeff", 1
		cash$ concourse.add "name", "jeff", 1

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
## Changelog

#### Version 0.3.1 (March 9, 2014)
* Added the ability to have multiple concurrent connections for a single user.
* Added support for connection pooling to the client API.
* Removed unused `transaction_directory` key from concourse.prefs.
* Fixed an issue that allowed the storage of blank string keys and values.
* Fixed an issue that prevented the client from properly processing compound #get operations that attemped to retrieve data for a key in a record that did not contain any values.
* Slighly increased full text indexing speed.
* Improved CaSH documentation.

#### Version 0.3.0 (February 1, 2014)
* Fixed a bug that caused a TLinkedTableMap to display its header incorrectly.
* Added concurrenty controls to AccessManager `authorize()` method.
* Fixed a bug that caused an AtomicOperation to fail unnecessarily if it touched a key in a record but another operation touched a different key in the same record.

#### Version 0.3.0 RC2 (January 18, 2014)
* Fixed bug that occured when invoking `#verifyAndSwap()` on an existing swap value.
* Improved error message when client cannot connect to server.
* Additional unit and integration tests.
* Improved build infrastructure.
* Improved documentation.

#### Version 0.3.0 RC1 (January 5th, 2014)
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
* Improved documentation.
* Added Timestamp wrapper class that is interoperable with Joda DateTime, but has microsecond precision.
* Added requirement for authentication when using management CLIs.
* Fixed bug that allowed access to private variables in CaSH.
* Improved CLI error messages.
* Added API method to get server release version.
* Improved background data indexing protocol.
* Made artifact versioning more consistent.
* Added server side range locking protocol for `#find()` queries.
* Bug fixes.

#### Version 0.2.0
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
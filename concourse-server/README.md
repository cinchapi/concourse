# Concourse Server
The **concourse-server** project contains all of the logic for data storage and retrieval.

## Important Classes
### org.cinchapi.concourse.server.storage
This package contains the meat of the logic for storing and retrieving data.

#### Store
This is the interface that defines the **read** operations that are supported at the Engine level. These operations are considered primitive, so they must be fast and simple. Any operation that compounds multiple operations should never be defined here (they belong in ConcourseServer instead).

#### BufferedStore
This is an abstract implementation of the Store interface for situations where data must be initially stored in a buffer and eventually transported to a permanent destination. 

In addition to providing implementations for read operations, this class also defines primitive write operations (add/remove). This class is extended by Engine and AtomicOperation. 

__*This is generally the first place to search for root causes of correctness issues that happen when reading data.*__

#### Engine
The Engine is the primary coordinator within the system. It initially writes data to a Buffer and has a background thread that will continuously transport data to the Database. The Engine extends the methods and BufferedStore to globally (but granularly) lock records, fields, ranges, etc depending on the operation so that multiple threads can concurrently operate while maintaining data consistency.

The Engine is used as the destination for atomic operations and transactions. This allows ConcourseServer to create complex operations by compounding the primitive operations defined in the Store/Engine.

### org.cinchapi.concourse.server.storage.temp
This package contains tempoary data structures that are the initial home for data.

#### Limbo
Limbo is an implementation of Store that is considered lightweight because it does not do any indexing. Limbo is append-only, so writes are fast, but reads can be slow if there is a lot of data it does a linear scan for every read. Therefore, data is only ever temporary stored in Limbo and is transported to a Store that has suitable indexing as soon as possible.

#### Queue
This is a very simple implementation of Limbo that merely stores data in an ArrayList. This is used as the buffer for an AtomicOperation.

#### Buffer
The Buffer is a special implementation of Limbo that should only be used by the Engine. The Buffer immediately flushes data to disk for durability and stores data in multiple pages to increase read/write/transport throughput.

### org.cinchapi.concourse.server.storage.db
This package contains the components that handle the logic for efficiently storing indexed data.

#### Database
The Database handles the loading and caching of the underlying storage components. This class doesn't contain much interesting logic since it delegates most calls to a Block or Record.

#### Revision
A Revision is described by a locator, key and value and is used to encapsulate data that has been indexed. 

#### Block
A Block is a (possibly) sorted collection of Revisions. This class handles the logic for efficiently appending new Revisions to a mutable Block and efficiently seeking Revisions in a Block with the help of a BloomFilter and BlockIndex.

#### Record
A Record is a collection of Revisions that all have the same locator. If all the Revisions in the Record also have the same key, then it is a partial Record. 

**Record classes contain the logic for the ways in which the Store interface is handled by Database (i.e. the logic for the find() method is defined in the SecondaryRecord class)**

### org.cinchapi.concourse.server.model
This package contains immutable and serializable representation of data that is stored in Concourse.

#### Position
A Position associates the location of a text term with a PrimaryKey in a SearchRecord.

#### PrimaryKey
A PrimaryKey is the unique identifier and locator for a (Primary) Record.

#### Text
Text is a wrapper around a UTF-8 encoded string.

#### Value
A Value is a wrapper around a TObject (which is a wrapper around a java object) that handles weak type comparisons.

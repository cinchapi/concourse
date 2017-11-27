# Data Types

Concourse is a dynamically typed database which means that value types are intelligently inferred and comparisons can be made across value types.

Values in Concourse can be

## Primitive Types
### Boolean

### Double

### Float

### Integer

### Link
A `Link` is a pointer to another record. Links are used model relationship graphs within Concourse.

* Links can be created using the `link(key, destination, source)` method.
* Links can be [queried](queries) using the `LINKS_TO` operator.

### Long

### String

### Tag
A `Tag` is a String that is *not* indexed for full-text search.

### Timestamp
A `Timestamp` is a

## Advanced Types
### Dynamic Link

### Resolvable Link
A `resolvable link` is an instruction to create a link to *all* the records that match a criteria. Unlike dynamic links, a resolvable link is only evaluated once, at the time of write, so the linked records won't automatically change as the criteria's results do.

!!! warning "Do not add resolvable links directly"
    You cannot use the `add` methods to write resolvable links because the operation would not be atomic. You should only use resolvable links when writing data within a larger blob of information (e.g. a `Map` or JSON string being written using the `insert` method). In this case, you can add a resolvable link to the blob using the `Link.toWhere(criteria)` method.

# This file contains shared definitions that are unlikely to be modified
# post thrift generation.

# To generate java source code run:
# thrift -out concourse-driver-java/src/main/java -gen java thrift-api/shared.thrift
namespace java org.cinchapi.concourse.thrift

# To generate python source code run:
# thrift -out concourse-driver-python -gen py thrift-api/shared.thrift
namespace py concourse.thriftapi.shared

# To generate PHP source code run:
# thrift -out concourse-driver-php -gen php thrift-api/shared.thrift
namespace php thrift.shared

# To generate Ruby source code run:
# thrift -out concourse-driver-ruby/lib/thrift_api -gen rb thrift-api/shared.thrift

/**
 * Enumerates the list of operators that can be used in criteria specifications.
 */
enum Operator {
  REGEX = 1,
  NOT_REGEX = 2,
  EQUALS = 3,
  NOT_EQUALS = 4,
  GREATER_THAN = 5,
  GREATER_THAN_OR_EQUALS = 6,
  LESS_THAN = 7,
  LESS_THAN_OR_EQUALS = 8,
  BETWEEN = 9,
  LINKS_TO = 10,
  LIKE = 11,
  NOT_LIKE = 12
}

/**
 * Enumerates the possible TObject types
 */
enum Type {
  BOOLEAN = 1,
  DOUBLE = 2,
  FLOAT = 3,
  INTEGER = 4,
  LONG = 5,
  LINK = 6,
  STRING = 7,
  TAG = 8,
  NULL = 9,
}

enum Diff {
  ADDED = 1,
  REMOVED = 2,
}

/**
 * A temporary token that is returned by the
 * {@link ConcourseService#login(String, String)} method to grant access
 * to secure resources in place of raw credentials.
 */
struct AccessToken {
  1:required binary data
}

/**
 * A token that identifies a Transaction.
 */
struct TransactionToken {
  1:required AccessToken accessToken
  2:required i64 timestamp
}

/**
 * The security ex that occurs when the user session
 * is invalidated from Concourse server.
 */
exception TSecurityException {
  1: string message
}

/**
 * The exception that is thrown from the server when a
 * transaction related exception occurs.
 */
exception TTransactionException {}

/**
 * The exception that is thrown from the server when an
 * error occurs while parsing a string.
 */
exception TParseException {
  1: string message
}

/**
 * The exception that is thrown from the server when multiple matches exists
 * for a criteria when performing a write that simulates unique indexes
 * (e.g. findOrAdd, findOrInsert, etc)
 */
exception TDuplicateEntryException {
  1: string message
}

# This file contains shared definitions that are unlikely to be modified
# post thrift generation.

# To generate java source code run:
# thrift -out concourse-driver-java/src/main/java -gen java thrift-api/shared.thrift
namespace java com.cinchapi.concourse.thrift

# To generate python source code run:
# thrift -out concourse-driver-python -gen py thrift-api/shared.thrift
namespace py concourse.thriftapi.shared

# To generate PHP source code run:
# thrift -out concourse-driver-php/src -gen php thrift-api/shared.thrift
namespace php concourse.thrift.shared

# To generate Ruby source code run:
# thrift -out concourse-driver-ruby/lib/ -gen rb:namespaced thrift-api/shared.thrift
namespace rb concourse.thrift

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

/** When re-constructing the state of a record/field/index from some base state,
 * A {@link Diff} describes the {@link Action} necessary to perform using the
 * data from a {@link Write}.
 */
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

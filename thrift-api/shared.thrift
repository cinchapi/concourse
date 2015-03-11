# This file contains shared definitions that are unlikely to be modified
# post thrift generation.
#
# To generate java source code run:
# thrift -out ../../java -gen java shared.thrift

namespace java org.cinchapi.concourse.thrift

/**
 * Enumerates the list of operators that can be used in
 {@link ConcourseService#find(String, Operator, List<TObject>, long)}.
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

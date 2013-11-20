# This file defines the RPC protocol for Concourse client/server interaction
# using Thrift. The services and resources that are defined in this file are 
# not intended to be used by third parties, but they could be so they must meet
# high standards of quality.
# 
# To generate java source code run:
# thrift -out ../../java -gen java concourse.thrift 

namespace java org.cinchapi.concourse.thrift

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
	STRING = 7
}

/**
 * A lightweight wrapper for a typed Object that has been encoded 
 * as binary data.
 */	
struct TObject {
	1:required binary data,
	2:required Type type = Type.STRING
}

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
	BETWEEN = 9
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
 * The RPC protocol that forms the basis of cross-language client/server 
 * communication in Concourse. This is considered a public API; however
 * it is intended to be wrapped in a more expressive APIs in the target
 * implementation language.
 */
service ConcourseService {
	
	/**
	 * Login to the service. A user must login to receive an {@link AccessToken} 
	 * which is required for all other method invocations.
	 */
	AccessToken login(1: string username, 2: string password);
	
	/**
	 * Logout of the service and deauthorize {@code token}.
	 */
	void logout(1: AccessToken token);
	
	/**
	 * Turn on {@code staging} mode so that all subsequent changes are collected 
	 * in a staging area before possibly being committed to the database. Staged 
	 * operations are guaranteed to be reliable, all or nothing units of work that 
	 * allow correct  recovery from failures and provide isolation between clients 
	 * so the database is always in a consistent state.
	 * <p>
	 * After this method returns, all subsequent operations will be done in 
	 * {@code staging} mode until either {@link #abort(AccessToken)} or 
	 * {@link #commit(AccessToken)} is invoked.
	 * </p>
	 */
	TransactionToken stage(1: AccessToken token);
	
	/**
	 * Abort and remove any changes that are currently sitting in the staging area.
	 * <p>
	 * After this function returns, all subsequent operations will commit to the
	 * database immediately until {@link #stage(AccessToken)} is invoked.
	 * </p>
	 */
	void abort(1: AccessToken creds, 2: TransactionToken transaction);
	
	/**
	 * Attempt to permanently commit all the changes that are currently sitting in 
	 * the staging area to the database. This function only returns {@code true} 
	 * if all the changes can be successfully applied to the database. Otherwise, 
	 * this function returns {@code false} and all the changes are aborted. 
	 * <p>
	 * After this function returns, all subsequent operations will commit to the 
	 * database immediately until {@link #stage(AccessToken)} is invoked.
	 * </p>
	 */
	bool commit(1: AccessToken creds, 2: TransactionToken transaction);
		
	/**
	 * Add {@code key} as {@code value} to {@code record}. This method returns
	 * {@code true} if there is no mapping from {@code key} to {@code value}
	 * in {@code record} prior to invocation.
	 */
	bool add(1: string key, 2: TObject value, 3: i64 record, 4: AccessToken creds, 5: TransactionToken transaction);
	
	/**
	 * Remove {@code key} as {@code value} from {@code record}. This method returns
	 * {@code true} if there is a mapping from {@code key} to {@code value} in 
	 * {@code record} prior to invocation.
	 */
	bool remove(1: string key, 2: TObject value, 3: i64 record, 4: AccessToken creds, 5: TransactionToken transaction);
	
	/**
	 * Audit {@code record} or {@code key} in {@code record}. This method returns a 
	 * map from timestamp to a string describing the revision that occurred.
	 */
	map<i64,string> audit(1: i64 record, 2: string key, 3: AccessToken creds, 5: TransactionToken transaction);
	
	/**
	 * Describe {@code record} at {@code timestamp}. This method returns keys for 
	 * fields in {@code record} that contain at least one value at {@code timestamp}. 
	 */
	set<string> describe(1: i64 record, 2: i64 timestamp, 3: AccessToken creds, 4: TransactionToken transaction);
	
	/**
	 * Fetch {@code key} from {@code record} at {@code timestamp}. This method returns
	 * the values that exist in the field mapped from {@code key} at {@code timestamp}.
	 */
	set<TObject> fetch(1: string key, 2: i64 record, 3: i64 timestamp, 4: AccessToken creds, 5: TransactionToken transaction);
	
	/**
	 * Find {@code key} {@code operator} {@code values} at {@code timestamp}. This
	 * method returns the records that match the criteria at {@code timestamp}.
	 */
	set<i64> find(1: string key, 2: Operator operator, 3: list<TObject> values, 4: i64 timestamp, 5: AccessToken creds, 6: TransactionToken transaction);
	
	/**
	 * Ping {@code record}. This method returns {@code true} if {@code record} has at
	 * least one populated field.
	 */
	bool ping(1: i64 record, 2: AccessToken creds, 3: TransactionToken transaction);
	
	/**
	 * Search {@code key} for {@code query}. This method returns the records that have 
	 * a value matching {@code query} in the field mapped from {@code key}.
	 */
	set<i64> search(1: string key, 2: string query, 3: AccessToken creds, 4: TransactionToken transaction);
	
	/**
	 * Verify {@code key} as {@code value} in {@code record} at {@code timestamp}. This
	 * method returns {@code true} if the field contains {@code value} at
	 * {@code timestamp}.
	 */
	bool verify(1: string key, 2: TObject value, 3: i64 record, 4: i64 timestamp, 5: AccessToken creds, 6: TransactionToken transaction);
	
	/**
	 * Revert {@code key} in {@code record} to {@code timestamp}.
	 */
	void revert(1: string key, 2: i64 record, 3: i64 timestamp, 4: AccessToken creds, 5: TransactionToken token);
}

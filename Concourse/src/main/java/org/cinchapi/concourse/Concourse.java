/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse;

import java.net.ConnectException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.cinchapi.common.configuration.Configurations;
import org.cinchapi.common.time.Time;
import org.cinchapi.common.tools.Transformers;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.util.Convert;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * <p>
 * Concourse is a schemaless distributed version control database with
 * serializable transactions and full-text search. Concourse provides a more
 * intuitive approach to data management that is easy to deploy, access and
 * scale with minimal tuning while also maintaining the referential integrity,
 * atomicity, consistency, isolability and durability found in traditional
 * database systems.
 * </p>
 * <h2>Data Model</h2>
 * <p>
 * The Concourse data model is lightweight and flexible which enables it to
 * support any kind of data at very large scales. Concourse trades unnecessary
 * structural notions of predefined schemas, tables and indexes for a more
 * natural modeling of data based solely on the following concepts:
 * </p>
 * <p>
 * <ul>
 * <li><strong>Record</strong> &mdash; A logical grouping of information about a
 * single person, place, thing (i.e. object). Each record is a collection of
 * key/value mappings.
 * <li><strong>Primary Key</strong> &mdash; An immutable marker that is used to
 * identify a single Record. Each Record has a unique Primary Key.
 * <li><strong>Key</strong> &mdash; A label that maps to one or more distinct
 * Values. A Record can have many different Keys. And since Records are
 * independent, the Keys in one Record do not affect the Keys in another Record.
 * <li><strong>Value</strong> &mdash; A typed quantity that is mapped from a Key
 * in a Record.
 * </ul>
 * </p>
 * <h4>Data Types</h4>
 * <p>
 * Concourse natively stores most of the Java primitives: boolean, double,
 * float, integer, long, and string (utf-8 encoded). Otherwise, the value of the
 * {@link #toString()} method for other, non-primitive Objects is used to so
 * those Objects should return a string representation from which the Object can
 * be reconstructed (i.e. JSON, base64 encoded binary, etc).
 * </p>
 * <h4>Links</h4>
 * <p>
 * Concourse supports links between Records and enforces referential integrity
 * with the ability to map a key in one Record to the PrimaryKey of another
 * Record using the {@link #link(String, long, long)} and
 * {@link #link(String, long, String, long)} methods.
 * 
 * <h2>Transactions</h2>
 * <p>
 * By default, Concourse conducts every operation in {@code autocommit} mode
 * where every change is immediately written. Concourse supports the ability to
 * group and stage operations in transactions that are atomic, consistent,
 * isolated, and durable (ACID) using the {@link #stage()}, {@link #commit()}
 * and {@link #abort()} methods.
 * 
 * </p>
 * 
 * @author jnelson
 */
public interface Concourse {

	/**
	 * Discard any changes that are currently staged for commit.
	 * <p>
	 * After this function returns, Concourse will return to {@code autocommit}
	 * mode and all subsequent changes will be immediately written to the
	 * database.
	 * </p>
	 */
	public void abort();

	/**
	 * Add {@code key} as {@code value} in {@code record} if no such mapping
	 * currently exist. No other mappings are affected because {@code key} in
	 * {@code record} may map to multiple distinct values.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is added
	 */
	public boolean add(String key, Object value, long record);

	/**
	 * Audit {@code record} and return a log of revisions.
	 * 
	 * @param record
	 * @return a mapping of timestamps to revision descriptions
	 */
	public Map<DateTime, String> audit(long record);

	/**
	 * Audit {@code key} in {@code record} and return a log of revisions.
	 * 
	 * @param key
	 * @param record
	 * @return a mapping of timestamps to revision descriptions
	 */
	public Map<DateTime, String> audit(String key, long record);

	/**
	 * Clear {@code key} in {@code record} and remove all the currently
	 * contained mappings.
	 * 
	 * @param record
	 */
	public void clear(String key, long record);

	/**
	 * Attempt to permanently commit all the changes that are currently staged.
	 * This function returns {@code true} if and only if all the changes can be
	 * successfully applied to the database.Otherwise, this function returns
	 * {@code false} and all the changes are aborted.
	 * <p>
	 * After this function returns, Concourse will return to {@code autocommit}
	 * mode and all subsequent changes will be immediately written to the
	 * database.
	 * </p>
	 */
	public boolean commit();

	/**
	 * Create a new Record and return its Primary Key.
	 * 
	 * @return the Primary Key of the new Record
	 */
	public long create();

	/**
	 * Describe {@code record} and return its keys that currently map to at
	 * least one value. If there are no such keys, an empty Set is returned.
	 * 
	 * @param record
	 * @return the populated keys
	 */
	public Set<String> describe(long record);

	/**
	 * Describe {@code record} at {@code timestamp} and return its keys that
	 * mapped to at least one value. If there were no such keys, an empty Set is
	 * returned.
	 * 
	 * @param record
	 * @param timestamp
	 * @return the keys for populated keys
	 */
	public Set<String> describe(long record, DateTime timestamp);

	/**
	 * Disconnect from the remote Concourse server.
	 */
	public void exit();

	/**
	 * Fetch {@code key} from {@code record} and return the currently mapped
	 * values. If there are none, an empty Set is returned.
	 * 
	 * @param key
	 * @param record
	 * @return the contained values
	 */
	public Set<Object> fetch(String key, long record);

	/**
	 * Fetch {@code key} from {@code record} at {@code timestamp} and return the
	 * values that were mapped. If there were none, an empty Set is returned.
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 * @return the contained values
	 */
	public Set<Object> fetch(String key, long record, DateTime timestamp);

	/**
	 * Find {@code key} {@code operator} {@code values} at {@code timestamp} and
	 * return the records that satisfied the criteria. This is analogous to a
	 * SELECT query in a RDBMS. If there were no records that matched the
	 * criteria, an empty Set is returned.
	 * 
	 * @param timestamp
	 * @param key
	 * @param operator
	 * @param values
	 * @return the records that match the criteria
	 */
	public Set<Long> find(DateTime timestamp, String key, Operator operator,
			Object... values);

	/**
	 * Find {@code key} {@code operator} {@code values} and return the records
	 * that satisfy the criteria. This is analogous to a SELECT query in a
	 * RDBMS. If there are now records that match the criteria, an empty Set is
	 * returned.
	 * 
	 * @param key
	 * @param operator
	 * @param values
	 * @return the records that match the criteria
	 */
	public Set<Long> find(String key, Operator operator, Object... values);

	/**
	 * Get {@code key} from {@code record} and return the first mapped value or
	 * {@code null} if there are none. Compared to {@link #fetch(String, long)},
	 * this method is suited for cases when the caller is certain that
	 * {@code key} in {@code record} maps to a single value of type {@code T}.
	 * 
	 * @param key
	 * @param record
	 * @return the first mapped value
	 */
	public <T> T get(String key, long record);

	/**
	 * Get {@code key} from {@code record} at {@code timestamp} and return the
	 * first mapped value or {@code null} if there were none. Compared to
	 * {@link #fetch(String, long, long)}, this method is suited for cases when
	 * the caller is certain that {@code key} in {@code record} mapped to a
	 * single value of type {@code T} at {@code timestamp}.
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 * @return the first mapped value
	 */
	public <T> T get(String key, long record, DateTime timestamp);

	/**
	 * Link {@code key} in {@code source} to {@code destination}. In other
	 * words, a {@link Link} to {@code destination} is mapped from {@code key}
	 * in {@code source}.
	 * 
	 * @param key
	 * @param source
	 * @param destination
	 * @return {@code true} if the one way link is added
	 */
	public boolean link(String key, long source, long destination);

	/**
	 * Link {@code sourceKey} in {@code source} to {@code destinationKey} in
	 * {@code destination}. In other words, a {@link Link} to
	 * {@code destination} is mapped from {@code sourceKey} in {@code source}
	 * and a {@link Link} to {@code source} is mapped from
	 * {@code destinationKey} in {@code destination}.
	 * 
	 * @param sourceKey
	 * @param source
	 * @param destinationKey
	 * @param destination
	 * @return {@code true} if a two way link exists between {@code source} and
	 *         {@code destination}
	 */
	public boolean link(String sourceKey, long source, String destinationKey,
			long destination);

	/**
	 * Ping {@code record} and return {@code true} if there is
	 * <em>currently</em> at least one populated key.
	 * 
	 * @param record
	 * @return {@code true} if {@code record} currently contains data
	 */
	public boolean ping(long record);

	/**
	 * Remove {@code key} as {@code value} from {@code record} if the mapping
	 * currently exists. No other mappings are affected.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping is removed
	 */
	public boolean remove(String key, Object value, long record);

	/**
	 * Revert {@code key} in {@code record} to {@code timestamp}. This method
	 * restores the key to its state at {@code timestamp} by reversing all
	 * revisions that have occurred since.
	 * <p>
	 * Please note that this method <strong>does not</strong> {@code rollback}
	 * any revisions, but creates <em>new</em> revisions that are the inverse of
	 * all revisions since {@code timestamp} in reverse order.
	 * </p>
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 */
	public void revert(String key, long record, DateTime timestamp);

	/**
	 * Search {@code key} for {@code query} and return the records that match
	 * the fulltext query. If there are no such records, an empty Set is
	 * returned.
	 * 
	 * @param key
	 * @param query
	 * @return the records that match the query
	 */
	public Set<Long> search(String key, String query);

	/**
	 * Set {@code key} as {@code value} in {@code record}. This is a convenience
	 * method that clears the values currently mapped from {@code key} and adds
	 * a new mapping for {@code value}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the old mappings are removed and the new one is
	 *         added
	 */
	public boolean set(String key, Object value, long record);

	/**
	 * Turn on {@code staging} mode so that all subsequent changes are
	 * collected in a staging area before possibly being committed to the
	 * database. Staged operations are guaranteed to be reliable, all or nothing
	 * units of work that allow correct recovery from failures and provide
	 * isolation between clients so the database is always in a consistent state
	 * (i.e. a transaction).
	 * <p>
	 * After this method returns, all subsequent operations will be done in
	 * {@code staging} mode until either {@link #abort()} or {@link #commit()}
	 * is invoked.
	 * </p>
	 */
	public void stage();

	/**
	 * Verify {@code key} equals {@code value} in {@code record} and return
	 * {@code true} if {@code value} is currently mapped.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @return {@code true} if the mapping exists
	 */
	public boolean verify(String key, Object value, long record);

	/**
	 * Verify {@code key} equaled {@code value} in {@code record} at
	 * {@code timestamp} and return {@code true} if {@code value} was mapped.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @param timestamp
	 * @return {@code true} if the mapping existed
	 */
	public boolean verify(String key, Object value, long record,
			DateTime timestamp);

	/**
	 * The implementation of the {@link Concourse} interface that establishes a
	 * connection with the remote server and handles communication. This class
	 * is a more user friendly wrapper around a Thrift
	 * {@link ConcourseService.Client}.
	 * 
	 * @author jnelson
	 */
	public final static class Client implements Concourse {

		private static final Logger log = LoggerFactory
				.getLogger(Concourse.class);

		/**
		 * All configuration information is contained in this prefs file.
		 */
		private static final String configFileName = "concourse.prefs";

		/**
		 * Handler for configuration preferences.
		 */
		private static final PropertiesConfiguration config = Configurations
				.loadPropertiesConfiguration(configFileName);

		/**
		 * Represents a request to respond to a query using the current state as
		 * opposed to the history.
		 */
		private static DateTime now = new DateTime(0);

		private final String username;
		private final String password;

		/**
		 * The Thrift client that actually handles all RPC communication.
		 */
		private final ConcourseService.Client client;

		/**
		 * The client keeps a copy of its {@link AccessToken} and passes it to
		 * the
		 * server for each remote procedure call. The client will
		 * re-authenticate
		 * when necessary using the username/password read from the prefs file.
		 */
		private AccessToken creds = null;

		/**
		 * Whenever the client starts a Transaction, it keeps a
		 * {@link TransactionToken} so that the server can stage the changes in
		 * the
		 * appropriate place.
		 */
		private TransactionToken transaction = null;

		/**
		 * Create a new Client connection to the Concourse server specified in
		 * {@code concourse.prefs} and return a handler to facilitate database
		 * interaction.
		 */
		public Client() {
			this(config.getString("CONCOURSE_SERVER_HOST", "localhost"), config
					.getInt("CONCOURSE_SERVER_PORT", 1717), config.getString(
					"USERNAME", "admin"), config.getString("PASSWORD", "admin"));
		}

		/**
		 * Create a new Client connection to a Concourse server and return a
		 * handler to facilitate database interaction.
		 * 
		 * @param host
		 * @param port
		 * @param username
		 * @param password
		 */
		private Client(String host, int port, String username, String password) {
			this.username = username;
			this.password = password;
			TTransport transport = new TSocket(host, port);
			try {
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				client = new ConcourseService.Client(protocol);
				authenticate();
				log.info("Connected to Concourse server at {}:{}.", host, port);
				Runtime.getRuntime().addShutdownHook(new Thread("shutdown") {

					@Override
					public void run() {
						if(transaction != null) {
							abort();
							log.warn("Prior to shutdown, the client was in the middle "
									+ "of an uncommitted transaction. That transaction "
									+ "has been aborted and all of its uncommited changes "
									+ "have been lost.");
						}
					}

				});
			}
			catch (TTransportException e) {
				if(e.getCause() instanceof ConnectException) {
					log.error("Unable to establish a connection with the "
							+ "Concourse server at {}:{}. Please check "
							+ "that the remote service is actually running "
							+ "and accepting connections.", host, port);

				}
				throw Throwables.propagate(e);
			}
		}

		@Override
		public void abort() {
			execute(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					if(transaction != null) {
						client.abort(creds, transaction);
						transaction = null;
					}
					else {
						log.warn("There is no transaction to abort.");
					}
					return null;
				}

			});
		}

		@Override
		public boolean add(final String key, final Object value,
				final long record) {
			return execute(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					return client.add(key, Convert.javaToThrift(value), record,
							creds, transaction);
				}

			});
		}

		@Override
		public Map<DateTime, String> audit(final long record) {
			return execute(new Callable<Map<DateTime, String>>() {

				@Override
				public Map<DateTime, String> call() throws Exception {
					Map<Long, String> audit = client.audit(record, null, creds,
							transaction);
					return Transformers.transformMap(audit,
							new Function<Long, DateTime>() {

								@Override
								public DateTime apply(Long input) {
									return Convert.unixToJoda(input);
								}

							});
				}

			});
		}

		@Override
		public Map<DateTime, String> audit(final String key, final long record) {
			return execute(new Callable<Map<DateTime, String>>() {

				@Override
				public Map<DateTime, String> call() throws Exception {
					Map<Long, String> audit = client.audit(record, key, creds,
							transaction);
					return Transformers.transformMap(audit,
							new Function<Long, DateTime>() {

								@Override
								public DateTime apply(Long input) {
									return Convert.unixToJoda(input);
								}

							});
				}

			});
		}

		@Override
		public void clear(String key, long record) {
			Set<Object> values = fetch(key, record);
			for (Object value : values) {
				remove(key, value, record);
			}
		}

		@Override
		public boolean commit() {
			return execute(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					boolean result = client.commit(creds, transaction);
					transaction = null;
					return result;
				}

			});
		}

		@Override
		public long create() {
			return Time.now(); // TODO get a primary key using a plugin
		}

		@Override
		public Set<String> describe(long record) {
			return describe(record, now);
		}

		@Override
		public Set<String> describe(final long record, final DateTime timestamp) {
			return execute(new Callable<Set<String>>() {

				@Override
				public Set<String> call() throws Exception {
					return client.describe(record,
							Convert.jodaToUnix(timestamp), creds, transaction);
				}

			});
		}

		@Override
		public void exit() {
			client.getInputProtocol().getTransport().close();
			client.getOutputProtocol().getTransport().close();
			log.info("The client has disconnected");
		}

		@Override
		public Set<Object> fetch(String key, long record) {
			return fetch(key, record, now);
		}

		@Override
		public Set<Object> fetch(final String key, final long record,
				final DateTime timestamp) {
			return execute(new Callable<Set<Object>>() {

				@Override
				public Set<Object> call() throws Exception {
					Set<TObject> values = client.fetch(key, record,
							Convert.jodaToUnix(timestamp), creds, transaction);
					return Transformers.transformSet(values,
							new Function<TObject, Object>() {

								@Override
								public Object apply(TObject input) {
									return Convert.thriftToJava(input);
								}

							});
				}

			});
		}

		@Override
		public Set<Long> find(final DateTime timestamp, final String key,
				final Operator operator, final Object... values) {
			return execute(new Callable<Set<Long>>() {

				@Override
				public Set<Long> call() throws Exception {
					return client.find(key, operator, Lists.transform(
							Lists.newArrayList(values),
							new Function<Object, TObject>() {

								@Override
								public TObject apply(Object input) {
									return Convert.javaToThrift(input);
								}

							}), Convert.jodaToUnix(timestamp), creds,
							transaction);
				}

			});
		}

		@Override
		public Set<Long> find(String key, Operator operator, Object... values) {
			return find(now, key, operator, values);
		}

		@Override
		@Nullable
		public <T> T get(String key, long record) {
			return get(key, record, now);
		}

		@SuppressWarnings("unchecked")
		@Override
		@Nullable
		public <T> T get(String key, long record, DateTime timestamp) {
			Set<Object> values = fetch(key, record, timestamp);
			if(!values.isEmpty()) {
				return (T) values.iterator().next();
			}
			return null;
		}

		@Override
		public boolean link(String key, long source, long destination) {
			return add(key, Link.to(destination), source);
		}

		@Override
		public boolean link(String sourceKey, long source,
				String destinationKey, long destination) {
			return link(sourceKey, source, destination)
					^ link(destinationKey, destination, source);
		}

		@Override
		public boolean ping(final long record) {
			return execute(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					return client.ping(record, creds, transaction);
				}

			});
		}

		@Override
		public boolean remove(final String key, final Object value,
				final long record) {
			return execute(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					return client.remove(key, Convert.javaToThrift(value),
							record, creds, transaction);
				}

			});
		}

		@Override
		public void revert(final String key, final long record,
				final DateTime timestamp) {
			execute(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					client.revert(key, record, Convert.jodaToUnix(timestamp),
							creds, transaction);
					return null;
				}

			});

		}

		@Override
		public Set<Long> search(final String key, final String query) {
			return execute(new Callable<Set<Long>>() {

				@Override
				public Set<Long> call() throws Exception {
					return client.search(key, query, creds, transaction);
				}

			});
		}

		@Override
		public boolean set(String key, Object value, long record) {
			Set<Object> values = fetch(key, record);
			for (Object v : values) {
				remove(key, v, record);
			}
			return add(key, value, record);
		}

		@Override
		public void stage() {
			execute(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					transaction = client.stage(creds);
					return null;
				}

			});
		}

		@Override
		public boolean verify(String key, Object value, long record) {
			return verify(key, value, record, now);
		}

		@Override
		public boolean verify(final String key, final Object value,
				final long record, final DateTime timestamp) {
			return execute(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					return client.verify(key, Convert.javaToThrift(value),
							record, Convert.jodaToUnix(timestamp), creds,
							transaction);
				}

			});
		}

		/**
		 * Authenticate the {@link #username} and {@link #password} and populate
		 * {@link #creds} with the appropriate AccessToken.
		 */
		private void authenticate() {
			try {
				creds = client.login(username, password);
			}
			catch (TException e) {
				throw Throwables.propagate(e);
			}
		}

		/**
		 * Execute the task defined in {@code callable}. This method contains
		 * retry logic to handle cases when {@code creds} expires and must be
		 * updated.
		 * 
		 * @param callable
		 * @return the task result
		 */
		private <T> T execute(Callable<T> callable) {
			try {
				return callable.call();
			}
			catch (SecurityException e) {
				authenticate();
				return execute(callable);
			}
			catch (Exception e) {
				throw Throwables.propagate(e);
			}
		}

	}

}

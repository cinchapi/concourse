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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.cinchapi.common.configuration.Configurations;
import org.cinchapi.common.tools.Transformers;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * The implementation of the {@link Concourse} interface that establishes a
 * connection with the remote server and handles communication. This class is a
 * more user friendly wrapper around a Thrift {@link ConcourseService.Client}.
 * 
 * @author jnelson
 */
final class ConcourseHandler extends Concourse {

	/**
	 * All configuration information is contained in this prefs file.
	 */
	private static final String configFileName = "concourse.prefs";

	/**
	 * Handler for configuration preferences.
	 */
	private static final PropertiesConfiguration config = Configurations
			.loadPropertiesConfiguration(configFileName);

	private final String host;
	private final int port;
	private final String username;
	private final String password;

	/**
	 * The Thrift client that actually handles all RPC communication.
	 */
	private final ConcourseService.Client client;

	/**
	 * The client keeps a copy of its {@link AccessToken} and passes it to the
	 * server for each remote procedure call. The client will re-authenticate
	 * when necessary using the username/password read from the prefs file.
	 */
	private AccessToken creds = null;

	/**
	 * Whenever the client starts a Transaction, it keeps a
	 * {@link TransactionToken} so that the server can stage the changes in the
	 * appropriate place.
	 */
	private TransactionToken transaction = null;

	/**
	 * Construct a new instance.
	 */
	public ConcourseHandler() {
		this.host = config.getString("CONCOURSE_SERVER_HOST", "localhost");
		this.port = config.getInt("CONCOURSE_SERVER_PORT", 1717);
		this.username = config.getString("USERNAME", "admin");
		this.password = config.getString("PASSWORD", "admin");
		TTransport transport = new TSocket(host, port);
		try {
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			this.client = new ConcourseService.Client(protocol);
			authenticate();
		}
		catch (TTransportException e) {
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void abort() {
		execute(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				client.abort(creds, transaction);
				return null;
			}

		});
	}

	@Override
	public boolean add(final String key, final Object value, final long record) {
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
	public Set<String> describe(final long record, final DateTime timestamp) {
		return execute(new Callable<Set<String>>() {

			@Override
			public Set<String> call() throws Exception {
				return client.describe(record, Convert.jodaToUnix(timestamp),
						creds, transaction);
			}

		});
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

						}), Convert.jodaToUnix(timestamp), creds, transaction);
			}

		});
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
				return client.remove(key, Convert.javaToThrift(value), record,
						creds, transaction);
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
	public void stage() {
		execute(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				client.stage(creds);
				return null;
			}

		});
	}

	@Override
	public boolean verify(final String key, final Object value,
			final long record, final DateTime timestamp) {
		return execute(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return client.verify(key, Convert.javaToThrift(value), record,
						Convert.jodaToUnix(timestamp), creds, transaction);
			}

		});
	}

	/**
	 * Authenticate the {@link #username} and {@link #password} and return an
	 * populated {@link #creds} with the appropriate AccessToken.
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
	 * Execute the task defined in {@code callable}. This method contains retry
	 * logic to handle cases when {@code creds} expires and must be updated.
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

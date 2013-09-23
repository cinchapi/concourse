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
package org.cinchapi.concourse.server;

import static org.cinchapi.concourse.server.util.Loggers.getLogger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.Files;
import org.cinchapi.common.time.Time;
import org.cinchapi.concourse.server.engine.Engine;
import org.cinchapi.concourse.server.engine.Transaction;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.AccessTokens;
import org.cinchapi.concourse.thrift.ConcourseService;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.ConcourseService.Iface;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * Accepts requests from clients to read and write data in Concourse. The server
 * is configured with a {@code concourse.prefs} file.
 * 
 * @author jnelson
 */
@PackagePrivate
public class ConcourseServer implements ConcourseService.Iface {

	/**
	 * Run the server...
	 * 
	 * @param args
	 * @throws TTransportException
	 */
	public static void main(String... args) throws TTransportException {
		final ConcourseServer server = new ConcourseServer();

		// Start the server...
		Thread serverThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					server.prepare();
					server.hello();
					server.start();
				}
				catch (TTransportException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}

		}, "server-thread");
		serverThread.start();

		// Prepare for graceful shutdown...
		// NOTE: It may be necessary to run the Java VM with
		// -Djava.net.preferIPv4Stack=true
		Thread shutdownThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					ServerSocket socket = new ServerSocket(SHUTDOWN_PORT);
					socket.accept(); // block until a shutdown request is made
					log.info("Shutdown request received");
					server.stop();
					socket.close();
				}
				catch (IOException e) {
					e.printStackTrace();
					System.exit(-1);
				}

			}

		}, "shutdown-thread");
		shutdownThread.setDaemon(true);
		shutdownThread.start();
	}

	private static final int SERVER_PORT = 1717; // This may become
													// configurable in a
													// prefs file in a
													// future release.

	private static final int SHUTDOWN_PORT = 3434; // This may become
													// configurable in a prefs
													// file in a future release.

	private static final int NUM_WORKER_THREADS = 100; // This may become
														// configurable in a
														// prefs file in a
														// future release.

	private static final Logger log = getLogger();

	/**
	 * The Thrift server controls the RPC protocol. Use
	 * https://github.com/m1ch1/mapkeeper/wiki/Thrift-Java-Servers-Compared for
	 * a reference.
	 */
	private final TServer server;

	/**
	 * The Engine controls all the logic for data storage and retrieval.
	 */
	private final Engine engine;

	/**
	 * The context that is passed to and around the Engine for global
	 * configuration and state.
	 */
	private final Context context;

	/**
	 * The server maintains a collection of {@link Transaction} objects to
	 * ensure that client requests are properly routed. When the client makes a
	 * call to {@link #stage(AccessToken)}, a Transaction is started on the
	 * server and a {@link TransactionToken} is used for the client to reference
	 * that Transaction in future calls.
	 */
	private final Map<TransactionToken, Transaction> transactions = Maps
			.newHashMap();

	/**
	 * Construct a ConcourseServer that listens on {@link #SERVER_PORT} and
	 * stores data in {@link Properties#DATA_HOME}.
	 * 
	 * @throws TTransportException
	 */
	public ConcourseServer() throws TTransportException {
		this(SERVER_PORT, Properties.DATA_HOME);
	}

	/**
	 * Construct a ConcourseServer that listens on {@code port} and store data
	 * in {@code backingStore}.
	 * 
	 * @param port
	 * @param backingStore
	 * @throws TTransportException
	 */
	public ConcourseServer(int port, String backingStore)
			throws TTransportException {
		Files.mkdirs(backingStore);
		TServerSocket socket = new TServerSocket(port);
		ConcourseService.Processor<Iface> processor = new ConcourseService.Processor<Iface>(
				this);
		Args args = new TThreadPoolServer.Args(socket);
		args.processor(processor);
		args.maxWorkerThreads(NUM_WORKER_THREADS);
		this.server = new TThreadPoolServer(args);
		this.context = Context.getContext();
		this.engine = new Engine(backingStore, context);
	}

	@Override
	public void abort(AccessToken creds, TransactionToken transaction)
			throws TException {
		authenticate(creds);
		Preconditions.checkArgument(transaction.getAccessToken().equals(creds)
				&& transactions.containsKey(transaction));
		transactions.remove(transaction).abort();
	}

	@Override
	public boolean add(String key, TObject value, long record,
			AccessToken creds, TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			return transactions.get(transaction).add(key, value, record);

		}
		return engine.add(key, value, record);
	}

	@Override
	public Map<Long, String> audit(long record, String key, AccessToken creds,
			TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return Strings.isNullOrEmpty(key) ? t.audit(record) : t.audit(key,
					record);
		}
		return Strings.isNullOrEmpty(key) ? engine.audit(record) : engine
				.audit(key, record);
	}

	@Override
	public boolean commit(AccessToken creds, TransactionToken transaction)
			throws TException {
		authenticate(creds);
		Preconditions.checkArgument(transaction.getAccessToken().equals(creds)
				&& transactions.containsKey(transaction));
		return transactions.remove(transaction).commit();
	}

	@Override
	public Set<String> describe(long record, long timestamp, AccessToken creds,
			TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return timestamp == 0 ? t.describe(record) : t.describe(record,
					timestamp);
		}
		return timestamp == 0 ? engine.describe(record) : engine.describe(
				record, timestamp);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp,
			AccessToken creds, TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return timestamp == 0 ? t.fetch(key, record) : t.fetch(key, record,
					timestamp);
		}
		return timestamp == 0 ? engine.fetch(key, record) : engine.fetch(key,
				record, timestamp);
	}

	@Override
	public Set<Long> find(String key, Operator operator, List<TObject> values,
			long timestamp, AccessToken creds, TransactionToken transaction)
			throws TException {
		authenticate(creds);
		TObject[] tValues = values.toArray(new TObject[values.size()]);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return timestamp == 0 ? t.find(key, operator, tValues) : t.find(
					timestamp, key, operator, tValues);
		}
		return timestamp == 0 ? engine.find(key, operator, tValues) : engine
				.find(timestamp, key, operator, tValues);
	}

	@Override
	public AccessToken login(String username, String password)
			throws TException {
		validate(username, password);
		return AccessTokens.createAccessToken(username, password);
	}

	@Override
	public void logout(AccessToken creds) throws TException {
		authenticate(creds);
		expire(creds);

	}

	@Override
	public boolean ping(long record, AccessToken creds,
			TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return t.ping(record);
		}
		return engine.ping(record);
	}

	@Override
	public boolean remove(String key, TObject value, long record,
			AccessToken creds, TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			return transactions.get(transaction).remove(key, value, record);
		}
		return engine.remove(key, value, record);
	}

	@Override
	public void revert(String key, long record, long timestamp,
			AccessToken creds, TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			t.revert(key, record, timestamp);
		}
		engine.revert(key, record, timestamp);
	}

	@Override
	public Set<Long> search(String key, String query, AccessToken creds,
			TransactionToken transaction) throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return t.search(key, query);
		}
		return engine.search(key, query);
	}

	@Override
	public TransactionToken stage(AccessToken creds) throws TException {
		authenticate(creds);
		TransactionToken token = new TransactionToken(creds, Time.now());
		Transaction transaction = engine.startTransaction();
		transactions.put(token, transaction);
		log.info("Started Transaction {}", transaction.hashCode());
		return token;
	}

	/**
	 * Start the server.
	 * 
	 * @throws TTransportException
	 */
	public void start() throws TTransportException {
		engine.start();
		log.info("The Concourse server has started");
		server.serve();

	}

	/**
	 * Stop the server.
	 */
	public void stop() {
		if(server.isServing()) {
			server.stop();
			log.info("The Concourse server has stopped");
			System.exit(0);
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record,
			long timestamp, AccessToken creds, TransactionToken transaction)
			throws TException {
		authenticate(creds);
		if(transaction != null) {
			Preconditions.checkArgument(transaction.getAccessToken().equals(
					creds)
					&& transactions.containsKey(transaction));
			Transaction t = transactions.get(transaction);
			return timestamp == 0 ? t.verify(key, value, record) : t.verify(
					key, value, record, timestamp);
		}
		return timestamp == 0 ? engine.verify(key, value, record) : engine
				.verify(key, value, record, timestamp);
	}

	/**
	 * Verify that {@code token} is valid.
	 * 
	 * @param token
	 * @throws SecurityException
	 */
	private void authenticate(AccessToken token) throws SecurityException {
		// TODO check token and throw an exception if its not valid
	}

	/**
	 * Expire {@code token} so that it is no longer valid.
	 * 
	 * @param token
	 * @throws SecurityException
	 */
	private void expire(AccessToken token) throws SecurityException {
		// TODO implement
	}

	/**
	 * Print the server banner to the console.
	 */
	private void hello() {
		StringBuilder banner = new StringBuilder();
		banner.append(" _____").append(System.lineSeparator());
		banner.append("/  __ \\").append(System.lineSeparator());
		banner.append("| /  \\/ ___  _ __   ___ ___  _   _ _ __ ___  ___")
				.append(System.lineSeparator());
		banner.append("| |    / _ \\| '_ \\ / __/ _ \\| | | | '__/ __|/ _ \\")
				.append(System.lineSeparator());
		banner.append("| \\__/\\ (_) | | | | (_| (_) | |_| | |  \\__ \\  __/")
				.append(System.lineSeparator());
		banner.append(" \\____/\\___/|_| |_|\\___\\___/ \\__,_|_|  |___/\\___|")
				.append(System.lineSeparator());
		banner.append("").append(System.lineSeparator());
		banner.append(
				"Copyright (c) 2013, Cinchapi Software Collective, LLC. All Rights Reserved.")
				.append(System.lineSeparator());
		System.out.print(banner);
	}

	/**
	 * Prepare the server for startup by running health checks.
	 * 
	 * @throws TTransportException
	 */
	private void prepare() throws TTransportException {
		log.info("concourse.home located at {}", context.properties().home());
//		long heap = Runtime.getRuntime().maxMemory() / 1048576;
	}

	/**
	 * Validate that the {@code username} and {@code password} pair represent
	 * correct credentials.
	 * 
	 * @param username
	 * @param password
	 * @throws SecurityException
	 */
	private void validate(String username, String password)
			throws SecurityException {
		// TODO check if creds are correct
	}

	/**
	 * Connects to the {@link ConcourseServer#SHUTDOWN_PORT} in order to
	 * initiate a graceful shutdown.
	 * 
	 * @author jnelson
	 */
	public static class ShutdownRunner {

		/**
		 * Run as org.cinchapi.concourse.server.ConcourseServer$ShutdownRunner
		 * 
		 * @param args
		 * @throws IOException
		 * @throws UnknownHostException
		 */
		public static void main(String... args) {
			try {
				log.info("Binding to shutdown port {}", SHUTDOWN_PORT);
				Socket socket = new Socket("localhost", SHUTDOWN_PORT);
				socket.close();
				System.exit(0);
			}
			catch (IOException e) {
				// do nothing
			}
		}
	}

}

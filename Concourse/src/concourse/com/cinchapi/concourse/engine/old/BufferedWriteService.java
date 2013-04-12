/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.engine.old;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * <p>
 * A {@link ConcourseService} that maintains two embedded services for the
 * purpose of buffering writes.
 * </p>
 * <p>
 * Writes are initially written to the {@code secondary} service and must be
 * eventually flushed to the {@code primary} such that the correct value for a
 * read is always the XOR (see {@link Sets#symmetricDifference(Set, Set)}) or
 * XOR truth (see <a
 * href="http://en.wikipedia.org/wiki/Exclusive_or#Truth_table"
 * >http://en.wikipedia.org/wiki/Exclusive_or#Truth_table</a>) of the read
 * values returned from the individual stores.
 * </p>
 * <p>
 * <strong>Note</strong>: The implementing class is responsible for flushing the
 * {@code secondary} service to the {@code primary} and can override the write
 * functions as necessary to do so.
 * </p>
 * 
 * @author jnelson
 */
public abstract class BufferedWriteService extends ConcourseService {

	private static final int EXECUTOR_SHUTDOWN_WAIT_IN_SECS = 60;
	private static final Logger log = LoggerFactory
			.getLogger(BufferedWriteService.class);

	protected final ConcourseService buffer;
	protected final ConcourseService primary;
	private final ExecutorService executor = Executors.newCachedThreadPool();

	/**
	 * Construct a new instance.
	 * 
	 * @param buffer
	 * @param primary
	 */
	protected BufferedWriteService(ConcourseService buffer,
			ConcourseService primary) {
		Preconditions.checkArgument(
				!this.getClass().isAssignableFrom(primary.getClass()),
				"Cannot embed a %s into %s", primary.getClass(),
				this.getClass());
		Preconditions
				.checkArgument(
						!this.getClass().isAssignableFrom(buffer.getClass()),
						"Cannot embed a %s into %s", buffer.getClass(),
						this.getClass());
		this.primary = primary;
		this.buffer = buffer;
	}

	@Override
	public synchronized void shutdown() {
		// This is a generic shutdown that only handles private variables.
		// A subclasses should override this method and explicitly handle the
		// primary and initial services, etc
		executor.shutdown();
		try {
			if(!executor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_IN_SECS,
					TimeUnit.SECONDS)) {
				List<Runnable> tasks = executor.shutdownNow();
				log.error(
						"The service did not properly shutdown. The following tasks were dropped: {}",
						tasks);
			}
		}
		catch (InterruptedException e) {
			log.error("An error occured while shutting down the {}: {}", this
					.getClass().getName(), e);
		}
	}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		Future<Boolean> clr = executor.submit(Threads.add(buffer, row, column,
				value));
		try {
			return clr.get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected final Set<String> describeSpi(long row) {
		Future<Set<String>> dbr = executor.submit(Threads
				.describe(primary, row));
		Future<Set<String>> clr = executor
				.submit(Threads.describe(buffer, row));
		try {
			return Sets.symmetricDifference(dbr.get(), clr.get());
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Sets.newHashSet();
		}
	}

	@Override
	protected final boolean existsSpi(String column, Object value, long row) {
		Future<Boolean> dbr = executor.submit(Threads.exists(primary, row,
				column, value));
		Future<Boolean> clr = executor.submit(Threads.exists(buffer, row,
				column, value));
		try {
			return dbr.get() ^ clr.get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected final Set<Object> fetchSpi(String column, long timestamp, long row) {
		Future<Set<Object>> dbr = executor.submit(Threads.fetch(primary, row,
				column, timestamp));
		Future<Set<Object>> clr = executor.submit(Threads.fetch(buffer, row,
				column, timestamp));
		try {
			return Sets.symmetricDifference(dbr.get(), clr.get());
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Sets.newHashSet();
		}
	}

	@Override
	protected final Set<Long> querySpi(String column, Operator operator,
			Object... values) {
		Future<Set<Long>> dbr = executor.submit(Threads.query(primary, column,
				operator, values));
		Future<Set<Long>> clr = executor.submit(Threads.query(buffer, column,
				operator, values));
		try {
			return Sets.symmetricDifference(dbr.get(), clr.get());
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Sets.newHashSet();
		}
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		Future<Boolean> clr = executor.submit(Threads.remove(buffer, row,
				column, value));
		try {
			return clr.get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected final long sizeOfSpi(String column, Long row) {
		return primary.sizeOf(column, row) + buffer.sizeOf(column, row);
	}

	/**
	 * Contains various callables and runnables to enable concurrent operations
	 * within a {@link BufferedWriteService}.
	 * 
	 * @author jnelson
	 */
	protected static class Threads {

		/**
		 * Execute {@link ConcourseService#add(String, Object, long)}.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 * @return the method return value
		 */
		public static Add add(ConcourseService service, long row,
				String column, Object value) {
			return new Add(service, row, column, value);
		}

		/**
		 * Execute {@link ConcourseService#describe(long)}.
		 * 
		 * @param service
		 * @param row
		 * @return the method return value
		 */
		public static Describe describe(ConcourseService service, long row) {
			return new Describe(service, row);
		}

		/**
		 * Execute {@link ConcourseService#exists(String, Object, long)}.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 * @return the method return value
		 */
		public static Exists exists(ConcourseService service, long row,
				String column, Object value) {
			return new Exists(service, row, column, value);
		}

		/**
		 * Execute {@link ConcourseService#fetch(String, long)}.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param timestamp
		 * @return the method return value
		 */
		public static Fetch fetch(ConcourseService service, long row,
				String column, long timestamp) {
			return new Fetch(service, row, column, timestamp);
		}

		/**
		 * Execute {@link ConcourseService#query(String, Operator, Object...)} .
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 * @return the method return value
		 */
		public static Query query(ConcourseService service, String column,
				Operator operator, Object... values) {
			return new Query(service, column, operator, values);
		}

		/**
		 * Execute {@link ConcourseService#remove(String, Object, long)}.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 * @return the method return value
		 */
		public static Remove remove(ConcourseService service, long row,
				String column, Object value) {
			return new Remove(service, row, column, value);
		}

		private Threads() {}

		/**
		 * A {@link Callable} that can execute methods in an
		 * {@link ConcourseService} and return a result.
		 * 
		 * @author jnelson
		 * @param <V>
		 *            - the result type of the called method
		 */
		@Immutable
		private static abstract class AbstractConcourseServiceCallable<V> implements
				Callable<V> {

			protected final ConcourseService service;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 */
			public AbstractConcourseServiceCallable(ConcourseService service) {
				this.service = service;
			}
		}

		/**
		 * Execute the {@link ConcourseService#add(String, Object, long)}
		 * method.
		 * 
		 * @author jnelson
		 */
		private static final class Add extends
				AbstractConcourseServiceCallable<Boolean> {

			private final long row;
			private final String column;
			private final Object value;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param row
			 * @param column
			 * @param value
			 */
			public Add(ConcourseService service, long row, String column,
					Object value) {
				super(service);
				this.row = row;
				this.column = column;
				this.value = value;
			}

			@Override
			public Boolean call() throws Exception {
				return service.addSpi(column, value, row); // I'm calling
															// addSpi() instead
															// of add() to
															// prevent an
															// erroneous/extra
															// call to exists()
			}

		}

		/**
		 * Execute the {@link ConcourseService#describe(long)} method.
		 * 
		 * @author jnelson
		 */
		private static final class Describe extends
				AbstractConcourseServiceCallable<Set<String>> {

			private final long row;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param row
			 */
			public Describe(ConcourseService service, long row) {
				super(service);
				this.row = row;
			}

			@Override
			public Set<String> call() throws Exception {
				return service.describe(row);
			}

		}

		/**
		 * Execute the {@link ConcourseService#exists(String, Object, long)}
		 * method.
		 * 
		 * @author jnelson
		 */
		private static final class Exists extends
				AbstractConcourseServiceCallable<Boolean> {

			private final long row;
			private final String column;
			private final Object value;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param row
			 * @param column
			 * @param value
			 */
			public Exists(ConcourseService service, long row, String column,
					Object value) {
				super(service);
				this.row = row;
				this.column = column;
				this.value = value;
			}

			@Override
			public Boolean call() throws Exception {
				return service.exists(column, value, row);
			}

		}

		/**
		 * Execute the {@link ConcourseService#fetch(String, long)} method.
		 * 
		 * @author jnelson
		 */
		private static final class Fetch extends
				AbstractConcourseServiceCallable<Set<Object>> {

			private final long row;
			private final String column;
			private final long timestamp;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param row
			 * @param column
			 */
			public Fetch(ConcourseService service, long row, String column,
					long timestamp) {
				super(service);
				this.row = row;
				this.column = column;
				this.timestamp = timestamp;
			}

			@Override
			public Set<Object> call() throws Exception {
				return service.fetch(column, timestamp, row);
			}

		}

		/**
		 * Execute the
		 * {@link ConcourseService#query(String, com.cinchapi.concourse.store.api.Queryable.Operator, Object...)}
		 * method.
		 * 
		 * @author jnelson
		 */
		private static final class Query extends
				AbstractConcourseServiceCallable<Set<Long>> {

			private final String column;
			private final Operator operator;
			private final Object[] values;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param column
			 * @param operator
			 * @param values
			 */
			public Query(ConcourseService service, String column,
					Operator operator, Object... values) {
				super(service);
				this.column = column;
				this.operator = operator;
				this.values = values;
			}

			@Override
			public Set<Long> call() throws Exception {
				return service.query(column, operator, values);
			}

		}

		/**
		 * Execute the {@link ConcourseService#remove(String, Object, long)}
		 * method.
		 * 
		 * @author jnelson
		 */
		private static final class Remove extends
				AbstractConcourseServiceCallable<Boolean> {

			private final long row;
			private final String column;
			private final Object value;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param row
			 * @param column
			 * @param value
			 */
			public Remove(ConcourseService service, long row, String column,
					Object value) {
				super(service);
				this.row = row;
				this.column = column;
				this.value = value;
			}

			@Override
			public Boolean call() throws Exception {
				return service.removeSpi(column, value, row); // I'm calling
																// removeSpi()
																// instead of
																// remove() to
																// prevent an
																// erroneous/extra
																// call to
																// exists()
			}

		}

	}

}

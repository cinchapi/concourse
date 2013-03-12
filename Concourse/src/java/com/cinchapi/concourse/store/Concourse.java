package com.cinchapi.concourse.store;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.store.api.ConcourseService;
import com.cinchapi.concourse.store.perm.Database;
import com.cinchapi.concourse.store.temp.CommitLog;
import com.google.common.collect.Sets;

/**
 * <p>
 * Concourse is a schemaless database that is designed for applications that
 * have large amounts of sparse data in read and write heavy environments.
 * Concourse comes with automatic indexing, data versioning and support for
 * transactions.
 * 
 * <h2>Intent</h2>
 * Concourse aims to be a service that is easy for developers to deploy, access
 * and scale with minimal tuning, while also being highly optimized for fast
 * read/write operations. With Concourse there is no need to declare any
 * structure up front--no schema, no tables, no keyspaces, no indexes, no column
 * families, etc. Instead, you simply write any value <sup>1</sup> to any column
 * in any row at any time and the necessary structure is created for you on
 * demand. <br>
 * <br>
 * <sup>1</sup> - You cannot write a duplicate value to a cell.
 * <h2>Data Model</h2>
 * Concourse is a big matrix where each row represents a single, canonical
 * object record and each column represents an attribute in the data universe.
 * The intersection of a row and column--a cell--specifies the
 * <strong>values</strong><sup>2</sup> for the relevant attribute on the
 * relevant object.
 * <ul>
 * <li>Each value is versioned by timestamp.<sup>3</sup></li>
 * <li>Each cell sorts its values by timestamp in descending order and also
 * maintains a historical log of revisions.</li>
 * <li>An index of rows sorted by id, an index of columns sorted logically, and
 * a full text index of values are all maintained for optimal reads.</li>
 * </ul>
 * <sup>2</sup> - A cell can simultaneously hold many distinct values and
 * multiple types.<br>
 * <sup>3</sup> - Each value is guaranteed to have a unique timestamp.
 * 
 * <h2>Graph Model</h2>
 * As a matrix, Concourse naturally represents which nodes on a graph are
 * connected to which other nodes: each row and each column corresponds to a
 * node and each value in the cell formed at the intersection of the row and
 * column corresponds to an edge between the corresponding row node and column
 * node on the graph--an edge whose weight is equal to the value.
 * 
 * <h2>Data Processing</h2>
 * Concourse is designed for highly efficient reads and writes.
 * <h4>Writes</h4>
 * Initially all data is written to an append only commit log. The commit log
 * exists in memory and is flushed to disk periodically.
 * <h4>Reads</h4>
 * For reads, Concourse queries its internal database and commit log for the
 * appropriate result sets according to their respective views of the data. The
 * two results sets are resolved by taking their XOR (see
 * {@link Sets#symmetricDifference(Set, Set)} before being returned.
 * 
 * <h2>Additional Notes</h2>
 * <ul>
 * <li>
 * In its present implementation, Concourse can only increase in size (even if
 * data is removed) because every single revision is tracked. In the future,
 * functionality to purge history and therefore reduce the size of the database
 * <em>should</em> be added.</li>
 * </ul>
 * </p>
 * 
 * 
 * @author jnelson
 */
public class Concourse extends ConcourseService {

	/**
	 * Close Concourse.
	 * 
	 * @param concourse
	 */
	public static void close(Concourse concourse) {
		concourse.close();
	}

	/**
	 * Return Concourse based out of the {@link DEFAULT_HOME} directory.
	 * 
	 * @return Concourse
	 */
	public static Concourse withDefaultHome() {
		return Concourse.withHomeAt(DEFAULT_HOME);
	}

	/**
	 * Return Concourse based out of the directory at {@code location}.
	 * 
	 * @param home
	 * @return Concourse
	 */
	public static Concourse withHomeAt(String home) {
		CommitLog commitLog;
		Database database;

		File commitLogFile = new File(home + File.separator + "commitlog");
		boolean commitLogIsFlushed;
		if(commitLogFile.exists()) {
			commitLog = CommitLog.fromFile(commitLogFile.getAbsolutePath());
			commitLogIsFlushed = false;
		}
		else {
			commitLog = CommitLog.newInstance(commitLogFile.getAbsolutePath(),
					CommitLog.DEFAULT_SIZE_IN_BYTES);
			commitLogIsFlushed = true;
		}

		File databaseDir = new File(home + File.separator + "db");
		databaseDir.mkdirs();
		database = Database.inDir(databaseDir.getAbsolutePath());

		return new Concourse(commitLog, database, commitLogIsFlushed);
	}

	/**
	 * The default HOME directory for Concourse.
	 */
	public static final String DEFAULT_HOME = System.getProperty("user.home")
			+ File.separator + "concourse";
	private static final Logger log = LoggerFactory.getLogger(Concourse.class);

	private CommitLog commitLog;
	private Database database;
	private boolean flushed;

	private Concourse(CommitLog commitLog, Database database, boolean flushed) {
		this.commitLog = commitLog;
		this.database = database;
		this.flushed = flushed;
	}

	/**
	 * Close Concourse.
	 */
	public synchronized void close() {
		if(!flushed) {
			log.warn("The commitlog has not been flushed");
		}
	}

	/**
	 * <p>
	 * Force an immediate flush of the {@code commitLog} to the permanent
	 * {@code database}.
	 * </p>
	 * <p>
	 * <strong>Note:</strong> This method is synchronized and will block so that
	 * reads/writes do not occur simultaneously.
	 * </p>
	 */
	public synchronized void flush() {
		database.flush(commitLog);
		flushed = true;
	}

	@Override
	protected boolean addSpi(long row, String column, Object value) {
		flush(false);
		ExecutorService executor = Executors.newCachedThreadPool();

		Future<Boolean> clr = executor.submit(Threads.add(commitLog, row,
				column, value));
		try {
			flushed = false;
			return clr.get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected Set<String> describeSpi(long row) {
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Set<String>> dbr = executor.submit(Threads.describe(database,
				row));
		Future<Set<String>> clr = executor.submit(Threads.describe(commitLog,
				row));
		try {
			return Sets.symmetricDifference(dbr.get(), clr.get());
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Sets.newHashSet();
		}
	}

	@Override
	protected boolean existsSpi(long row, String column, Object value) {
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> dbr = executor.submit(Threads.exists(database, row,
				column, value));
		Future<Boolean> clr = executor.submit(Threads.exists(commitLog, row,
				column, value));
		try {
			return dbr.get() ^ clr.get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Flush the {@code commitLog} to the {@code database} iff it is full or
	 * {@code force} is {@code true}.
	 * 
	 * @param force
	 */
	protected void flush(boolean force) {
		if(force || commitLog.isFull()) {
			flush();
		}
	}

	@Override
	protected Set<Object> getSpi(long row, String column) {
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Set<Object>> dbr = executor.submit(Threads.get(database, row,
				column));
		Future<Set<Object>> clr = executor.submit(Threads.get(commitLog, row,
				column));
		try {
			return Sets.symmetricDifference(dbr.get(), clr.get());
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Sets.newHashSet();
		}
	}

	@Override
	protected boolean removeSpi(long row, String column, Object value) {
		flush(false);
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> clr = executor.submit(Threads.remove(commitLog, row,
				column, value));
		try {
			flushed = false;
			return clr.get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected Set<Long> selectSpi(String column, SelectOperator operator,
			Object... values) {
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Set<Long>> dbr = executor.submit(Threads.select(database,
				column, operator, values));
		Future<Set<Long>> clr = executor.submit(Threads.select(commitLog,
				column, operator, values));
		try {
			return Sets.symmetricDifference(dbr.get(), clr.get());
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return Sets.newHashSet();
		}
	}

	/**
	 * Contains various callables and runnables to enable concurrent operations
	 * within {@link Concourse}.
	 * 
	 * @author Jeff Nelson
	 */
	public static class Threads {

		/**
		 * Execute {@link ConcourseService#add(long, String, Object)}.
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
		 * Execute {@link ConcourseService#exists(long, String, Object)}.
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
		 * Execute {@link ConcourseService#get(long, String)}.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @return the method return value
		 */
		public static Get get(ConcourseService service, long row, String column) {
			return new Get(service, row, column);
		}

		/**
		 * Execute {@link ConcourseService#remove(long, String, Object)}.
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

		/**
		 * Execute
		 * {@link ConcourseService#select(String, SelectOperator, Object...)} .
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 * @return the method return value
		 */
		public static Select select(ConcourseService service, String column,
				SelectOperator operator, Object... values) {
			return new Select(service, column, operator, values);
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
		 * Execute the {@link ConcourseService#add(long, String, Object)}
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
				return service.add(row, column, value);
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
		 * Execute the {@link ConcourseService#exists(long, String, Object)}
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
				return service.exists(row, column, value);
			}

		}

		/**
		 * Execute the {@link ConcourseService#get(long)} method.
		 * 
		 * @author jnelson
		 */
		private static final class Get extends
				AbstractConcourseServiceCallable<Set<Object>> {

			private final long row;
			private final String column;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param row
			 * @param column
			 */
			public Get(ConcourseService service, long row, String column) {
				super(service);
				this.row = row;
				this.column = column;
			}

			@Override
			public Set<Object> call() throws Exception {
				return service.get(row, column);
			}

		}

		/**
		 * Execute the {@link ConcourseService#remove(long, String, Object)}
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
				return service.remove(row, column, value);
			}

		}

		/**
		 * Execute the
		 * {@link ConcourseService#select(String, com.cinchapi.concourse.store.api.Queryable.SelectOperator, Object...)}
		 * method.
		 * 
		 * @author jnelson
		 */
		private static final class Select extends
				AbstractConcourseServiceCallable<Set<Long>> {

			private final String column;
			private final SelectOperator operator;
			private final Object[] values;

			/**
			 * Construct a new instance.
			 * 
			 * @param service
			 * @param column
			 * @param operator
			 * @param values
			 */
			public Select(ConcourseService service, String column,
					SelectOperator operator, Object... values) {
				super(service);
				this.column = column;
				this.operator = operator;
				this.values = values;
			}

			@Override
			public Set<Long> call() throws Exception {
				return service.select(column, operator, values);
			}

		}
	}
}

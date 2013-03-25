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
package com.cinchapi.concourse.db;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * TODO add some comments
 * 
 * @author jnelson
 */
class PrimaryStorage extends FlushingService {

	public static PrimaryStorage inDir(String directory) {
		return new PrimaryStorage(directory);
	}

	private static final String ROW_HOME = "rows";
	private static final String COLUMN_HOME = "cols";
	private static final Logger log = LoggerFactory.getLogger(PrimaryStorage.class);

	private static final int EXECUTOR_SHUTDOWN_WAIT_IN_SECS = 60;
	private final String home;

	private final ExecutorService executor = Executors.newCachedThreadPool();

	private PrimaryStorage(String home) {
		this.home = home;
		log.info("The Database has started.");
	}

	@Override
	public Set<Object> fetch(String column, long row) {
		Set<Object> result = Sets.newHashSet();
		Cell cell = use(row).fetch(column);
		if(cell != null) {
			Iterable<Value> values = cell.getValues();
			for (Value value : values) {
				result.add(value.getQuantity());
			}
		}
		return result;
	}

	@Override
	public synchronized void flush(FlushableService commitLog) {
		Iterator<Write> flusher = commitLog.flusher();
		while (flusher.hasNext()) {
			Write commit = flusher.next();
			
			// The commits do not specify the associated operation, so the
			// easiest thing is to attempt to add first and then attempt to
			// remove if that fails
			try {
				flushAdd(commit.getColumn(), commit.getValue(), commit.getRow());
			}
			catch (IllegalStateException e) {
				flushRemove(commit.getColumn(), commit.getValue(),
						commit.getRow());
			}
		}
	}

	@Override
	public synchronized void shutdown() {
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
		log.info("The Database has shutdown gracefully.");

	}

	@Override
	public long sizeOf(long row) {
		return use(row).size();
	}

	@Override
	protected Set<String> describeSpi(long row) {
		return use(row).describe();
	}

	@Override
	protected boolean existsSpi(String column, Object value, long row) {
		return use(row).exists(column, Value.notForStorage(value));
	}

	@Override
	protected Set<Object> fetchSpi(String column, long timestamp, long row) {
		Set<Object> result = Sets.newHashSet();
		Cell cell = use(row).fetch(column);
		if(cell != null) {
			Iterable<Value> values = cell.getValues(timestamp);
			for (Value value : values) {
				result.add(value.getQuantity());
			}
		}
		return result;
	}

	@Override
	protected Set<Long> querySpi(String column, Operator operator,
			Object... values) {
		return use(column).query(operator, values);
	}

	@Override
	protected long sizeOfSpi(String column, Long row) {
		return use(row).size(column);
	}

	/**
	 * Add {@code value} to {@code row}:{@code column}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 */
	private void flushAdd(final String column, final Value value, final Key row) {
		final Row r = use(row);
		final Column c = use(column);

		Future<?> f1 = executor.submit(new Runnable() {
			@Override
			public void run() {
				r.add(column, value);
			}
		});

		Future<?> f2 = executor.submit(new Runnable() {
			@Override
			public void run() {
				c.add(row, value);
			}
		});

		while (!f1.isDone() || !f2.isDone()) {
			continue;
		}

		r.fsync();
		c.fsync();
	}

	/**
	 * Remove {@code value} from {@code row}:{@code column}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 */
	private void flushRemove(final String column, final Value value,
			final Key row) {
		final Row r = use(row);
		final Column c = use(column);

		Future<?> f1 = executor.submit(new Runnable() {
			@Override
			public void run() {
				r.remove(column, value);
			}
		});

		Future<?> f2 = executor.submit(new Runnable() {
			@Override
			public void run() {
				c.remove(row, value);
			}
		});

		while (!f1.isDone() || !f2.isDone()) {
			continue;
		}

		r.fsync();
		c.fsync();
	}

	/**
	 * Use the {@link Row} identified by the {@code key}.
	 * 
	 * @param key
	 * @return the row
	 */
	private Row use(Key key) {
		return Row.identifiedBy(key, home + File.separator + ROW_HOME);
	}

	/**
	 * Use the {@link Row} identified by the {@code key}.
	 * 
	 * @param key
	 * @return the row
	 */
	private Row use(long key) {
		return use(Key.fromLong(key));
	}

	/**
	 * Use the {@link Column} identified by the {@code name}.
	 * 
	 * @param name
	 * @return the column
	 */
	private Column use(String name) {
		return Column.identifiedBy(name, home + File.separator + COLUMN_HOME);
	}

}

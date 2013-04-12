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

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.google.common.collect.Sets;

/**
 * <p>
 * A heavyweight {@link ConcourseService} that is maintained entirely on disk
 * and loads components into memory as necessary.
 * </p>
 * <p>
 * The service stores both rows<sup>1</sup> and columns<sup>2</sup> distinctly.
 * Since the service is disk based, it has no limits on storage beyond those
 * imposed by the underlying filesystem. Once loaded into memory, data will take
 * up more space than it does on disk. A cache is implemented so that frequently
 * used components can be deserialized more quickly and less frequently used
 * components will be evicted from memory before throwing an OOM.
 * </p>
 * <sup>1</sup> - A red-black tree is used to index <em>every</em> column for
 * logarithmic query operations. Additionally, a hashmap is used to index
 * <em>every</em> cell in <em>every</em> row so that exists, fetch and describe
 * operations take amortized constant time.<br >
 * <sup>2</sup> - Individual row and column indexes have size limitations.
 * </p>
 * 
 * @author jnelson
 */
class Storage extends FlushingService {

	/**
	 * Return {@link Storage} based in {@code dir}.
	 * 
	 * @param dir
	 * @return storage
	 */
	public static Storage in(String dir) {
		return new Storage(dir);
	}

	private static final int EXECUTOR_SHUTDOWN_WAIT_IN_SECS = 60;
	private static final String ROW_HOME = "cr";
	private static final String COLUMN_HOME = "cc";
	private static final Logger log = LoggerFactory.getLogger(Storage.class);

	private final String home;
	private final ExecutorService executor = Executors.newCachedThreadPool();

	/**
	 * Construct a new instance.
	 * 
	 * @param home
	 */
	private Storage(String home) {
		this.home = home;
		log.info("The storage is ready.");
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
	public synchronized void shutdown() {
		executor.shutdown();
		try {
			if(!executor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_IN_SECS,
					TimeUnit.SECONDS)) {
				List<Runnable> tasks = executor.shutdownNow();
				log.error("The storage could not properly shutdown. "
						+ "The following tasks were dropped: {}", tasks);
			}
		}
		catch (InterruptedException e) {
			log.error("An error occured while shutting down the {}: {}", this
					.getClass().getName(), e);
		}
		log.info("The storage has shutdown gracefully.");

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

	@Override
	synchronized void flush(FlushableService buffer) {
		WriteFlusher flusher = buffer.flusher();
		while (flusher.hasNext()) {
			Write write = flusher.next();
			try {
				flush(write);
			}
			catch (Exception e) {
				log.error("An error occured while trying to flush "
						+ "write {} to storage: {}", write, e);
				throw new ConcourseRuntimeException(e);
			}
			flusher.ack();
		}
	}

	/**
	 * Flush the {@code write} immediately.
	 * 
	 * @param write
	 */
	synchronized void flush(Write write) {
		String column = write.getColumn();
		Value value = write.getValue();
		Key row = write.getRow();
		WriteType type = write.getType();
		if(type == WriteType.ADD) {
			flushAdd(column, value, row);
		}
		else {
			flushRemove(column, value, row);
		}
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

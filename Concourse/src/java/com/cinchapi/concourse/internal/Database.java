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
package com.cinchapi.concourse.internal;

import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
class Database extends ConcourseService {

	private static final String ROW_HOME = "rows";
	private static final String COLUMN_HOME = "cols";
	private static final Logger log = LoggerFactory.getLogger(Database.class);
	
	private final String home;
	private final ExecutorService executor = Executors.newCachedThreadPool();

	public static Database inDir(String directory) {
		return new Database(directory);
	}

	private Database(String home) {
		this.home = home;
	}

	/**
	 * Flush the contents of the {@code commitLog} to the database.
	 * 
	 * @param commitLog
	 */
	public synchronized void flush(CommitLog commitLog) {
		Iterator<Commit> flusher = commitLog.flusher();
		while (flusher.hasNext()) {
			Commit commit = flusher.next();
			String column = commit.getColumn();
			Value value = commit.getValue();
			Row row = use(commit.getRow());
			try {
				row.add(column, value);
			}
			catch (IllegalStateException e) {
				row.remove(column, value);
			}
			row.fsync();
		}
	}

	@Override
	public synchronized void shutdown() {
		log.info("Successfully shutdown the Database.");

	}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		Value v = Value.forStorage(value);
		use(row).add(column, v);
		use(column).add(Key.fromLong(row), v);
		return true;
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
		use(column).query(operator, values);
		return Sets.newHashSet();
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		Value v = Value.forStorage(value);
		use(row).remove(column, v);
		use(column).remove(Key.fromLong(row), v);
		return true;
	}

	@Override
	public long sizeOf(long row) {
		return use(row).size();
	}

	@Override
	protected long sizeOfSpi(String column, Long row) {
		return use(row).size(column);
	}

	private Row use(long row) {
		return use(Key.fromLong(row));
	}

	private Row use(Key row) {
		return Row.identifiedBy(row, home + File.separator + ROW_HOME);
	}
	
	private Column use(String name){
		return Column.identifiedBy(name, home + File.separator + COLUMN_HOME);
	}

}

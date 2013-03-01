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

import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.search.Indexer;
import com.cinchapi.concourse.util.Time;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;

/**
 * A collection of {@link Cell} where each is mapped from a column name. A
 * single row cannot be larger than 2GB.
 * 
 * @author jnelson
 */
public final class Row implements Locatable{

	/**
	 * Return an empty row identified by {@code key}.
	 * 
	 * @param key
	 * @return the row.
	 */
	public static Row createEmpty(Key key) {
		return new Row(key);
	}

	private static final int defaultInitialCapacity = 4;
	private static final int defaultConcurencyLevel = 4;
	private static final Indexer indexer = null; // TODO get an indexer
	private static final Logger log = LoggerFactory.getLogger(Row.class);
	private static final int maxSizeInBytes = 2147483647; // 2GB
	private static final int initialSizeInBytes = 2 * (Integer.SIZE / 8)
			+ (Long.SIZE / 8);
	private final Key key;
	private int count = 0;
	private int size = initialSizeInBytes;
	private final ConcurrentMap<String, Cell> cells = new MapMaker()
			.initialCapacity(defaultInitialCapacity)
			.concurrencyLevel(defaultConcurencyLevel).makeMap(); // this behaves
																	// like a
																	// ConcurrentHashMap

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 */
	protected Row(Key key) {
		this.key = key;
	}

	/**
	 * Add {@code value} to the cell under {@code column}.
	 * 
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is added.
	 */
	public boolean add(String column, Value value) {
		Preconditions.checkArgument(!column.contains(" "),
				"'%s' is an invalid column name because it contains spaces",
				column);
		Preconditions.checkArgument(Strings.isNullOrEmpty(column),
				"column cannot be empty");
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(value.isForStorage(),
				"'%s' is notForStorage and cannot be added", value);
		Preconditions.checkArgument(size + value.size() < maxSizeInBytes,
				"%s is too big to fit in this row", value);

		Cell cell;
		if(cells.containsKey(column)) {
			cell = cells.get(column);
		}
		else {
			cell = Cell.createEmpty();
			cells.put(column, cell);
			setCount(count + 1);
			setSize(size + value.size());
		}
		if(cell.add(value)) {
			ExecutorService executor = Executors.newCachedThreadPool();
			executor.execute(new IndexAdder(key, column, value));
			executor.shutdown();
			try {
				// try to avoid race conditions where certain threads finish
				// before others and prevent some of the indexing from occurring
				executor.awaitTermination(1, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				log.info("{}", e);
			}
			return true;
		}
		return false;

	}

	/**
	 * Return the columns that have non-empty cells.
	 * 
	 * @return
	 */
	public Set<String> columnSet() {
		return columnSet(Time.now());
	}

	/**
	 * Return the columns that had non-empty cell right before {@code at}.
	 * 
	 * @param at
	 * @return the columns.
	 */
	public Set<String> columnSet(long at) {
		Iterator<Entry<String, Cell>> it = cells.entrySet().iterator();
		Set<String> columns = Sets.newHashSet();
		while (it.hasNext()) {
			Entry<String, Cell> entry = it.next();
			if(!entry.getValue().getValues(at).isEmpty()) {
				columns.add(entry.getKey());
			}
		}
		return columns;
	}

	/**
	 * Return all the columns that have cells (both empty and non-empty).
	 * 
	 * @return the columns.
	 */
	public Set<String> columnSetAll() {
		return cells.keySet();
	}

	/**
	 * Return the number of non-empty cells.
	 * 
	 * @return the count.
	 */
	public int count() {
		return count;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Row) {
			Row other = (Row) obj;
			return Objects.equal(this.key, other.key)
					&& Objects.equal(this.cells, other.cells)
					&& Objects.equal(this.count, other.count);
		}
		return false;
	}

	/**
	 * Get the cell under {@code column}, even if the cell is empty.
	 * 
	 * @param column
	 * @return the cell.
	 */
	@Nullable
	public Cell get(String column) {
		return cells.get(column);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(key, cells, count);
	}

	/**
	 * Return {@code true} if the row is empty.
	 * 
	 * @return {@code true} if there are - non-empty cells in the row.
	 */
	public boolean isEmpty() {
		return cells.size() == 0;
	}

	/**
	 * Return the {@code key} that identifies this row.
	 * 
	 * @return the {@code key}.
	 */
	public Key key() {
		return key;
	}

	/**
	 * Remove {@code value} from the the cell under {@code column}.
	 * 
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is removed.
	 */
	public boolean remove(String column, Value value) {
		if(cells.containsKey(column)) {
			Cell cell = cells.get(column);
			if(cell.remove(value)) {
				if(cell.isEmpty()) {
					setCount(count - 1);
				}
				setSize(size - value.size());
				ExecutorService executor = Executors.newCachedThreadPool();
				executor.execute(new IndexRemover(key, column, value));
				executor.shutdown();
				try {
					// try to avoid race conditions where certain threads finish
					// before others and prevent some of the deindexing from
					// occurring
					executor.awaitTermination(1, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					log.info("{}", e);
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Remove any existing values from the cell under {@code column} and
	 * add {@code value} to the cell.
	 * 
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is set.
	 */
	public boolean set(String column, Value value) {
		if(cells.containsKey(column)) {
			cells.get(column).removeAll(); // Calling the Cell#removeAll()
											// method directly bypasses the
											// local remove() method, which
											// adjusts the #size appropriately,
											// but this is OK here because
											// this operation has a net neutral
											// affect on the #size
		}
		return add(column, value);
	}

	@Override
	public String toString() {
		return key + ": " + cells;
	}
	
	

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Locatable#getLocator()
	 */
	@Override
	public String getLocator() {
		return key.toString();
	}

	/**
	 * Thread-safe method to set {@link #count}.
	 * 
	 * @param size
	 */
	private synchronized void setCount(int size) {
		this.count = size;
	}

	/**
	 * Thread safe method to set {@link #size}.
	 * 
	 * @param size
	 */
	private synchronized void setSize(int size) {
		this.size = size;
	}

	/**
	 * Base class for index modifiers.
	 */
	private abstract class AbstractIndexModifier implements Runnable {

		protected final Key row;
		protected final String column;
		protected final Value value;

		/**
		 * Construct a new instance.
		 * 
		 * @param key
		 * @param column
		 * @param value
		 */
		AbstractIndexModifier(Key key, String column, Value value) {
			this.row = key;
			this.column = column;
			this.value = value;
		}
	}

	/**
	 * {@link Runnable} that adds indexes.
	 */
	private class IndexAdder extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param key
		 * @param column
		 * @param value
		 */
		IndexAdder(Key key, String column, Value value) {
			super(key, column, value);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			indexer.index(row, column, value);

		}

	}

	/**
	 * {@link Runnable} that removes indexes.
	 */
	private class IndexRemover extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param key
		 * @param column
		 * @param value
		 */
		IndexRemover(Key key, String column, Value value) {
			super(key, column, value);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			indexer.deindex(row, column, value);

		}

	}

}

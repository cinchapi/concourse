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
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Time;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * An append log that tracks revisions to a {@link Cell}. Whenever a value is
 * added or removed, an entry is made in the cell history. If a value
 * appears (as defined by {@link Value#equals(Object)}) in the history an
 * odd number of times then it is considered to exist, otherwise it does not
 * exist.
 * 
 * @author jnelson
 */
@Immutable
public final class CellHistory {

	/**
	 * Return an empty cell history.
	 * 
	 * @return the cell history.
	 */
	public static CellHistory createEmpty() {
		return new CellHistory();
	}

	private static final Logger log = LoggerFactory
			.getLogger(CellHistory.class);

	/**
	 * Mapping of timestamp to value, sorted in ascending order.
	 */
	private final SortedMap<Long, Value> history = new ConcurrentSkipListMap<Long, Value>();

	/**
	 * Construct a new instance.
	 */
	private CellHistory() {}

	/**
	 * Count the number of times that <code>value</code> appears in the history.
	 * 
	 * @param value
	 * @return the number of appearances.
	 */
	public int count(Value value) {
		return count(value, Time.now());
	}

	/**
	 * Count the number of times that <code>value</code> appears in the history
	 * with a timestamp that is less than <code>before</code>.
	 * 
	 * @param value
	 * @param before
	 * @return the number of appearances
	 */
	public int count(Value value, long before) {
		int count = 0;
		synchronized (history) { // the iterator for a concurrent collection is
									// weakly consistent, so I'm grabbing a lock
									// to block updates while I get the count

			final Iterator<Entry<Long, Value>> window = history
					.headMap(before).entrySet().iterator();
			while (window.hasNext()) {
				count += value.equals(window.next().getValue()) ? 1 : 0;
			}
		}
		return count;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CellHistory) {
			CellHistory other = (CellHistory) obj;
			return Objects.equals(this.history, other.history);
		}
		return false;
	}

	/**
	 * Return <code>true</code> if <code>value</code> currently exists in the
	 * cell (meaning there is an odd number of appearances for
	 * <code>value</code> in the history).
	 * 
	 * @param value
	 * @return <code>true</code> if <code>value</code> exists.
	 */
	public boolean exists(Value value) {
		return exists(value, Time.now());
	}

	/**
	 * Return <code>true</code> if <code>value</code> existed in the cell prior
	 * to <code>before</code> (meaning there is an odd number of appearances for
	 * <code>value</code> in the history).
	 * 
	 * @param value
	 * @param before
	 * @return <code>true</code> if <code>value</code> existed.
	 */
	public boolean exists(Value value, long before) {
		return Numbers.isOdd(count(value, before));
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(history);
	}

	/**
	 * Return <code>true</code> if <code>value</code> can be added to the cell
	 * because it does not exist.
	 * 
	 * @param value
	 * @return <code>true</code> if <code>value</code> does not exist.
	 */
	public boolean isPossibleToAdd(Value value) {
		return !exists(value);
	}

	/**
	 * Return <code>true</code> if <code>value</code> can be removed from the
	 * cell because it does exist.
	 * 
	 * @param value
	 * @return <code>true</code> if <code>value</code> exists.
	 */
	public boolean isPossibleToRemove(Value value) {
		return exists(value);
	}

	/**
	 * Log a revision pertaining to <code>value</code>. The calling method
	 * should first check if <code>value</code> is eligible for the appropriate
	 * revision.
	 * 
	 * @param value
	 * @return <code>true</code> if the revision is record.
	 * @see {@link #isPossibleToAdd(Value)}
	 * @see {@link #isPossibleToRemove(Value)}
	 */
	public boolean log(Value value) {
		Preconditions.checkState(isPossibleToAdd(value) && value.isForStorage()
				|| isPossibleToRemove(value) && value.isNotForStorage(),
				"Attempting to log an illegal revision for '%s'", value);

		long timestamp = value.isForStorage() ? value.getTimestamp() : Time
				.now(); // remove revisions will use a notForStorage value
						// in which case the current timestamp should be
						// associated

		assert !history.containsKey(timestamp) : "A timestamp conflict occured in "
				+ "the history log. This should not happen because every revision"
				+ "and value should have a unique timestamp";

		history.put(timestamp, value);

		if(log.isDebugEnabled()) {
			log.debug("Logged {} revision at {} for '{}'",
					value.isForStorage() ? "ADD" : "REMOVE", timestamp, value);
		}

		return history.get(timestamp) == value;
	}

	/**
	 * Return a view of the cell at <code>to</code>. Only revisions at
	 * timestamps strictly less than <code>to</code> are returned.
	 * 
	 * @param to
	 * @return the state of the cell at <code>to</code>.
	 */
	public SortedSet<Value> rewind(long to) {
		SortedSet<Value> set = Sets.newTreeSet();
		synchronized (history) { // the iterator for a concurrent collection is
									// weakly consistent, so I'm grabbing a lock
									// to block updates while rewind

			final Iterator<Entry<Long, Value>> window = history.headMap(to)
					.entrySet().iterator();
			while (window.hasNext()) {
				Value value = window.next().getValue();
				if(set.contains(value)) {// if value is already contained, then
											// this means i've encountered a
											// remove revision and this
											// comparison and removal will work
											// because the value is guaranteed to
											// be notForStorage

					assert value.isNotForStorage() : "An even numbered value "
							+ "revision had a non-nil timestamp which means that "
							+ "it was not logged during a removal. All even numbered "
							+ "value revisions must be logged during a removal and "
							+ "therefore not have a nil timestamp";

					set.remove(value);
				}
				else {
					set.add(value);
				}
			}
		}
		return set;
	}

	@Override
	public String toString() {
		return history.toString();
	}

}

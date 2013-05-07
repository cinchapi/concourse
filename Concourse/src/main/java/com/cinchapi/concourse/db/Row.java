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

import java.nio.ByteBuffer;
import java.util.Map;

import com.cinchapi.common.lock.Lock;
import com.google.common.collect.Maps;

/**
 * 
 * 
 * @author jnelson
 */
final class Row extends Store<ByteSizedString, Value> {

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	protected Row(Key key) {
		super(key);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param filename
	 * @param bytes
	 */
	public Row(String filename) {
		super(filename);
	}

	private static final RowCell mock = Bucket.mock(RowCell.class);

	// @Override from {@link Store}
	protected static final String FILE_NAME_EXT = "ccr";

	@Override
	protected Bucket<ColumnName, Value> getMockBucket() {
		return mock;
	}

	@Override
	protected Bucket<ColumnName, Value> getNewBucket(ColumnName key) {
		return RowCell.newInstance(key.toString()); // TODO make a newInstance
													// method that takes a
													// columnname
	}

	@Override
	protected Bucket<ColumnName, Value> getBucketFromByteSequence(
			ByteBuffer bytes) {
		return RowCell.fromByteSequence(bytes);
	}

	Lock readLock(ColumnName column) {
		return ((RowCell) get(column)).readLock();
	}

	Lock writeLock(ColumnName column) {
		return ((RowCell) get(column)).writeLock();
	}

	@Override
	protected Map<ColumnName, Bucket<ColumnName, Value>> getNewBuckets(
			int expectedSize) {
		return Maps.newHashMapWithExpectedSize(expectedSize);
	}

}

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
package com.cinchapi.concourse;

import com.cinchapi.concourse.store.temp.CommitLog;
import com.cinchapi.concourse.store.temp.HeapDatabase;

/**
 * 
 * 
 * @author jnelson
 */
public class ConcourseServiceProvider {

	public static final int HEAP_DATABASE_DEFAULT_EXPECTED_CAPACITY = 100;
	public static final String COMMIT_LOG_DEFAULT_LOCATION = "test/commitlog";
	public static final int COMMIT_LOG_DEFAULT_SIZE_IN_BYTES = 1024 * 1024;

	public static HeapDatabase provideHeapDatabase() {
		return ConcourseServiceProvider
				.provideHeapDatabase(HEAP_DATABASE_DEFAULT_EXPECTED_CAPACITY);
	}

	public static HeapDatabase provideHeapDatabase(int expectedCapacity) {
		return HeapDatabase.newInstancewithExpectedCapacity(expectedCapacity);
	}

	public static CommitLog provideNewCommitLog() {
		return ConcourseServiceProvider.provideNewCommitLog(
				COMMIT_LOG_DEFAULT_LOCATION, COMMIT_LOG_DEFAULT_SIZE_IN_BYTES);

	}

	public static CommitLog provideNewCommitLog(String location, int size) {
		return CommitLog.newInstance(location, size);
	}
}

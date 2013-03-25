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

import com.cinchapi.concourse.config.ConcourseConfiguration;

/**
 * 
 * 
 * @author jnelson
 */
public class ConcourseServiceProvider {

	public static final int VOLATILE_STORAGE_DEFAULT_EXPECTED_CAPACITY = 100000;
	public static final String COMMIT_LOG_DEFAULT_LOCATION = "test/commitlog";
	public static final String CONCOURSE_PREFS = "concourse.prefs";

	public static VolatileStorage provideVolatileStorage() {
		return ConcourseServiceProvider
				.provideVolatileStorage(VOLATILE_STORAGE_DEFAULT_EXPECTED_CAPACITY);
	}

	public static VolatileStorage provideVolatileStorage(int expectedCapacity) {
		return VolatileStorage
				.newInstancewithExpectedCapacity(expectedCapacity);
	}

	public static CommitLog provideNewCommitLog() {
		return ConcourseServiceProvider.provideNewCommitLog(
				COMMIT_LOG_DEFAULT_LOCATION);

	}

	public static CommitLog provideNewCommitLog(String location) {
		return CommitLog.newInstance(location, ConcourseConfiguration.fromFile(CONCOURSE_PREFS));
	}
}

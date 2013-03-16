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
package com.cinchapi.concourse.store;

import com.cinchapi.concourse.services.ConcourseServiceBenchmark;
import com.cinchapi.concourse.services.ConcourseServiceProvider;
import com.cinchapi.concourse.store.CommitLog;

/**
 * 
 * 
 * @author jnelson
 */
public class CommitLogBenchmark extends ConcourseServiceBenchmark {

	private static final String location = "test/output/benchmark/commitlog";
	private static final int size = 1024 * 1024 * 100;

	@Override
	protected CommitLog getService() {
		return ConcourseServiceProvider.provideNewCommitLog(location, size);
	}

}

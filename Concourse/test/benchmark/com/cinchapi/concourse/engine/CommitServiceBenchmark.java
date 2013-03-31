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
package com.cinchapi.concourse.engine;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.SizeUnit;
import com.cinchapi.concourse.engine.ConcourseService;

/**
 * 
 * 
 * @author jnelson
 */
public abstract class CommitServiceBenchmark extends ConcourseServiceBenchmark {

	@Test
	public void testTimeToFillUp() {
		long count = 0;
		double targetSize = SizeUnit.BYTES.convertFrom(1, SizeUnit.GiB);
		log("The target size is {} {}", targetSize, SizeUnit.BYTES);
		ConcourseService service = getService();
		watch.start();
		long size = 0;
		while (size < targetSize) {
			service.add(randomColumnName(), randomObject(), randomLong());
			count++;
			size = service.sizeOf();
			log("{} MiB written",
					SizeUnit.MiB.convertFrom(size, SizeUnit.BYTES));
		}
		TimeUnit unit = TimeUnit.SECONDS;
		long elapsed = watch.stop(unit);
		log("Wrote {} values in {} {}", count, elapsed, unit);
	}

}

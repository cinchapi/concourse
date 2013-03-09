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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.concourse.store.api.ConcourseService;

/**
 * Base benchmark tests for the {@link ConcourseService} interface.
 * 
 * @author jnelson
 */
public abstract class ConcourseServiceBenchmark extends BaseBenchmark {

	/**
	 * Return the service.
	 * 
	 * @return the service.
	 */
	protected abstract ConcourseService getService();

	@Test
	public void testAdd() {
		int target = 100000;
		TimeUnit unit = TimeUnit.SECONDS;

		log("Running benchmark for add() with a target of {} values...",
				format.format(target));

		log("Generating data...");
		long[] rows = new long[target];
		String[] columns = new String[target];
		Object[] values = new Object[target];

		for (int count = 0; count < target; count++) {
			rows[count] = randomLong();
			columns[count] = randomStringNoSpaces();
			values[count] = randomObject();
		}

		ConcourseService service = getService();

		log("Writing data...");
		timer().start();
		for (int count = 0; count < target; count++) {
			service.add(rows[count], columns[count], values[count]);
		}

		long elapsed = timer().stop(unit);
		log("Runtime for add() with a target of {} values: {} {}",
				format.format(target), format.format(elapsed), unit);
	}

	@Test
	public void testRemove() {
		ConcourseService service = getService();

		int initTarget = 300000;
		int removeTarget = 100000;
		TimeUnit unit = TimeUnit.SECONDS;

		log("Running benchmark for remove() with {} initial values and {} removed values",
				format.format(initTarget), format.format(removeTarget));

		log("Generating and writing data...");
		long[] rows = new long[initTarget];
		String[] columns = new String[initTarget];
		Object[] values = new Object[initTarget];

		for (int count = 0; count < initTarget; count++) {
			long row = randomLong();
			String column = randomStringNoSpaces();
			Object value = randomObject();

			rows[count] = row;
			columns[count] = column;
			values[count] = value;

			service.add(row, column, value);
		}

		log("Removing data...");
		timer().start();
		for (int count = 0; count < removeTarget; count++) {
			int index = getRandom().nextInt(initTarget);

			long row = rows[index];
			String column = columns[index];
			Object value = values[index];

			service.remove(row, column, value);
		}
		long elapsed = timer().stop(unit);
		log("Runtime for remove() with an initial target of {} and a removal target of {} values: {} {}",
				format.format(initTarget), format.format(removeTarget),
				format.format(elapsed), unit);
	}

	protected String randomStringNoSpaces() {
		return randomString().replace(" ", "");
	}

}

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
package com.cinchapi.concourse.services;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.concourse.BaseBenchmark;
import com.cinchapi.concourse.service.ConcourseService;
import com.cinchapi.concourse.service.QueryService.Operator;

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
			columns[count] = randomColumnName();
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
		log("Wrote a total of {} bytes", format.format(service.sizeOf()));
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
			String column = randomColumnName();
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

	@Test
	public void testQuery() {
		ConcourseService service = getService();

		// Tuning parameters
		int numValues = 500000;
		int numColumns = (int) Math.round(.1 * numValues); // a lower number
															// means more
															// values/column
		int numRows = (int) Math.round(.4 * numValues); // a lower number means
														// more values/row

		TimeUnit unit = TimeUnit.MICROSECONDS;

		log("Running benchmark for query() with {} values {} columns and {} rows",
				format.format(numValues), format.format(numColumns),
				format.format(numRows));

		log("Generating and writing data...");

		log("Generating initial data...");
		long[] rows = new long[numRows];
		for (int i = 0; i < rows.length; i++) {
			rows[i] = randomLong();
		}

		String[] columns = new String[numColumns];
		for (int i = 0; i < columns.length; i++) {
			columns[i] = randomColumnName();
		}
		Object[] values = new Object[numValues];
		for (int i = 0; i < values.length; i++) {
			values[i] = randomObject();
		}

		log("Writing initial data...");
		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			String column = columns[getRandom().nextInt(columns.length)];
			long row = rows[getRandom().nextInt(rows.length)];
			service.add(row, column, value);
		}

		String column;
		Object value;
		long elapsed;
		Set<Long> results;

		Operator[] operators = Operator.values();
		for (Operator operator : operators) {
			try {
				column = columns[getRandom().nextInt(columns.length)];
				value = values[getRandom().nextInt(values.length)];
				log("Performing QUERY with {} operator", operator);
				timer().start();
				if(operator == Operator.BETWEEN) {
					Object value2 = values[getRandom().nextInt(values.length)];
					results = service.query(column, operator, value, value2);
				}
				else {
					results = service.query(column, operator, value);
				}
				elapsed = timer().stop(unit);
				log("Runtime for query: {} {}", elapsed, unit);
				log("Results for query: {}", results);
				log(System.lineSeparator());
			}
			catch (UnsupportedOperationException e) {
				log("{}", e.getMessage());
				timer().stop();
				continue;
			}

		}

	}

}

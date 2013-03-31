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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.engine.ConcourseService;
import com.cinchapi.concourse.engine.QueryService.Operator;

/**
 * Base benchmark tests for the {@link ConcourseService} interface.
 * 
 * @author jnelson
 */
public abstract class ConcourseServiceBenchmark extends DbBaseBenchmark {

	private static final int DEFAULT_SERVICE_CAPACITY = 1000000;

	/**
	 * Return the service.
	 * 
	 * @return the service.
	 */
	protected abstract ConcourseService getService();

	/**
	 * Return a service that is guaranteed to have entries for each of the
	 * columns in {@code c}.
	 * 
	 * @param c
	 * @return the service
	 */
	protected ConcourseService getServiceWithColumns(String... c) {
		ConcourseService service = getService();

		int capacity = DEFAULT_SERVICE_CAPACITY;
		log("Creating a service with {} pre-generated values",
				format.format(capacity));

		String[] columns = new String[(int) Math.round(.1 * capacity)];
		Object[] values = new Object[capacity];
		long[] rows = new long[(int) Math.round(.4 * capacity)];

		log("Generating data for service...");
		for (int i = 0; i < capacity; i++) {
			if(i < columns.length) {
				columns[i] = i < c.length ? c[i] : randomColumnName();
			}
			if(i < values.length) {
				values[i] = randomObject();
			}
			if(i < rows.length) {
				rows[i] = randomLong();
			}
		}

		log("Writing data to service...");
		writeToService(service, columns, values, rows);

		return service;
	}

	/**
	 * Return a service that is guaranteed to have entries for each of the
	 * rows in {@code r}.
	 * 
	 * @param c
	 * @return the service
	 */
	protected ConcourseService getServiceWithRows(long... r) {
		ConcourseService service = getService();

		int capacity = DEFAULT_SERVICE_CAPACITY;
		log("Creating a service with {} pre-generated values",
				format.format(capacity));

		String[] columns = new String[(int) Math.round(.1 * capacity)];
		Object[] values = new Object[capacity];
		long[] rows = new long[(int) Math.round(.4 * capacity)];

		log("Generating data for service...");
		for (int i = 0; i < capacity; i++) {
			if(i < columns.length) {
				columns[i] = randomColumnName();
			}
			if(i < values.length) {
				values[i] = randomObject();
			}
			if(i < rows.length) {
				rows[i] = i < r.length ? r[i] : randomLong();
			}
		}

		log("Writing data to service...");
		writeToService(service, columns, values, rows);

		return service;
	}

	/**
	 * Write the specified values to the provided service as best as possible.
	 * There is no guarantee that every row or every column or every value
	 * will be written because elements are chosen from the arrays at random.
	 * 
	 * @param service
	 * @param columns
	 * @param values
	 * @param rows
	 */
	protected void writeToService(ConcourseService service, String[] columns,
			Object[] values, long[] rows) {
		int length = Numbers.max(columns.length, values.length, rows.length);
		for (int i = 0; i < length; i++) {
			String column = columns[rand.nextInt(columns.length)];
			Object value = values[rand.nextInt(values.length)];
			long row = rows[rand.nextInt(rows.length)];
			while (service.exists(column, value, row)) {
				column = columns[rand.nextInt(columns.length)];
				value = values[rand.nextInt(values.length)];
				row = rows[rand.nextInt(rows.length)];
			}
			service.add(column, value, row);
		}
	}

	@Test
	public void testAdd() {
		int target = 100000;
		TimeUnit unit = TimeUnit.MILLISECONDS;

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
		watch.start();
		for (int count = 0; count < target; count++) {
			service.add(columns[count], values[count], rows[count]);
		}

		long elapsed = watch.stop(unit);
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

			service.add(column, value, row);
		}

		log("Removing data...");
		watch.start();
		for (int count = 0; count < removeTarget; count++) {
			int index = rand.nextInt(initTarget);

			long row = rows[index];
			String column = columns[index];
			Object value = values[index];

			service.remove(column, value, row);
		}
		long elapsed = watch.stop(unit);
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
		writeToService(service, columns, values, rows);

		String column;
		Object value;
		long elapsed;
		Set<Long> results;

		Operator[] operators = Operator.values();
		for (Operator operator : operators) {
			try {
				column = columns[rand.nextInt(columns.length)];
				value = values[rand.nextInt(values.length)];
				log("Performing QUERY with {} operator", operator);
				watch.start();
				if(operator == Operator.BETWEEN) {
					Object value2 = values[rand.nextInt(values.length)];
					results = service.query(column, operator, value, value2);
				}
				else {
					results = service.query(column, operator, value);
				}
				elapsed = watch.stop(unit);
				log("Runtime for query: {} {}", elapsed, unit);
				log("Results for query: {}", results);
				log(System.lineSeparator());
			}
			catch (UnsupportedOperationException e) {
				log("{}", e.getMessage());
				watch.stop();
				continue;
			}

		}

	}

	@Test
	public void testFetch() {
		String column = randomColumnName();
		ConcourseService service = getServiceWithColumns(column);

		TimeUnit unit = TimeUnit.MILLISECONDS;
		watch.start();
		service.fetch(column, randomLong());
		long elapsed = watch.stop(unit);
		log("Runtime for fetch(): {} {}", elapsed, unit);
	}

	@Test
	public void testDescribe() {
		long row = randomLong();
		ConcourseService service = getServiceWithRows(row);

		TimeUnit unit = TimeUnit.MILLISECONDS;
		watch.start();
		service.describe(row);
		long elapsed = watch.stop(unit);
		log("Runtime for describe(): {} {}", elapsed, unit);
	}

}

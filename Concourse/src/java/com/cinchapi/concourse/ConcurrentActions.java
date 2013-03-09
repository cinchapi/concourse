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

import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.api.ConcourseService;
import com.cinchapi.concourse.api.Queryable.SelectOperator;

/**
 * Contains various callables and runnables for concurrency in {@link Concourse}
 * .
 * 
 * @author Jeff Nelson
 */
public class ConcurrentActions {

	/**
	 * Execute {@link ConcourseService#add(long, String, Object)}.
	 * 
	 * @param service
	 * @param row
	 * @param column
	 * @param value
	 * @return the method return value
	 */
	public static Add add(ConcourseService service, long row, String column,
			Object value) {
		return new Add(service, row, column, value);
	}

	/**
	 * Execute {@link ConcourseService#describe(long)}.
	 * 
	 * @param service
	 * @param row
	 * @return the method return value
	 */
	public static Describe describe(ConcourseService service, long row) {
		return new Describe(service, row);
	}

	/**
	 * Execute {@link ConcourseService#exists(long, String, Object)}.
	 * 
	 * @param service
	 * @param row
	 * @param column
	 * @param value
	 * @return the method return value
	 */
	public static Exists exists(ConcourseService service, long row,
			String column, Object value) {
		return new Exists(service, row, column, value);
	}

	/**
	 * Execute {@link ConcourseService#get(long, String)}.
	 * 
	 * @param service
	 * @param row
	 * @param column
	 * @return the method return value
	 */
	public static Get get(ConcourseService service, long row, String column) {
		return new Get(service, row, column);
	}

	/**
	 * Execute {@link ConcourseService#remove(long, String, Object)}.
	 * 
	 * @param service
	 * @param row
	 * @param column
	 * @param value
	 * @return the method return value
	 */
	public static Remove remove(ConcourseService service, long row,
			String column, Object value) {
		return new Remove(service, row, column, value);
	}

	/**
	 * Execute
	 * {@link ConcourseService#select(String, SelectOperator, Object...)} .
	 * 
	 * @param service
	 * @param row
	 * @param column
	 * @param value
	 * @return the method return value
	 */
	public static Select select(ConcourseService service, String column,
			SelectOperator operator, Object... values) {
		return new Select(service, column, operator, values);
	}

	private ConcurrentActions() {}

	/**
	 * A {@link Callable} that can execute methods in an
	 * {@link ConcourseService} and return a result.
	 * 
	 * @author jnelson
	 * @param <V>
	 *            - the result type of the called method
	 */
	@Immutable
	private static abstract class AbstractConcourseServiceCallable<V> implements
			Callable<V> {

		protected final ConcourseService service;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 */
		public AbstractConcourseServiceCallable(ConcourseService service) {
			this.service = service;
		}
	}

	/**
	 * Execute the {@link ConcourseService#add(long, String, Object)} method.
	 * 
	 * @author jnelson
	 */
	private static final class Add extends
			AbstractConcourseServiceCallable<Boolean> {

		private final long row;
		private final String column;
		private final Object value;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 */
		public Add(ConcourseService service, long row, String column,
				Object value) {
			super(service);
			this.row = row;
			this.column = column;
			this.value = value;
		}

		@Override
		public Boolean call() throws Exception {
			return service.add(row, column, value);
		}

	}

	/**
	 * Execute the {@link ConcourseService#describe(long)} method.
	 * 
	 * @author jnelson
	 */
	private static final class Describe extends
			AbstractConcourseServiceCallable<Set<String>> {

		private final long row;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 * @param row
		 */
		public Describe(ConcourseService service, long row) {
			super(service);
			this.row = row;
		}

		@Override
		public Set<String> call() throws Exception {
			return service.describe(row);
		}

	}

	/**
	 * Execute the {@link ConcourseService#exists(long, String, Object)} method.
	 * 
	 * @author jnelson
	 */
	private static final class Exists extends
			AbstractConcourseServiceCallable<Boolean> {

		private final long row;
		private final String column;
		private final Object value;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 */
		public Exists(ConcourseService service, long row, String column,
				Object value) {
			super(service);
			this.row = row;
			this.column = column;
			this.value = value;
		}

		@Override
		public Boolean call() throws Exception {
			return service.exists(row, column, value);
		}

	}

	/**
	 * Execute the {@link ConcourseService#get(long)} method.
	 * 
	 * @author jnelson
	 */
	private static final class Get extends
			AbstractConcourseServiceCallable<Set<Object>> {

		private final long row;
		private final String column;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 */
		public Get(ConcourseService service, long row, String column) {
			super(service);
			this.row = row;
			this.column = column;
		}

		@Override
		public Set<Object> call() throws Exception {
			return service.get(row, column);
		}

	}

	/**
	 * Execute the {@link ConcourseService#remove(long, String, Object)} method.
	 * 
	 * @author jnelson
	 */
	private static final class Remove extends
			AbstractConcourseServiceCallable<Boolean> {

		private final long row;
		private final String column;
		private final Object value;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 * @param row
		 * @param column
		 * @param value
		 */
		public Remove(ConcourseService service, long row, String column,
				Object value) {
			super(service);
			this.row = row;
			this.column = column;
			this.value = value;
		}

		@Override
		public Boolean call() throws Exception {
			return service.remove(row, column, value);
		}

	}

	/**
	 * Execute the
	 * {@link ConcourseService#select(String, com.cinchapi.concourse.api.Queryable.SelectOperator, Object...)}
	 * method.
	 * 
	 * @author jnelson
	 */
	private static final class Select extends
			AbstractConcourseServiceCallable<Set<Long>> {

		private final String column;
		private final SelectOperator operator;
		private final Object[] values;

		/**
		 * Construct a new instance.
		 * 
		 * @param service
		 * @param column
		 * @param operator
		 * @param values
		 */
		public Select(ConcourseService service, String column,
				SelectOperator operator, Object... values) {
			super(service);
			this.column = column;
			this.operator = operator;
			this.values = values;
		}

		@Override
		public Set<Long> call() throws Exception {
			return service.select(column, operator, values);
		}

	}

}

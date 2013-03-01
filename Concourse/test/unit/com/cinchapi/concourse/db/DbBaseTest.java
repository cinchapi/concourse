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

import com.cinahpi.concourse.BaseTest;

/**
 * Base class for all tests in the com.cinchapi.concourse.db package.
 * 
 * @author jnelson
 */
public abstract class DbBaseTest extends BaseTest {

	/**
	 * Return a random {@link Value} where {@link Value#isForStorage()} is
	 * {@code true}.
	 * 
	 * @return the value.
	 */
	protected Value randomValueForStorage() {
		return new ValueBuilder().build();
	}

	/**
	 * Return a random {@link Value} where {@link Value#isForStorage()} is
	 * {@code false}.
	 * 
	 * @return the value.
	 */
	protected Value randomValueNotForStorage() {
		return new ValueBuilder().setForStorage(false).build();
	}

	/**
	 * A builder for {@link Value} objects.
	 * 
	 * @author jnelson
	 */
	protected class ValueBuilder {

		private Object quantity = randomObject();
		private boolean forStorage = true;

		/**
		 * Set the {@code quantity} for the {@link Value} that will be built.
		 * Default is random.
		 * 
		 * @param quantity
		 * @return this
		 */
		public ValueBuilder setQuantity(Object quantity) {
			this.quantity = quantity;
			return this;
		}

		/**
		 * If {@code true} then the built {@link Value} will return {@code true}
		 * for {@link Value#isForStorage()}. Default is {@code true}.
		 * 
		 * @param quantity
		 * @return this
		 */
		public ValueBuilder setForStorage(boolean forStorage) {
			this.forStorage = forStorage;
			return this;
		}

		/**
		 * Build the {@link Value}.
		 * 
		 * @return the value
		 */
		public Value build() {
			return forStorage ? Value.forStorage(quantity) : Value
					.notForStorage(quantity);
		}

	}

}

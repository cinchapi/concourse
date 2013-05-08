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

/**
 * The type contained within a {@link Value}.
 */
enum Type {
	BOOLEAN, DOUBLE, FLOAT, INTEGER, LONG, RELATION, STRING;

	/**
	 * Return the {@link Type} for {@code value}.
	 * 
	 * @param value
	 * @return the value type.
	 */
	static Type of(Object value) {
		Type type;
		if(value instanceof Boolean) {
			type = BOOLEAN;
		}
		else if(value instanceof Double) {
			type = DOUBLE;
		}
		else if(value instanceof Float) {
			type = FLOAT;
		}
		else if(value instanceof Integer) {
			type = INTEGER;
		}
		else if(value instanceof Long) {
			type = LONG;
		}
		else if(value instanceof PrimaryKey) {
			type = RELATION;
		}
		else {
			type = STRING;
		}
		return type;
	}

	@Override
	public String toString() {
		return this.name().toLowerCase();
	}
}
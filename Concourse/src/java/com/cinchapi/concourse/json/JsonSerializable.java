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
package com.cinchapi.concourse.json;

import com.google.gson.JsonElement;

/**
 * An object whose state can be serialized using the JSON format.
 * 
 * @author jnelson
 * @param <T>
 *            - the object type
 */
public interface JsonSerializable<T> {

	/**
	 * Return a {@link JsonElement} that represents the object in JSON notation.
	 * 
	 * @return the serialized element.
	 */
	public JsonElement jsonSerialize();

	/**
	 * Return a string that represents the object in JSON notation.
	 * 
	 * @return the serialized string.
	 */
	public String toJsonString();

}

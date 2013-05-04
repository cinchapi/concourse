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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.concourse.io.ByteSized;

/**
 * Represents a {@link ByteSized} UTF-8 encoded string that is used internally
 * for column names.
 * 
 * @author jnelson
 */
@Immutable
class ColumnName implements ByteSized {

	/**
	 * Return a {@link ColumnName} from the encoded {@code bytes}.
	 * 
	 * @param bytes
	 * @return the UTF-8 encoded string
	 */
	public static ColumnName fromBytes(byte[] bytes) {
		String string = Utilities.bytesToString(bytes);
		ColumnName utf8 = cache.get(string);
		if(utf8 == null) {
			utf8 = new ColumnName(bytes);
			cache.put(utf8, string);
		}
		return utf8;
	}

	/**
	 * Return a {@link ColumnName} based on {@code string}.
	 * 
	 * @param string
	 * @return the UTF-8 encoded string
	 */
	public static ColumnName fromString(String string) {
		ColumnName utf8 = cache.get(string);
		if(utf8 == null) {
			utf8 = new ColumnName(string);
			cache.put(utf8, string);
		}
		return utf8;
	}

	private final static ObjectReuseCache<String> stringCache = new ObjectReuseCache<String>();
	private final static ObjectReuseCache<ColumnName> cache = new ObjectReuseCache<ColumnName>();
	private final static Charset UTF_8 = StandardCharsets.UTF_8;
	private final byte[] bytes;

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	private ColumnName(byte[] bytes) {
		this.bytes = bytes;
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param string
	 */
	private ColumnName(String string) {
		this(string.getBytes(UTF_8));
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}

	@Override
	public int size() {
		return bytes.length;
	}

	@Override
	public String toString() {
		return Utilities.bytesToString(bytes);
	}

	/**
	 * Utilities for the {@link ColumnName} class.
	 * 
	 * @author jnelson
	 */
	private final static class Utilities {

		/**
		 * Convert {@code bytes} to a UTF-8 encoded {@link String} object.
		 * This
		 * method takes advantage of an {@link ObjectReuseCache} to prevent
		 * unnecessary string creation.
		 * 
		 * @param bytes
		 * @return the string object
		 */
		public static String bytesToString(byte[] bytes) {
			int hashCode = Arrays.hashCode(bytes);
			String string = stringCache.get(hashCode);
			if(string == null) {
				string = new String(bytes, UTF_8);
				stringCache.put(string, hashCode);
			}
			return string;
		}
	}

}
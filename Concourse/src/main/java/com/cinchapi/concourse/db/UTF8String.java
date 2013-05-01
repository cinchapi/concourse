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
 * Represents a {@link ByteSized} UTF-8 encoded string.
 * 
 * @author jnelson
 */
@Immutable
public class UTF8String implements ByteSized {

	private final static ObjectReuseCache<String> cache = new ObjectReuseCache<String>();
	private final static Charset UTF_8 = StandardCharsets.UTF_8;
	private final byte[] bytes;

	/**
	 * Construct a new instance.
	 * 
	 * @param string
	 */
	public UTF8String(String string) {
		this(string.getBytes(UTF_8));
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	public UTF8String(byte[] bytes) {
		this.bytes = bytes;
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
	 * Utilities for the {@link UTF8String} class.
	 * 
	 * @author jnelson
	 */
	private final static class Utilities {

		/**
		 * Convert {@code bytes} to a UTF-8 encoded {@link String} object. This
		 * method takes advantage of an {@link ObjectReuseCache} to prevent
		 * unnecessary string creation.
		 * 
		 * @param bytes
		 * @return the string object
		 */
		public static String bytesToString(byte[] bytes) {
			int hashCode = Arrays.hashCode(bytes);
			String string = cache.get(hashCode);
			if(string == null) {
				string = new String(bytes, UTF_8);
				cache.put(string, hashCode);
			}
			return string;
		}
	}

}

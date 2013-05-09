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
 * Represents a {@link ByteSized} UTF-8 encoded string that is used internally.
 * This class implements {@link #hashCode()} and {@link #equals(Object)} such
 * that an instance is considered equivalent to the {@link String} object of its
 * {@link #toString()} value.
 * 
 * @author jnelson
 */
@Immutable
class ByteSizedString implements ByteSized {

	/**
	 * Return a {@link ByteSizedString} from the encoded {@code bytes}.
	 * 
	 * @param bytes
	 * @return the UTF-8 encoded string
	 */
	public static ByteSizedString fromBytes(byte[] bytes) {
		String string = Utilities.bytesToString(bytes);
		ByteSizedString utf8 = cache.get(string);
		if(utf8 == null) {
			utf8 = new ByteSizedString(bytes);
			cache.put(utf8, string);
		}
		return utf8;
	}

	/**
	 * Return a {@link ByteSizedString} based on {@code string}.
	 * 
	 * @param string
	 * @return the UTF-8 encoded string
	 */
	public static ByteSizedString fromString(String string) {
		ByteSizedString utf8 = cache.get(string);
		if(utf8 == null) {
			utf8 = new ByteSizedString(string);
			cache.put(utf8, string);
		}
		return utf8;
	}

	private final static ObjectReuseCache<String> stringCache = new ObjectReuseCache<String>();
	private final static ObjectReuseCache<ByteSizedString> cache = new ObjectReuseCache<ByteSizedString>();
	private final static Charset UTF_8 = StandardCharsets.UTF_8;
	private final byte[] bytes;

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	private ByteSizedString(byte[] bytes) {
		this.bytes = bytes;
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param string
	 */
	private ByteSizedString(String string) {
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

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ByteSizedString) {
			ByteSizedString other = (ByteSizedString) obj;
			return Arrays.equals(bytes, other.bytes);
		}
		else if(obj instanceof String) {
			String other = (String) obj;
			return toString().equals(other);
		}
		return false;
	}

	/**
	 * Utilities for the {@link ByteSizedString} class.
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
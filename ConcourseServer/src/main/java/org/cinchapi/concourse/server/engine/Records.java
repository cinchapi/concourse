/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.engine;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.cache.ReferenceCache;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteable;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import static org.cinchapi.concourse.server.engine.Record.LOCALE_NESTING_FACTOR;

/**
 * A collection of utility methods that are used in the {@link Record} class.
 * 
 * @author jnelson
 */
@PackagePrivate
abstract class Records { 

	/**
	 * Return the locale for {@code locator}. There is a 1:1 mapping between
	 * locators and locales.
	 * 
	 * @param locator
	 * @return the locale
	 */
	public static <L extends Byteable> String getLocale(L locator) {
		byte[] bytes = ByteBuffers.toByteArray(locator.getBytes());
		char[] hex = DigestUtils.sha256Hex(bytes).toCharArray();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < hex.length; i++) {
			sb.append(hex[i]);
			int next = i + 1;
			if(next >= LOCALE_NESTING_FACTOR
					&& next % LOCALE_NESTING_FACTOR == 0 && next < hex.length) {
				sb.append(File.separator);
			}
		}
		return sb.toString();
	}

	/**
	 * Open the record of type {@code clazz} identified by a {@code locator} of
	 * type {@code locatorClass}. This method will store a reference to the
	 * record in a dynamically created cache.
	 * 
	 * @param clazz
	 * @param locatorClass
	 * @param locator
	 * @return the Record
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Record<L, ?, ?>, L extends Byteable> T open(
			Class<T> clazz, Class<L> locatorClass, L locator) {
		try {
			ReferenceCache<T> cache = (ReferenceCache<T>) caches.get(clazz);
			if(cache == null) {
				cache = new ReferenceCache<T>();
				caches.put(clazz, cache);
			}
			T record = cache.get(locator);
			if(record == null) {
				Constructor<T> constructor = clazz.getConstructor(locatorClass);
				constructor.setAccessible(true);
				record = constructor.newInstance(locator);
				cache.put(record, locator);
			}
			return record;
		}
		catch (ReflectiveOperationException e) {
			throw Throwables.propagate(e);
		}
	}

	private static final HashMap<Class<?>, ReferenceCache<?>> caches = Maps
			.newHashMap();

}

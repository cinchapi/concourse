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
package org.cinchapi.concourse.server;

import java.util.Map;
import java.util.Objects;

import org.cinchapi.concourse.server.util.Loggers;
import org.cinchapi.concourse.thrift.TObject;
import org.slf4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * The {@link Context} contains global configuration and state that must be
 * accessible to various parts of the Server. A single Context is created in
 * {@link ConcourseServer} and passed to and around the Engine components}.
 * 
 * @author jnelson
 */
public final class Context {

	/**
	 * Return the current {@link Context}.
	 * 
	 * @return the Context
	 */
	public static Context getContext() {
		if(context == null) {
			context = new Context();
		}
		return context;
	}

	/**
	 * Enforces the singleton pattern.
	 */
	private static Context context = null;

	/**
	 * The expected number of bloom filters. This value should correspond to the
	 * expected number of keys.
	 */
	private static final int EXPECTED_NUM_BLOOM_FILTERS = 100000;
	private static final Logger log = Loggers.getLogger();

	/* COMPONENTS */
	private final BloomFilters bf = new BloomFilters();

	/**
	 * Construct a new instance.
	 */
	private Context() {} /* restricted */

	/**
	 * Return a pointer to the collection of {@link BloomFilter} objects that
	 * are used to efficiently determine if data with {@code key} as
	 * {@code value} in {@code record} <em>probably</em> exists.
	 */
	public BloomFilters getBloomFilters() {
		return bf;
	}

	/**
	 * A collection of {@link BloomFilter} objects that are used to efficiently
	 * determine if data with {@code key} as {@code value} in {@code record}
	 * <em>probably</em> exists.
	 * 
	 * @author jnelson
	 */
	public final class BloomFilters {

		/**
		 * Mapping from key to bloom filter.
		 */
		private final Map<String, BloomFilter<Integer>> filters = Maps
				.newHashMapWithExpectedSize(EXPECTED_NUM_BLOOM_FILTERS);

		/**
		 * Add {@code key} as {@code value} to {@code record} in the appropriate
		 * bloom filter.
		 * 
		 * @param key
		 * @param value
		 * @param record
		 */
		public void add(String key, TObject value, long record) {
			BloomFilter<Integer> filter = filters.get(key);
			if(filter == null) {
				filter = BloomFilter.create(Funnels.integerFunnel(), 100000);
				filters.put(key, filter);
				log.info("Added new bloom filter for '{}'", key);
			}
			filter.put(Objects.hash(value, record));
			log.debug("Added {} as {} to {} in a bloom filter", key, value,
					record);
		}

		/**
		 * Verify that data with {@code key} as {@code value} in {@code record}
		 * <em>probably</em> exists. If this function returns {@code true}, it
		 * is necessary to check the appropriate record(s) for certainty.
		 * However, if the function returns {@code false}, the caller can be
		 * sure that the data does not exist.
		 * 
		 * @param key
		 * @param value
		 * @param record
		 * @return {@code true} if the revision probably exists
		 */
		public boolean verify(String key, TObject value, long record) {
			BloomFilter<Integer> filter = filters.get(key);
			return filter == null ? false : filter.mightContain(Objects.hash(
					value, record));
		}
	}

}

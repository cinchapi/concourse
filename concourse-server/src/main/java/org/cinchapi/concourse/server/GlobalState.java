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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cinchapi.concourse.server.model.Write;
import org.cinchapi.concourse.server.util.Loggers;
import org.cinchapi.concourse.thrift.TObject;
import org.slf4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * Contains configuration and state that must be accessible to various parts of
 * the Server.
 * 
 * @author jnelson
 */
public final class GlobalState {

	public static final Logger log = Loggers.getLogger();
	public static final BloomFilterWrapper BLOOM_FILTERS = new BloomFilterWrapper();
	public static final Set<String> STOPWORDS = Sets.newHashSet();
	static {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"conf/stopwords.txt"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				STOPWORDS.add(line);
			}
			reader.close();
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * A class that wraps a collection of {@link BloomFilter} objects. We use a
	 * distinct BloomFilter for each key to determine if data with {@code key}
	 * as {@code value} in {@code record} <em>probably</em> exists without
	 * performing a disk lookup.
	 * 
	 * @author jnelson
	 */
	public static final class BloomFilterWrapper {

		/**
		 * The expected number of bloom filters. This value should correspond to
		 * the expected number of keys.
		 */
		private static final int EXPECTED_NUM_BLOOM_FILTERS = 100000;

		/**
		 * Mapping from key to bloom filter.
		 */
		private final Map<String, BloomFilter<Integer>> filters = Maps
				.newHashMapWithExpectedSize(EXPECTED_NUM_BLOOM_FILTERS);

		private BloomFilterWrapper() {/* Non-Initializable */};

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
		 * Add {@code write} to the appropriate bloom filter.
		 * 
		 * @param write
		 */
		public void add(Write write) {
			if(write.isForStorage()) {
				add(write.getKey().toString(), write.getValue().getQuantity(),
						write.getRecord().longValue());
			}
			else {
				throw new IllegalArgumentException(
						"Cannot add a notForStorage Write to a bloom filter");
			}
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

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


import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.util.RandomString;
import com.cinchapi.common.util.Strings;
import com.cinchapi.concourse.Operator;
import com.cinchapi.concourse.db.Record.Element;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.cinchapi.concourse.Operator.*;

/**
 * A Concordance is a {@link Tuple} that represents the mapping of terms to
 * locations.
 * 
 * @author jnelson
 */
public class SearchIndex extends Tuple<SuperString, Location> {

	/**
	 * Return the concordance that is located by {@code field}.
	 * 
	 * @param field
	 * @return the concordance
	 */
	public static Concordance fromKey(SuperString field) {
		Concordance concordance = cache.get(field);
		if(concordance == null) {
			concordance = new Concordance(field);
			cache.put(concordance, field);
		}
		return concordance;
	}

	public static void main(String... args) {
		String string = new RandomString().nextString();
		System.out.println("the length is " + string.length());
		System.out.println("the string is " + string);

		Set<String> m1 = Sets.newLinkedHashSet();
		Set<String> m2 = Sets.newLinkedHashSet();

		// m1: all substrings
		m1 = index(string);
		System.out.println(m1);
		System.out.println(m1.size());

		// m2: word substrings
		String[] toks = string.split(" ");
		for (String tok : toks) {
			m2.addAll(index(tok));
		}
		System.out.println(m2);
		System.out.println(m2.size());
	}

	static Set<String> index(String string) {
		Set<String> set = Sets.newLinkedHashSet();
		for (int i = 0; i < string.length(); i++) {
			for (int j = i + 1; j < string.length() + 1; j++) {
				String index = string.substring(i, j);
				if(index != " ") {
					set.add(string.substring(i, j));
				}
			}
		}
		return set;
	}

	private static final Element mock = Bucket.mock(Element.class);

	private static final ObjectReuseCache<Concordance> cache = new ObjectReuseCache<Concordance>();

	protected static final String FILE_NAME_EXT = "ccc"; // @Override from
															// {@link Store}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	protected SearchIndex(SuperString index) {
		super(index);
	}

	@Override
	protected Bucket<SuperString, Location> getBucketFromByteSequence(
			ByteBuffer bytes) {
		return new Element(bytes);
	}

	@Override
	protected Bucket<SuperString, Location> getMockBucket() {
		return mock;
	}

	final void add(Value value, PrimaryKey key) {
		if(value.getType() == Type.STRING.toString()) {
			String[] toks = ((String) value.getQuantity()).split(" ");
			int pos = 0;
			for (String tok : toks) {
				// TODO check if tok is stopword and if so remove
				for (int i = 0; i < tok.length(); i++) {
					for (int j = i + 1; j < tok.length() + 1; j++) {
						String index = tok.substring(i, j);
						if(index != " ") {
							add(SuperString.fromString(index),
									Location.forStorage(key, pos));
						}
					}
				}
				pos++;
			}
		}

	}
	
	final void remove(Value value, PrimaryKey key) {
		if(value.getType() == Type.STRING.toString()) {
			String[] toks = ((String) value.getQuantity()).split(" ");
			int pos = 0;
			for (String tok : toks) {
				// TODO check if tok is stopword and if so remove
				for (int i = 0; i < tok.length(); i++) {
					for (int j = i + 1; j < tok.length() + 1; j++) {
						String index = tok.substring(i, j);
						if(index != " ") {
							remove(SuperString.fromString(index),
									Location.forStorage(key, pos));
						}
					}
				}
				pos++;
			}
		}

	}
	
	/**
	 * Return the rows that satisfy {@code operator} in relation to the
	 * specified {@code values} at the present or at the specified
	 * {@code timestamp} if {@code historical is {@code true}
	 * 
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @param operator
	 * @param values
	 * @return the set of rows that match the query
	 */
	private Set<PrimaryKey> query(boolean historical, long timestamp,
			Operator operator, Value... values) {
		Set<PrimaryKey> keys = Sets.newTreeSet();
		Value value = values[0];

		if(operator == SEARCH) {
			// TODO implement search
			String[] toks = ((String) value.getQuantity()).split(" ");
			boolean initial = true;
			for(String tok : toks){
				if(initial){
					keys.addAll(get(SuperString.fromString(tok)).)
				}
			}
		}
		else {
			throw new UnsupportedOperationException(operator
					+ " operator is unsupported");
		}
		return keys;
	}

	@Override
	protected Bucket<SuperString, Location> getNewBucket(SuperString term) {
		return new Element(term);
	}

	@Override
	protected Map<SuperString, Bucket<SuperString, Location>> getNewBuckets(
			int expectedSize) {
		return Maps.newHashMapWithExpectedSize(expectedSize);
	}

	/**
	 * A single element within a {@link Concordance}.
	 * 
	 * @author jnelson
	 */
	final static class Element extends LockableBucket<SuperString, Location> {

		/**
		 * Construct a new instance. Use this constructor when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 */
		Element(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param term
		 */
		Element(SuperString term) {
			super(term);
		}

		@Override
		protected SuperString getKeyFromByteSequence(ByteBuffer bytes) {
			return SuperString.fromBytes(bytes.array());
		}

		@Override
		protected Location getValueFromByteSequence(ByteBuffer bytes) {
			return Location.fromByteSequence(bytes);
		}
	}

}

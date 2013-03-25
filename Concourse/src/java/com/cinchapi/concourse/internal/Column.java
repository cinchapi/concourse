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
package com.cinchapi.concourse.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.Hash;
import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.cinchapi.concourse.internal.QueryService.Operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A collection of {@link Value} where each is mapped to a {@link Key}.
 * 
 * @author jnelson
 */
final class Column extends PersistableIndex<String, Value, ValueIndex> {

	/**
	 * Return the column represented by {@code bytes}. Use this method when
	 * reading
	 * and reconstructing from a file. This method assumes that {@code bytes}
	 * was generated using {@link #getBytes()}.
	 * 
	 * @param filename
	 * @param name
	 * @param bytes
	 * @return the column
	 */
	private static Column fromByteSequences(String filename, String name,
			ByteBuffer bytes) {
		TreeMap<Value, ValueIndex> values = Maps.newTreeMap();
		IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
				.over(bytes.array());
		while (bsit.hasNext()) {
			ValueIndex index = ValueIndex.fromByteSequence(bsit.next());
			values.put(index.getValue(), index);
		}
		return new Column(filename, name, values);
	}

	/**
	 * Return the {link Column} identified by {@code name}.
	 * 
	 * @param name
	 * @param home
	 *            - the home directory where columns are stored
	 * @return the column
	 */
	static Column identifiedBy(String name, String home) {
		Column column = cache.get(name);
		if(column == null) {
			String filename = home + File.separator
					+ Utilities.getStorageFileNameFor(name);
			try {
				File file = new File(filename);
				file.getParentFile().mkdirs();
				file.createNewFile();

				byte[] bytes = new byte[(int) file.length()];
				ByteBuffer buffer = ByteBuffer.wrap(bytes);
				new FileInputStream(filename).getChannel().read(buffer); // deserialize
																			// entire
																			// column
				column = fromByteSequences(filename, name, buffer);
				cache.put(column, name);
			}
			catch (IOException e) {
				log.error(
						"An error occured while trying to deserialize column {} from {}: {}",
						name, filename, e);
				throw new ConcourseRuntimeException(e);
			}
		}
		return column;
	}

	/**
	 * A larger name length allows more buckets (and therefore a smaller
	 * bucket:row ratio), however the filesystem can act funny if a single
	 * directory has too many files. This number should seek to have the
	 * bucket:row ratio equitable to the number of possible buckets while being
	 * mindful of not having too many files in a single directory.
	 */
	private static final int STORAGE_BUCKET_NAME_LENGTH = 4;
	private static final String STORAGE_FILE_NAME_EXTENSION = ".cc";
	private static final Logger log = LoggerFactory.getLogger(Column.class);
	private static final ObjectReuseCache<Column> cache = new ObjectReuseCache<Column>();
	public static final int AVG_COLUMN_NAME_SIZE_IN_BYTES = 24;

	/**
	 * Construct a new instance.
	 * 
	 * @param filename
	 * @param name
	 * @param components
	 */
	private Column(String filename, String name,
			TreeMap<Value, ValueIndex> components) {
		super(filename, name, components);
	}

	@Override
	protected Logger getLogger() {
		return log;
	}

	/**
	 * Add {@code row} to {@code value}.
	 * 
	 * @param row
	 * @param value
	 */
	void add(Key row, Value value) {
		ValueIndex index;
		if(components.containsKey(value)) {
			index = components.get(value);
		}
		else {
			index = ValueIndex.forValue(value);
			components.put(value, index);
		}
		index.add(row);
	}

	/**
	 * Return the rows that satisfy {@code operator} in relation to
	 * {@code value}.
	 * 
	 * @param operator
	 * @param values
	 * @return the row set.
	 */
	Set<Key> query(Operator operator, Value... values) {
		Set<Key> keys = Sets.newLinkedHashSet();
		Value value = values[0];

		if(operator == Operator.EQUALS) {
			keys = this.components.get(value).getKeys();
		}
		else if(operator == Operator.NOT_EQUALS) {
			Iterator<Entry<Value, ValueIndex>> it = this.components.entrySet()
					.iterator();
			while (it.hasNext()) {
				Entry<Value, ValueIndex> entry = it.next();
				Value theVal = entry.getKey();
				ValueIndex index = entry.getValue();
				if(!theVal.equals(value)) {
					keys.addAll(index.getKeys());
				}
			}
		}
		else if(operator == Operator.GREATER_THAN) {
			Iterator<ValueIndex> it = ((TreeMap<Value, ValueIndex>) this.components)
					.tailMap(value, false).values().iterator();
			while (it.hasNext()) {
				keys.addAll(it.next().getKeys());
			}
		}
		else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
			Iterator<ValueIndex> it = ((TreeMap<Value, ValueIndex>) this.components)
					.tailMap(value, true).values().iterator();
			while (it.hasNext()) {
				keys.addAll(it.next().getKeys());
			}
		}
		else if(operator == Operator.LESS_THAN) {
			Iterator<ValueIndex> it = ((TreeMap<Value, ValueIndex>) this.components)
					.headMap(value, false).values().iterator();
			while (it.hasNext()) {
				keys.addAll(it.next().getKeys());
			}
		}
		else if(operator == Operator.LESS_THAN_OR_EQUALS) {
			Iterator<ValueIndex> it = ((TreeMap<Value, ValueIndex>) this.components)
					.headMap(value, true).values().iterator();
			while (it.hasNext()) {
				keys.addAll(it.next().getKeys());
			}
		}
		else if(operator == Operator.BETWEEN) {
			Preconditions.checkArgument(values.length > 1,
					"You must specify two arguments for the BETWEEN selector.");
			Value value2 = values[1];
			Iterator<ValueIndex> it = ((TreeMap<Value, ValueIndex>) this.components)
					.subMap(value, true, value2, false).values().iterator();
			while (it.hasNext()) {
				keys.addAll(it.next().getKeys());
			}
		}
		else if(operator == Operator.REGEX) {
			Iterator<Entry<Value, ValueIndex>> it = this.components.entrySet()
					.iterator();
			while (it.hasNext()) {
				Entry<Value, ValueIndex> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				ValueIndex index = entry.getValue();
				Pattern p = Pattern.compile(value.getQuantity().toString());
				Matcher m = p.matcher(obj.toString());
				if(m.matches()) {
					keys.addAll(index.getKeys());
				}
			}
		}
		else if(operator == Operator.NOT_REGEX) {
			Iterator<Entry<Value, ValueIndex>> it = this.components.entrySet()
					.iterator();
			while (it.hasNext()) {
				Entry<Value, ValueIndex> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				ValueIndex index = entry.getValue();
				Pattern p = Pattern.compile(value.getQuantity().toString());
				Matcher m = p.matcher(obj.toString());
				if(!m.matches()) {
					keys.addAll(index.getKeys());
				}
			}
		}
		else {
			throw new UnsupportedOperationException(operator
					+ " operator is unsupported");
		}
		return keys;
	}

	/**
	 * Remove {@code row} from {@code value}.
	 * 
	 * @param row
	 * @param value
	 */
	void remove(Key row, Value value) {
		if(components.containsKey(value)) {
			ValueIndex index = components.get(value);
			index.remove(row);
			if(index.isEmpty()) {
				components.remove(value);
			}
		}
	}

	/**
	 * Utilities for the {@link Column} class.
	 * 
	 * @author jnelson
	 */
	private static final class Utilities {

		/**
		 * Return the storage filename for the column identified by {@code name}
		 * .
		 * 
		 * @param name
		 * @return the storage filename
		 */
		public static String getStorageFileNameFor(String name) {
			StringBuilder sb = new StringBuilder();
			sb.append(getStorageBucketFor(name));
			sb.append(File.separator);
			sb.append(name);
			sb.append(STORAGE_FILE_NAME_EXTENSION);
			return sb.toString();
		}

		/**
		 * Return the appropriate storage bucket for the row identified by
		 * {@code key}.
		 * 
		 * @param key
		 * @return the storage bucket
		 */
		private static String getStorageBucketFor(String name) {
			return Hash.toString(
					Hash.sha256(name.getBytes(ByteBuffers.charset())))
					.substring(0, STORAGE_BUCKET_NAME_LENGTH);
		}
	}

}

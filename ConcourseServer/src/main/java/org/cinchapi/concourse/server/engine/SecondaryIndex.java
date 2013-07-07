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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.concourse.thrift.Operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A grouping of data for efficient indirect queries.
 * <p>
 * Each SecondaryIndex maps a value to a set of PrimaryKeys and provides an
 * interface for querying.
 * </p>
 * 
 * @author jnelson
 */
public class SecondaryIndex extends Record<Text, Value, PrimaryKey> {

	/**
	 * Return the SearchIndex that is identified by {@code key}.
	 * 
	 * @param key
	 * @return the SearchIndex
	 */
	public static SecondaryIndex loadSecondaryIndex(Text key) {
		return Records.open(SecondaryIndex.class, Text.class, key);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	@DoNotInvoke
	public SecondaryIndex(Text locator) {
		super(locator);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <T extends Field<Value, PrimaryKey>> Class<T> fieldImplClass() {
		return (Class<T>) SecondaryField.class;
	}

	@Override
	protected String fileNameExt() {
		return "csi";
	}

	@Override
	protected Map<Value, Field<Value, PrimaryKey>> init() {
		return Maps.newTreeMap();
	}

	@Override
	protected Class<Value> keyClass() {
		return Value.class;
	}

	/**
	 * Return the PrimaryKeys that <em>currently</em> satisfy {@code operator}
	 * in relation to the specified {@code values}.
	 * 
	 * @param operator
	 * @param values
	 * @return they Set of PrimaryKeys that match the query
	 */
	@PackagePrivate
	Set<PrimaryKey> find(Operator operator, Value... values) {
		return find(false, 0, operator, values);
	}

	/**
	 * Return the PrimaryKeys that satisfied {@code operator} in relation to the
	 * specified {@code values} at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @param operator
	 * @param values
	 * @return the Set of PrimaryKeys that match the query
	 */
	@PackagePrivate
	Set<PrimaryKey> find(long timestamp, Operator operator, Value... values) {
		return find(true, timestamp, operator, values);
	}

	/**
	 * Return the Set of PrimaryKeys that currently satisfy {@code operator} in
	 * relation to the specified {@code values} or at the specified
	 * {@code timestamp} if {@code historical} is {@code true}
	 * 
	 * @param historical - if {@code true} query the history for each field,
	 *            otherwise query the current state
	 * @param timestamp - this value is ignored if {@code historical} is
	 *            {@code false}, otherwise this value is the historical
	 *            timestamp at which to query the field
	 * @param operator
	 * @param values
	 * @return the Set of PrimaryKeys that match the query
	 */
	private Set<PrimaryKey> find(boolean historical, long timestamp,
			Operator operator, Value... values) {
		Set<PrimaryKey> keys = Sets.newTreeSet();
		Value value = values[0];
		if(operator == Operator.EQUALS) {
			keys.addAll(historical ? get(value).getValues(timestamp) : get(
					value).getValues());
		}
		else if(operator == Operator.NOT_EQUALS) {
			Iterator<Field<Value, PrimaryKey>> it = fields().values()
					.iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> field = it.next();
				if(field.getKey().equals(value)) {
					keys.addAll(historical ? field.getValues(timestamp) : field
							.getValues());
				}
			}
		}
		else if(operator == Operator.GREATER_THAN) {
			Iterator<Field<Value, PrimaryKey>> it = ((TreeMap<Value, Field<Value, PrimaryKey>>) fields())
					.tailMap(value, false).values().iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> field = it.next();
				keys.addAll(historical ? field.getValues(timestamp) : field
						.getValues());
			}
		}
		else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
			Iterator<Field<Value, PrimaryKey>> it = ((TreeMap<Value, Field<Value, PrimaryKey>>) fields())
					.tailMap(value, true).values().iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> field = it.next();
				keys.addAll(historical ? field.getValues(timestamp) : field
						.getValues());
			}
		}
		else if(operator == Operator.LESS_THAN) {
			Iterator<Field<Value, PrimaryKey>> it = ((TreeMap<Value, Field<Value, PrimaryKey>>) fields())
					.headMap(value, false).values().iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> field = it.next();
				keys.addAll(historical ? field.getValues(timestamp) : field
						.getValues());
			}
		}
		else if(operator == Operator.LESS_THAN_OR_EQUALS) {
			Iterator<Field<Value, PrimaryKey>> it = ((TreeMap<Value, Field<Value, PrimaryKey>>) fields())
					.headMap(value, true).values().iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> field = it.next();
				keys.addAll(historical ? field.getValues(timestamp) : field
						.getValues());
			}
		}
		else if(operator == Operator.BETWEEN) {
			Preconditions.checkArgument(values.length > 1);
			Value value2 = values[1];
			Iterator<Field<Value, PrimaryKey>> it = ((TreeMap<Value, Field<Value, PrimaryKey>>) fields())
					.subMap(value, true, value2, false).values().iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> field = it.next();
				keys.addAll(historical ? field.getValues(timestamp) : field
						.getValues());
			}
		}
		else if(operator == Operator.REGEX) {
			Iterator<Field<Value, PrimaryKey>> it = fields().values()
					.iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> bucket = it.next();
				Pattern p = Pattern.compile(value.getQuantity().toString());
				Matcher m = p.matcher(bucket.getKey().toString());
				if(m.matches()) {
					keys.addAll(historical ? it.next().getValues(timestamp)
							: it.next().getValues());
				}
			}
		}
		else if(operator == Operator.NOT_REGEX) {
			Iterator<Field<Value, PrimaryKey>> it = fields().values()
					.iterator();
			while (it.hasNext()) {
				Field<Value, PrimaryKey> bucket = it.next();
				Pattern p = Pattern.compile(value.getQuantity().toString());
				Matcher m = p.matcher(bucket.getKey().toString());
				if(!m.matches()) {
					keys.addAll(historical ? it.next().getValues(timestamp)
							: it.next().getValues());
				}
			}
		}
		else {
			throw new UnsupportedOperationException();
		}
		return keys;
	}

}

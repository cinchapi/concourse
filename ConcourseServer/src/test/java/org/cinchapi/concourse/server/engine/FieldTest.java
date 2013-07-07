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
import java.util.List;

import junit.framework.Assert;

import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.ByteableTest;
import org.cinchapi.common.time.Time;
import org.cinchapi.common.util.Tests;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Field} implementations
 * 
 * @author jnelson
 */
public abstract class FieldTest<K extends Byteable, V extends Storable> extends
		ByteableTest {

	@Override
	protected final Field<K, V> getObject() {
		return newPopulatedInstance();
	}

	@Override
	protected abstract Field<K, V> copy(Byteable object);
	
	protected Field<K, V> newInstance() {
		return newInstance(getKey());
	}

	protected abstract Field<K, V> newInstance(K key);

	protected Field<K, V> newPopulatedInstance() {
		Field<K, V> field = newInstance();
		populateWithAddAndRemove(field);
		return field;
	}

	protected abstract K getKey();

	protected abstract V getForStorageValue();

	protected abstract V getForStorageValueCopy(V value);

	protected abstract V getNotForStorageValue();

	@Test(expected = IllegalArgumentException.class)
	public void testCannotAddNotForStorageValue() {
		Field<K, V> field = newInstance();
		field.add(getNotForStorageValue());
	}

	@Test(expected = IllegalStateException.class)
	public void testCannotAddDuplicateValue() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		field.add(getForStorageValueCopy(value));
	}

	@Test
	public void testAdd() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		Assert.assertTrue(field.contains(value));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncreasingTimeStampOrderIsEnforcedOnAdd() {
		Field<K, V> field = newInstance();
		V first = getForStorageValue();
		V second = getForStorageValue();
		field.add(second);
		field.add(first);
	}

	// TODO test audit

	@Test
	public void testDoesNotContainBeforeAdd() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		Assert.assertFalse(field.contains(value));
	}

	@Test
	public void testDoesContainAfterAdd() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		Assert.assertTrue(field.contains(value));
	}

	@Test
	public void testDoesNotContainAfterRemove() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		field.remove(getForStorageValueCopy(value));
		Assert.assertFalse(field.contains(value));
	}

	@Test
	public void testDoesContainHistoricallyAfterRemove() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		long timestamp = Time.now();
		field.remove(getForStorageValueCopy(value));
		Assert.assertTrue(field.contains(value, timestamp));
	}

	@Test
	public void testDoesNotContainHistoricallyBeforeAdd() {
		Field<K, V> field = newInstance();
		V value = getForStorageValue();
		long timestamp = value.getTimestamp() - 1;
		field.add(value);
		Assert.assertFalse(field.contains(value, timestamp));
	}

	@Test
	public void testCountBeforeAdd() {
		Field<K, V> field = newInstance();
		Assert.assertEquals(0, field.count());
	}

	@Test
	public void testCountAfterAdd() {
		Field<K, V> field = newInstance();
		int count = populateWithAdd(field).size();
		Assert.assertEquals(count, field.count());
	}

	@Test
	public void testCountAfterRemove() {
		Field<K, V> field = newInstance();
		List<V> values = populateWithAddAndRemove(field);
		Assert.assertEquals(values.size(), field.count());
	}

	@Test
	public void testGetKey() {
		K key = getKey();
		Field<K, V> field = newInstance(key);
		Assert.assertEquals(key, field.getKey());
	}

	@Test
	public void testGetValuesBeforeAdd() {
		Field<K, V> field = newInstance();
		Assert.assertEquals(0, field.getValues().size());
	}

	@Test
	public void testGetValuesAfterAdd() {
		Field<K, V> field = newInstance();
		List<V> values = populateWithAdd(field);
		List<V> stored = field.getValues();
		for (V value : values) {
			Assert.assertTrue(stored.contains(value));
		}
	}

	@Test
	public void testGetValuesAfterRemove() {
		Field<K, V> field = newInstance();
		List<V> values = populateWithAdd(field);
		Iterator<V> it = values.iterator();
		while (it.hasNext()) {
			V value = it.next();
			if(Tests.getInt() % 2 == 0) {
				field.remove(getForStorageValueCopy(value));
			}
		}
		List<V> stored = field.getValues();
		for (V value : values) {
			Assert.assertEquals(field.contains(value), stored.contains(value));
		}
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testGetValuesIsReadOnly(){
		Field<K, V> field = newInstance();
		populateWithAddAndRemove(field);
		List<V> stored = field.getValues();
		stored.add(getForStorageValue());
	}
	
	@Test
	public void testGetValuesIsUpdated(){
		Field<K, V> field = newInstance();
		populateWithAddAndRemove(field);
		V value = getForStorageValue();
		while(field.contains(value)){
			value = getForStorageValue();
		}
		List<V> stored = field.getValues();
		Assert.assertFalse(stored.contains(value));
		field.add(value);
		Assert.assertTrue(stored.contains(value));
	}
	
	@Test
	public void testGetValuesHistorically(){
		Field<K, V> field = newInstance();
		List<V> values = populateWithAddAndRemove(field);
		long timestamp = Time.now();
		populateWithAddAndRemove(field);
		List<V> stored = field.getValues(timestamp);
		Assert.assertEquals(values.size(), stored.size());
		for(V value : values){
			Assert.assertTrue(stored.contains(value));
		}
	}
	
	@Test
	public void testIsEmptyBeforeAdd(){
		Field<K, V> field = newInstance();
		Assert.assertTrue(field.isEmpty());
	}
	
	@Test
	public void testIsEmptyAfterAdd(){
		Field<K, V> field = newInstance();
		populateWithAdd(field);
		Assert.assertFalse(field.isEmpty());
	}
	
	@Test
	public void testIsEmptyAfterRemove(){
		Field<K, V> field = newInstance();
		List<V> values = populateWithAdd(field);
		for(V value : values){
			field.remove(getForStorageValueCopy(value));
		}
		Assert.assertTrue(field.isEmpty());
	}
	
	@Test(expected=IllegalStateException.class)
	public void testRemoveBeforeAdd(){
		Field<K,V> field = newInstance();
		field.remove(getForStorageValue());
	}
	
	@Test
	public void testRemoveAfterAdd(){
		Field<K,V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		field.remove(getForStorageValueCopy(value));
		Assert.assertFalse(field.contains(value));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testInreasingTimeStampOrderIsEnforcedOnRemove(){
		Field<K,V> field = newInstance();
		V value = getForStorageValue();
		field.add(value);
		field.remove(value);
	}
	

	/**
	 * Populate {@code field} with ADD revisions.
	 * 
	 * @param field
	 * @return the list of contained values that populate the field
	 */
	private List<V> populateWithAdd(Field<K, V> field) {
		List<V> values = Lists.newArrayList();
		int count = Tests.getScaleCount();
		for (int i = 0; i < count; i++) {
			V value = getForStorageValue();
			while (field.contains(value)) {
				value = getForStorageValue();
			}
			values.add(value);
			field.add(value);
		}
		return values;
	}

	/**
	 * Populated {@code field} with ADD and REMOVE revisions.
	 * 
	 * @param field
	 * @return the list of contained values that populate the field
	 */
	private List<V> populateWithAddAndRemove(Field<K, V> field) {
		List<V> values = Lists.newArrayList();
		int count = Tests.getScaleCount();
		for (int i = 0; i < count; i++) {
			V value = getForStorageValue();
			while (field.contains(value)) {
				value = getForStorageValue();
			}
			values.add(value);
			field.add(value);
		}
		Iterator<V> it = values.iterator();
		while (it.hasNext()) {
			V value = it.next();
			if(Tests.getInt() % 2 == 0) {
				it.remove();
				field.remove(getForStorageValueCopy(value));
			}
		}
		return values;
	}
}

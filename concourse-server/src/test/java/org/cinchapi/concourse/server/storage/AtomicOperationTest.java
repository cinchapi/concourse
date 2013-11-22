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
package org.cinchapi.concourse.server.storage;

import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * 
 * @author jnelson
 */
public abstract class AtomicOperationTest extends BufferedStoreTest {
	
	private Compoundable destination;
	
	@Test
	public void testAbort(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		Assert.assertTrue(store.verify(key, value, record));
		((AtomicOperation) store).abort();
		Assert.assertFalse(destination.verify(key, value, record));
	}
	
	@Test
	public void testIsolation(){
		AtomicOperation a = AtomicOperation.start(destination);
		AtomicOperation b = AtomicOperation.start(destination);
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		Assert.assertTrue(((AtomicOperation) a).add(key, value, record));
		Assert.assertTrue(((AtomicOperation) b).add(key, value, record));
		Assert.assertFalse(destination.verify(key, value, record));
	}
	
	@Test
	public void testCommit(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		((AtomicOperation) store).commit();
		Assert.assertTrue(destination.verify(key, value, record));
	}
	
	@Test
	public void testCommitFailsIfVersionChanges(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		AtomicOperation other = AtomicOperation.start(destination);
		other.add(key, value, record);
		Assert.assertTrue(other.commit());
		Assert.assertFalse(((AtomicOperation) store).commit());
	}
	
	@Override
	protected void add(String key, TObject value, long record) {
		((AtomicOperation) store).add(key, value, record);		
	}
	
	protected abstract Compoundable getDestination();
	
	@Override
	protected AtomicOperation getStore() {
		destination = getDestination();
		return AtomicOperation.start(destination);
	}

	@Override
	protected void remove(String key, TObject value, long record) {
		((AtomicOperation) store).remove(key, value, record);
		
	}

}

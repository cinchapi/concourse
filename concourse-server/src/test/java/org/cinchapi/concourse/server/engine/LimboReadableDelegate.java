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

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

/**
 * 
 * 
 * @author jnelson
 */
public class LimboReadableDelegate implements ReadableDelegate{
	
	private Limbo limbo;

	@Override
	public Map<Long, String> audit(long record) {
		return limbo.audit(record);
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		return limbo.audit(key, record);
	}

	@Override
	public Set<String> describe(long record) {
		return limbo.describe(record);
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		return limbo.describe(record, timestamp);
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		return limbo.fetch(key, record);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		return limbo.fetch(key, record, timestamp);
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		return limbo.find(timestamp, key, operator, values);
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		return limbo.find(key, operator, values);
	}

	@Override
	public boolean ping(long record) {
		return limbo.ping(record);
	}

	@Override
	public Set<Long> search(String key, String query) {
		return limbo.search(key, query);
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		return limbo.verify(key, value, record);
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		return limbo.verify(key, value, record, timestamp);
	}


	@Override
	public void add(String key, Object value, long record) {
		limbo.add(key, new TObject(value), record);
		
	}

	@Override
	public void remove(String key, Object value, long record) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.cinchapi.concourse.server.engine.ReadableDelegate#nuke()
	 */
	@Override
	public void nuke() {
		// TODO Auto-generated method stub
		
	}

}

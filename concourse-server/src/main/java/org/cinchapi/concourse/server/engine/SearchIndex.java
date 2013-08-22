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

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.multithread.Lock;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.thrift.Type;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * A grouping of data for efficient fulltext searching.
 * <p>
 * Each SerchIndex is identified by a {@code key} and maps a term index
 * (substring of a word) to a set of positions and provides an interface for
 * fulltext searching.
 * </p>
 * 
 * @author jnelson
 */
@PackagePrivate
@ThreadSafe
class SearchIndex extends Record<Text, Text, Position> {

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param parentStore
	 */
	@DoNotInvoke
	public SearchIndex(Text key, String parentStore) {
		super(key, parentStore);
	}

	/**
	 * DO NOT CALL. Use {@link #add(Value, PrimaryKey)} instead.
	 */
	@Override
	@DoNotInvoke
	public final void add(Text key, Position value) {
		super.add(key, value);
	}

	/**
	 * DO NOT CALL. Use {@link #remove(Value, PrimaryKey)} instead.
	 */
	@Override
	@DoNotInvoke
	public final void remove(Text key, Position value) {
		super.remove(key, value);
	}

	@Override
	protected Map<Text, Set<Position>> __getMapType() {
		return Maps.newHashMap();
	}

	@Override
	protected Class<Text> keyClass() {
		return Text.class;
	}

	@Override
	protected Class<Position> valueClass() {
		return Position.class;
	}

	/**
	 * Add fulltext indices for {@code} value to {@code key}.
	 * 
	 * @param value
	 * @param key
	 */
	@PackagePrivate
	final void add(Value value, PrimaryKey key) {
		if(value.getType() == Type.STRING) {
			Lock lock = writeLock();
			try {
				Text text = Text.fromString((String) Convert.thriftToJava(value
						.getQuantity()));
				String[] toks = text.toString().split(" ");
				int pos = 0;
				for (String tok : toks) {
					// TODO check if tok is stopword and if so remove
					for (int i = 0; i < tok.length(); i++) {
						for (int j = i + 1; j < tok.length() + 1; j++) {
							Text index = Text.fromString(tok.substring(i, j));
							if(!Strings.isNullOrEmpty(index.toString())) {
								try {
									add(index,
											Position.fromPrimaryKeyAndMarker(
													key, pos)); // *authorized*
								}
								catch (IllegalStateException
										| IllegalArgumentException e) {
									// This indicates that an attempt was made
									// to add a duplicate index. In this
									// instance it is safe to ignore these
									// exceptions.
								}
							}
						}
					}
					pos++;
				}
			}
			finally {
				lock.release();
			}
		}
	}

	/**
	 * Remove the fulltext indices for {@code value} to {@code key}.
	 * 
	 * @param value
	 * @param key
	 */
	@PackagePrivate
	final void remove(Value value, PrimaryKey key) {
		if(value.getType() == Type.STRING) {
			Lock lock = writeLock();
			try {
				Text text = Text.fromString((String) Convert.thriftToJava(value
						.getQuantity()));
				String[] toks = text.toString().split(" ");
				int pos = 0;
				for (String tok : toks) {
					// TODO check if tok is stopword and if so remove
					for (int i = 0; i < tok.length(); i++) {
						for (int j = i + 1; j < tok.length() + 1; j++) {
							Text index = Text.fromString(tok.substring(i, j));
							if(!Strings.isNullOrEmpty(index.toString())) {
								remove(index, Position.fromPrimaryKeyAndMarker(
										key, pos)); // **Authorized**
							}
						}
					}
					pos++;
				}
			}
			finally {
				lock.release();
			}
		}
	}

	/**
	 * Return the Set of primary keys for records that match {@code query}.
	 * 
	 * @param query
	 * @return the Set of PrimaryKeys
	 */
	@PackagePrivate
	Set<PrimaryKey> search(Text query) {
		Map<PrimaryKey, Integer> reference = Maps.newHashMap();
		String[] toks = query.toString().split(" ");
		boolean initial = true;
		for (String tok : toks) {
			Map<PrimaryKey, Integer> temp = Maps.newHashMap();
			// TODO check if tok is a stopword and if so remove
			Set<Position> positions = get(Text.fromString(tok));
			for (Position position : positions) {
				PrimaryKey key = position.getPrimaryKey();
				int pos = position.getIndex();
				if(initial) {
					temp.put(key, pos);
				}
				else {
					Integer current = reference.get(key);
					if(current != null && pos == current + 1) {
						temp.put(key, pos);
					}
				}
			}
			initial = false;
			reference = temp;
		}
		return reference.keySet();
	}
}

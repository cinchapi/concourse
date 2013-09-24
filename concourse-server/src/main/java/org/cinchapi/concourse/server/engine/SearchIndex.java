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
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.Context;
import org.cinchapi.concourse.server.Properties;
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Type;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * A collection of n-gram indexes that enable fulltext infix searching. For
 * every word in a {@link Value}, each substring index is mapped to a
 * {@link Position}. The entire SearchIndex contains a collection of these
 * mappings.
 * 
 * @author jnelson
 */
@PackagePrivate
@ThreadSafe
class SearchIndex extends Record<Text, Text, Position> {

	/**
	 * Construct a new instance.
	 * 
	 * @param filename
	 * @param context
	 */
	@DoNotInvoke
	public SearchIndex(String filename, Context context) {
		super(filename, context);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param parentStore
	 * @param context
	 */
	@DoNotInvoke
	public SearchIndex(Text key, String parentStore, Context context) {
		super(key, parentStore, context);
	}

	/**
	 * DO NOT CALL. Use {@link #add(Value, PrimaryKey)} instead.
	 */
	@Override
	@DoNotInvoke
	public final void add(Text key, Position value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Add fulltext indices for {@code} value to {@code key}.
	 * 
	 * @param value
	 * @param key
	 */
	public final void add(Value value, PrimaryKey key) {
		if(value.getType() == Type.STRING) {
			Text text = Text.fromString((String) Convert.thriftToJava(value
					.getQuantity()));
			String[] toks = text.toString().split(" ");
			ExecutorService executor = Threads
					.newCachedThreadPool("search-index-worker");
			int pos = 0;
			for (String tok : toks) {
				executor.submit(new IndexWorker(tok, pos, key));
				pos++;
			}
			executor.shutdown();
			while (!executor.isTerminated()) {
				continue; // block until all tasks have completed
			}
		}
	}

	/**
	 * DO NOT CALL. Use {@link #remove(Value, PrimaryKey)} instead.
	 */
	@Override
	@DoNotInvoke
	public final void remove(Text key, Position value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Remove the fulltext indices for {@code value} to {@code key}.
	 * 
	 * @param value
	 * @param key
	 */
	public final void remove(Value value, PrimaryKey key) {
		if(value.getType() == Type.STRING) {
			Text text = Text.fromString((String) Convert.thriftToJava(value
					.getQuantity()));
			String[] toks = text.toString().split(" ");
			ExecutorService executor = Threads
					.newCachedThreadPool("search-deindex-worker");
			int pos = 0;
			for (String tok : toks) {
				executor.submit(new DeIndexWorker(tok, pos, key));
				pos++;
			}
			executor.shutdown();
			while (!executor.isTerminated()) {
				continue; // block until all tasks have completed
			}
		}
	}

	/**
	 * Return the Set of primary keys for records that match {@code query}.
	 * 
	 * @param query
	 * @return the Set of PrimaryKeys
	 */
	public Set<PrimaryKey> search(Text query) {
		Map<PrimaryKey, Integer> reference = Maps.newHashMap();
		String[] toks = query.toString().split(" ");
		boolean initial = true;
		for (String tok : toks) {
			Map<PrimaryKey, Integer> temp = Maps.newHashMap();
			if(Properties.STOPWORDS.contains(tok)) {
				continue;
			}
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

	@Override
	protected Class<Text> keyClass() {
		return Text.class;
	}

	@Override
	protected Map<Text, Set<Position>> mapType() {
		return Maps.newHashMap();
	}

	@Override
	protected Class<Position> valueClass() {
		return Position.class;
	}

	/**
	 * Add an index for {@code key} to {@code value}.
	 * 
	 * @param key
	 * @param value
	 */
	private void addIndex(Text key, Position value) {
		super.add(key, value);
	}

	/**
	 * Remove the index for {@code key} to {@code value}.
	 * 
	 * @param key
	 * @param value
	 */
	private void removeIndex(Text key, Position value) {
		super.remove(key, value);
	}

	/**
	 * A {@link Runnable} that does the work to deindex a specified word at a
	 * specified position in a specified record.
	 * 
	 * @author jnelson
	 */
	private class DeIndexWorker implements Runnable {
		private final String word;
		private final int position;
		private final PrimaryKey key;

		/**
		 * Construct a new instance.
		 * 
		 * @param word
		 * @param position
		 */
		public DeIndexWorker(String word, int position, PrimaryKey key) {
			this.word = word;
			this.position = position;
			this.key = key;
		}

		@Override
		public void run() {
			if(Properties.STOPWORDS.contains(word)) {
				return;
			}
			for (int i = 0; i < word.length(); i++) {
				for (int j = (i + Properties.SEARCH_INDEX_GRANULARITY - 1); j < word
						.length() + 1; j++) {
					Text index = Text.fromString(word.substring(i, j));
					if(!Strings.isNullOrEmpty(index.toString())) {
						removeIndex(index,
								Position.fromPrimaryKeyAndMarker(key, position));
					}
				}
			}
		}
	}

	/**
	 * A {@link Runnable} that does the work to index a specified word at a
	 * specified position in a specified record.
	 * 
	 * @author jnelson
	 */
	private class IndexWorker implements Runnable {

		private final String word;
		private final int position;
		private final PrimaryKey key;

		/**
		 * Construct a new instance.
		 * 
		 * @param word
		 * @param position
		 */
		public IndexWorker(String word, int position, PrimaryKey key) {
			this.word = word;
			this.position = position;
			this.key = key;
		}

		@Override
		public void run() {
			if(Properties.STOPWORDS.contains(word)) {
				return;
			}
			for (int i = 0; i < word.length(); i++) {
				for (int j = i + (Properties.SEARCH_INDEX_GRANULARITY - 1); j < word
						.length() + 1; j++) {
					Text index = Text.fromString(word.substring(i, j));
					if(!Strings.isNullOrEmpty(index.toString())) {
						try {
							addIndex(index, Position.fromPrimaryKeyAndMarker(
									key, position));
						}
						catch (IllegalStateException | IllegalArgumentException e) {
							// This indicates that an attempt was made
							// to add a duplicate index. In this
							// instance it is safe to ignore these
							// exceptions.
							continue;
						}
					}
				}
			}

		}

	}
}

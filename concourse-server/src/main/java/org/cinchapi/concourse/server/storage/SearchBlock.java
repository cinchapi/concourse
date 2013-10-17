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

import static org.cinchapi.concourse.server.GlobalState.MIN_SEARCH_INDEX_SIZE;
import static org.cinchapi.concourse.server.GlobalState.STOPWORDS;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.concurrent.ConcourseExecutors;
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Type;

import com.google.common.base.Strings;

/**
 * A Block that store SearchRevision data to be used in a SearchRecord.
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
final class SearchBlock extends Block<Text, Text, Position> {

	/**
	 * DO NOT CALL. Use {@link Block#createSearchBlock(String)} instead.
	 * 
	 * @param directory
	 */
	@PackagePrivate
	@DoNotInvoke
	SearchBlock(String directory) {
		super(directory);
	}

	/**
	 * DO NOT CALL. Use {@link Block#loadSearchBlock(String, String)} instead.
	 * 
	 * @param directory
	 * @param id
	 */
	@PackagePrivate
	@DoNotInvoke
	SearchBlock(String directory, String id) {
		super(directory, id);
	}

	/**
	 * DO NOT CALL. Use {@link #insert(Text, Value, PrimaryKey)} instead.
	 */
	@Override
	@DoNotInvoke
	public final void insert(Text locator, Text key, Position value,
			long version) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Insert a revision for {@code key} as {@code value} in {@code record} at
	 * {@code version}
	 * 
	 * @param key
	 * @param value
	 * @param record
	 * @param version
	 */
	public final void insert(Text key, Value value, PrimaryKey record,
			long version) {
		masterLock.writeLock().lock();
		try {
			if(value.getType() == Type.STRING) {
				String[] toks = value.getObject().toString().split(" ");
				ExecutorService executor = ConcourseExecutors
						.newCachedThreadPool("SearchBlock");
				int pos = 0;
				for (String tok : toks) {
					executor.submit(getRunnable(key, tok, pos, record, version));
					pos++;
				}
				executor.shutdown();
				while (!executor.isTerminated()) {
					continue; // block until all tasks have completed
				}
			}
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	@Override
	protected SearchRevision makeRevision(Text locator, Text key,
			Position value, long version) {
		return Revision.createSearchRevision(locator, key, value, version);
	}

	@Override
	protected Class<SearchRevision> xRevisionClass() {
		return SearchRevision.class;
	}

	/**
	 * Call super.{@link #insert(Text, Text, Position, long)}
	 * 
	 * @param locator
	 * @param key
	 * @param value
	 * @param version
	 */
	private final void doInsert(Text locator, Text key, Position value,
			long version) {
		super.insert(locator, key, value, version);
	}

	/**
	 * Return a Runnable that will insert a revision for {@code term} at
	 * {@code position} for {@code key} in {@code record} at {@code version}.
	 * 
	 * @param key
	 * @param term
	 * @param position
	 * @param record
	 * @param version
	 * @return the index Runnable
	 */
	private Runnable getRunnable(final Text key, final String term,
			final int position, final PrimaryKey record, final long version) {
		return new Runnable() {

			@Override
			public void run() {
				if(STOPWORDS.contains(term)) {
					return;
				}
				for (int i = 0; i < term.length(); i++) {
					for (int j = i + (MIN_SEARCH_INDEX_SIZE - 1); j < term
							.length() + 1; j++) {
						Text index = Text.wrap(term.substring(i, j));
						if(!Strings.isNullOrEmpty(index.toString())) {
							try {
								doInsert(key, index,
										Position.wrap(record, position),
										version);
							}
							catch (IllegalStateException
									| IllegalArgumentException e) {
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
		};
	}

}

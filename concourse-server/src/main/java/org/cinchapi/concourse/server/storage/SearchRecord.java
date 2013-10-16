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

import static org.cinchapi.concourse.server.GlobalState.STOPWORDS;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
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
final class SearchRecord extends Record<Text, Text, Position> {

	/**
	 * DO NOT INVOKE. Use {@link Record#createSearchRecord(Text)} or
	 * {@link Record#createPartialSearchRecord(Text, Text)} instead.
	 * 
	 * @param locator
	 * @param key
	 */
	@PackagePrivate
	@DoNotInvoke
	SearchRecord(Text locator, @Nullable Text key) {
		super(locator, key);
	}

	/**
	 * Return the Set of primary keys for records that match {@code query}.
	 * 
	 * @param query
	 * @return the Set of PrimaryKeys
	 */
	public Set<PrimaryKey> search(Text query) {
		Lock lock = readLock();
		try {
			Map<PrimaryKey, Integer> reference = Maps.newHashMap();
			String[] toks = query.toString().split(" ");
			boolean initial = true;
			for (String tok : toks) {
				Map<PrimaryKey, Integer> temp = Maps.newHashMap();
				if(STOPWORDS.contains(tok)) {
					continue;
				}
				Set<Position> positions = get(Text.wrap(tok));
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
		finally {
			lock.release();
		}
	}

	@Override
	protected Map<Text, Set<Position>> mapType() {
		return Maps.newHashMap();
	}
}

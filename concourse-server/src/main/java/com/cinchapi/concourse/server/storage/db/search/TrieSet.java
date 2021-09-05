/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage.db.search;

import java.util.AbstractSet;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.collections4.trie.PatriciaTrie;

import com.cinchapi.concourse.annotate.Experimental;

/**
 * A {@link Set} of {@link String Strings} that is backed by a Trie <a href=
 * "https://en.wikipedia.org/wiki/Trie">https://en.wikipedia.org/wiki/Trie</a>.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
@Experimental
public class TrieSet extends AbstractSet<String> {

    /**
     * Dummy value to associate with an entry in the backing {@link #trie}.
     */
    private static final Object PRESENT = new Object();

    /**
     * The backing store.
     */
    private final PatriciaTrie<Object> trie;

    /**
     * Construct a new instance.
     */
    public TrieSet() {
        this.trie = new PatriciaTrie<>();
    }

    @Override
    public Iterator<String> iterator() {
        return trie.keySet().iterator();
    }

    @Override
    public int size() {
        return trie.size();
    }

    @Override
    public boolean contains(Object o) {
        return trie.containsKey(o);
    }

    @Override
    public boolean add(String string) {
        return trie.put(string, PRESENT) == null;
    }

}

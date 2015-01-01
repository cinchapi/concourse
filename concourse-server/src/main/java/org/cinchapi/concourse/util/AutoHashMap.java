/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

import org.cinchapi.concourse.annotate.Experimental;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.base.Function;

/**
 * AutoMap with hash based sorting. Create using
 * {@link AutoMap#newAutoHashMap(Function, Function)}.
 * 
 * @author jnelson
 */
@Experimental
public class AutoHashMap<K, V> extends AutoMap<K, V> {
    
    private static final int INITIAL_SIZE = (int) Math.pow(2, 8);

    /**
     * Construct a new instance.
     * 
     * @param backingStore
     * @param loader
     * @param cleaner
     */
    protected AutoHashMap(Function<K, V> loader, Function<V, Boolean> cleaner) {
        super(new NonBlockingHashMap<K,V>(INITIAL_SIZE), loader, cleaner);
    }

}

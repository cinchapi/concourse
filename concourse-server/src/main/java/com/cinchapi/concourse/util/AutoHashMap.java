/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.cinchapi.concourse.annotate.Experimental;
import com.google.common.base.Function;

/**
 * AutoMap with hash based sorting. Create using
 * {@link AutoMap#newAutoHashMap(Function, Function)}.
 * 
 * @author Jeff Nelson
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

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
package com.cinchapi.concourse.lang;

/**
 * A {@link Symbol} that represents a key in a {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
public class KeySymbol extends AbstractSymbol {

    /**
     * Create a new {@link KeySymbol} for the given {@code key}.
     * 
     * @param key
     * @return the symbol
     */
    public static KeySymbol create(String key) {
        return new KeySymbol(key);
    }

    /**
     * Parse a {@link KeySymbol} from the given {@code string}.
     * 
     * @param string
     * @return the symbol
     */
    public static KeySymbol parse(String string) {
        return new KeySymbol(string);
    }

    /**
     * The associated key.
     */
    private final String key;

    /**
     * Construct a new instance.
     * 
     * @param key
     */
    private KeySymbol(String key) {
        this.key = key;
    }

    /**
     * Return the key associated with this {@link Symbol}.
     * 
     * @return the key
     */
    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }

}

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

import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;

/**
 * A {@link Symbol} that represents a value in a {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
class ValueSymbol extends AbstractSymbol {

    /**
     * Return the {@link ValueSymbol} for the specified {@code value}.
     * 
     * @param value
     * @return the symbol
     */
    public static ValueSymbol create(Object value) {
        return new ValueSymbol(value);
    }

    /**
     * Return the {@link ValueSymbol} that is parsed from {@code string}.
     * 
     * @param string
     * @return the symbol
     */
    public static ValueSymbol parse(String string) {
        return new ValueSymbol(Convert.stringToJava(string));
    }

    /**
     * The associated value.
     */
    private final TObject value;

    /**
     * Construct a new instance.
     * 
     * @param value
     */
    private ValueSymbol(Object value) {
        this.value = Convert.javaToThrift(value);
    }

    /**
     * Return the value associated with this {@link Symbol}.
     * 
     * @return the value
     */
    public TObject getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

}

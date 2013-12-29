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

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.TObject;

import com.google.common.base.Function;

/**
 * A collection of {@link Function} objects.
 * 
 * @author jnelson
 */
final class Functions {

    /**
     * A function that transforms an {@link TObject} to a {@link Value}.
     */
    @PackagePrivate
    static final Function<TObject, Value> TOBJECT_TO_VALUE = new Function<TObject, Value>() {

        @Override
        public Value apply(TObject input) {
            return Value.wrap(input);
        }

    };
    /**
     * A function that transforms a {@link Value} to an {@link Object}.
     */
    @PackagePrivate
    static final Function<Value, TObject> VALUE_TO_TOBJECT = new Function<Value, TObject>() {

        @Override
        public TObject apply(Value input) {
            return input.getTObject();
        }

    };
    /**
     * A function that transforms a {@link Text} object to a {@link String}.
     */
    @PackagePrivate
    static final Function<Text, String> TEXT_TO_STRING = new Function<Text, String>() {

        @Override
        public String apply(Text input) {
            return input.toString();
        }

    };
    /**
     * A function that transforms a {@link PrimaryKey} to a {@link Long}.
     */
    @PackagePrivate
    static final Function<PrimaryKey, Long> PRIMARY_KEY_TO_LONG = new Function<PrimaryKey, Long>() {

        @Override
        public Long apply(PrimaryKey input) {
            return input.longValue();
        }

    };

    private Functions() {/* Utility Class */}

}

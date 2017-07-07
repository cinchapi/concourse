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
package com.cinchapi.concourse.server.storage;

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.base.Function;

/**
 * A collection of {@link Function} objects.
 * 
 * @author Jeff Nelson
 */
public final class Functions {

    /**
     * A function that transforms an {@link TObject} to a {@link Value}.
     */
    public static final Function<TObject, Value> TOBJECT_TO_VALUE = new Function<TObject, Value>() {

        @Override
        public Value apply(TObject input) {
            return Value.wrap(input);
        }

    };
    /**
     * A function that transforms a {@link Value} to an {@link Object}.
     */
    public static final Function<Value, TObject> VALUE_TO_TOBJECT = new Function<Value, TObject>() {

        @Override
        public TObject apply(Value input) {
            return input.getTObject();
        }

    };
    /**
     * A function that transforms a {@link Text} object to a {@link String}.
     */
    public static final Function<Text, String> TEXT_TO_STRING = new Function<Text, String>() {

        @Override
        public String apply(Text input) {
            return input.toString();
        }

    };
    /**
     * A function that transforms a {@link PrimaryKey} to a {@link Long}.
     */
    public static final Function<PrimaryKey, Long> PRIMARY_KEY_TO_LONG = new Function<PrimaryKey, Long>() {

        @Override
        public Long apply(PrimaryKey input) {
            return input.longValue();
        }

    };

    private Functions() {/* Utility Class */}

}

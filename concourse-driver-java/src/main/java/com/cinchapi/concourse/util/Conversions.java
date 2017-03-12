/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.base.Function;
import com.google.common.base.Functions;

/**
 * A utility class that defines some {@link Function Functions} to perform
 * common conversions.
 * 
 * @author Jeff Nelson
 */
public final class Conversions {

    /**
     * Return a function to perform a conversion from a java {@link Object} to a
     * {@link TObject}.
     * 
     * @return the conversion function
     */
    public static Function<Object, TObject> javaToThrift() {
        return JAVA_TO_THRIFT_FUNCTION;
    }

    /**
     * Return a function that doesn't perform any conversion.
     * 
     * @return the (non) conversion function
     */
    public static <T> Function<T, T> none() {
        return Functions.identity();
    }

    /**
     * Return a function to perform a conversion from a possible {@link TObject}
     * to a java {#link Object}.
     * 
     * @return
     */
    public static Function<Object, Object> possibleThriftToJava() {
        return POSSIBLE_THRIFT_TO_JAVA_FUNCTION;
    }

    /**
     * Return a function to perform a conversion from a {@link TObject} to a
     * java {@link Object}.
     * 
     * @return the conversion function
     */
    public static Function<TObject, Object> thriftToJava() {
        return THRIFT_TO_JAVA_FUNCTION;
    }

    /**
     * Return a function to perform a casted conversion from {@link TObject} to
     * a java object of the parameterized type.
     * <p>
     * Compared to the {@link #thriftToJava()} method, functions returned here
     * will attempt to cast the object to type {@code T} instead of returning a
     * plain {@link Object}.
     * </p>
     * 
     * @return the conversion function
     */
    public static <T> Function<TObject, T> thriftToJavaCasted() {
        return new Function<TObject, T>() {

            @SuppressWarnings("unchecked")
            @Override
            public T apply(TObject input) {
                return (T) thriftToJava().apply(input);
            }

        };
    }

    /**
     * Return a function to perform a conversion from an {@link Timestamp} to a
     * long value that represents the corresponding unix timestamp with
     * microsecond precision.
     * 
     * @return the conversion function
     */
    public static Function<Long, Timestamp> timestampToMicros() {
        return TIMESTAMP_TO_MICROS;
    }

    /**
     * Function returned in {@link #javaToThrift()}.
     */
    private static final Function<Object, TObject> JAVA_TO_THRIFT_FUNCTION = new Function<Object, TObject>() {

        @Override
        public TObject apply(Object input) {
            return Convert.javaToThrift(input);
        }

    };

    /**
     * Function returned in {@link #thriftToJava()}.
     */
    private static final Function<TObject, Object> THRIFT_TO_JAVA_FUNCTION = new Function<TObject, Object>() {

        @Override
        public Object apply(TObject input) {
            return Convert.thriftToJava(input);
        }

    };

    /**
     * Function returned in {@link #possibleThriftToJava()}.
     */
    private static final Function<Object, Object> POSSIBLE_THRIFT_TO_JAVA_FUNCTION = new Function<Object, Object>() {

        @Override
        public Object apply(Object input) {
            return Convert.possibleThriftToJava(input);
        }

    };

    /**
     * Function returned in {@link #timestampToMicros()}.
     */
    private static final Function<Long, Timestamp> TIMESTAMP_TO_MICROS = new Function<Long, Timestamp>() {

        @Override
        public Timestamp apply(Long input) {
            return Timestamp.fromMicros(input);
        }

    };

    private Conversions() {/* noop */}

}

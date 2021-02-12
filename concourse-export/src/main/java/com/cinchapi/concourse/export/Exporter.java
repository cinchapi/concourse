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
package com.cinchapi.concourse.export;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.collect.Sequences;
import com.google.common.collect.Sets;

/**
 * An {@link Exporter} can push objects (represented as a Map} to an
 * {@link OutputStream}.
 *
 * @author Jeff Nelson
 */
@Immutable
public abstract class Exporter<V> {

    /**
     * Return the union of all keys within the {@code data} as an
     * {@link Iterable} that make up the headers for the data.
     * 
     * @param data
     * @return the headers
     */
    protected static <V> Iterable<String> getHeaders(
            Iterable<Map<String, V>> data) {
        Set<String> headers = Sets.newLinkedHashSet();
        for (Map<String, V> item : data) {
            for (String key : item.keySet()) {
                headers.add(key);
            }
        }
        return headers;
    }

    /**
     * Return the ideal string representation for {@code value}.
     * <p>
     * If {@code value} is a {@link Sequences#isSequence(Object) sequence}, this
     * method will return each item in the sequence as a comma separated string.
     * </p>
     * 
     * @param value
     * @return the ideal string representation for {@code value}
     */
    private static String stringify(Object value) {
        if(value == null) {
            return "null";
        }
        if(Sequences.isSequence(value)) {
            return AnyStrings.join(',', Sequences.stream(value).toArray());
        }
        else {
            return value.toString();
        }
    }

    /**
     * The place to which the {@code data} is exported.
     */
    protected final PrintStream output;

    /**
     * A function that converts values to a {@link String}.
     */
    protected final Function<V, String> toStringFunction;

    /**
     * Construct a new instance.
     * 
     * @param output
     */
    public Exporter(OutputStream output) {
        this(output, value -> stringify(value));
    }

    /**
     * Construct a new instance.
     * 
     * @param output
     * @param toStringFunction
     */
    public Exporter(OutputStream output, Function<V, String> toStringFunction) {
        // NOTE: The Exporters factory does not expose any methods that accept a
        // custom toStringFunction. This capability is added for future-proofing
        // in case more customizable toString generation is needed.
        this.output = output instanceof PrintStream ? (PrintStream) output
                : new PrintStream(output);
        this.toStringFunction = toStringFunction;
    }

    /**
     * Export each item in the {@code data} to the {@code output} stream using
     * the rules of this {@link Exporter}.
     * 
     * @param data
     */
    public abstract void export(Iterable<Map<String, V>> data);

    @Override
    protected void finalize() throws Throwable {
        output.close();
        super.finalize();
    }

}
